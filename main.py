"""
frigate-gif-sidecar

Listens to frigate/events on MQTT. When a doorbell person event ENDS inside the
'property' zone (and isn't recognized as Josh), pulls the Frigate clip.mp4,
runs a two-pass palette-optimized ffmpeg transcode to a low-fps, 720p GIF, drops
it where Home Assistant can serve it at /local/frigate_gifs/<id>.gif, and
publishes `frigate-gifs/ready/<id>` so HA can update the live notification.

Design notes:
- We listen to type=end (not type=new) because clip.mp4 only exists after Frigate
  finalizes the recording. Even then there's a small lag — we retry with backoff.
- Two-pass palette (stats_mode=diff + paletteuse dither=bayer:5) gives much better
  visual quality at the same byte budget than single-pass GIF encoding.
- 5 fps × 720 wide × ~10s clip ≈ 1.5–2 MB. Fits comfortably in Android's
  notification bigPicture budget without choking on cellular.
- Cleanup runs hourly + on startup: anything older than GIF_RETENTION_HOURS goes.
  GIF files are ephemeral notification fodder — the canonical recording is in
  Frigate, accessed via the clip.mp4 tap target.
"""
import json
import logging
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from urllib.request import urlretrieve
from urllib.error import HTTPError, URLError

import paho.mqtt.client as mqtt

# --- config from env ---------------------------------------------------------

FRIGATE_URL = os.environ["FRIGATE_URL"].rstrip("/")            # e.g. http://192.168.10.106:5000
MQTT_HOST = os.environ["MQTT_HOST"]
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER") or None
MQTT_PASS = os.environ.get("MQTT_PASS") or None
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "/output"))
CAMERAS = set(os.environ.get("CAMERAS", "doorbell").split(","))
LABELS = set(os.environ.get("LABELS", "person").split(","))
REQUIRED_ZONE = os.environ.get("REQUIRED_ZONE", "property")
IGNORE_SUBLABELS = set(s.strip() for s in os.environ.get("IGNORE_SUBLABELS", "Josh").split(",") if s.strip())
GIF_FPS = int(os.environ.get("GIF_FPS", "3"))
GIF_WIDTH = int(os.environ.get("GIF_WIDTH", "480"))
GIF_MAX_SECONDS = int(os.environ.get("GIF_MAX_SECONDS", "8"))  # cap clip length we transcode
GIF_RETENTION_HOURS = int(os.environ.get("GIF_RETENTION_HOURS", "24"))
CLIP_FETCH_TIMEOUT_S = int(os.environ.get("CLIP_FETCH_TIMEOUT_S", "45"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)-7s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("gif-sidecar")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Track which event IDs are already being processed so a noisy /events stream
# can't fire us twice for the same id.
_in_flight: set[str] = set()
_in_flight_lock = threading.Lock()


# --- ffmpeg ------------------------------------------------------------------

def _palette_filter() -> str:
    return f"fps={GIF_FPS},scale={GIF_WIDTH}:-2:flags=lanczos"


def transcode_to_gif(mp4_path: Path, gif_path: Path) -> None:
    """Two-pass palette-optimized GIF transcode."""
    with tempfile.TemporaryDirectory() as tmp:
        palette = Path(tmp) / "palette.png"

        # pass 1: build palette
        cmd1 = [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
            "-t", str(GIF_MAX_SECONDS),
            "-i", str(mp4_path),
            "-vf", f"{_palette_filter()},palettegen=stats_mode=diff",
            str(palette),
        ]
        subprocess.run(cmd1, check=True)

        # pass 2: apply palette
        cmd2 = [
            "ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
            "-t", str(GIF_MAX_SECONDS),
            "-i", str(mp4_path),
            "-i", str(palette),
            "-lavfi", f"{_palette_filter()}[v];[v][1:v]paletteuse=dither=bayer:bayer_scale=5",
            "-loop", "0",
            str(gif_path),
        ]
        subprocess.run(cmd2, check=True)


# --- Frigate clip fetch ------------------------------------------------------

def fetch_clip(evt_id: str, dest: Path) -> bool:
    """Retry-with-backoff GET on Frigate clip.mp4. Returns True iff downloaded."""
    url = f"{FRIGATE_URL}/api/events/{evt_id}/clip.mp4"
    delays = [1, 2, 3, 4, 5, 7, 10, 13]   # ~45s total
    waited = 0
    for delay in delays:
        if waited >= CLIP_FETCH_TIMEOUT_S:
            break
        try:
            urlretrieve(url, dest)
            if dest.stat().st_size > 0:
                log.info("evt=%s clip ready after %ds (%d bytes)", evt_id, waited, dest.stat().st_size)
                return True
            dest.unlink(missing_ok=True)
        except HTTPError as e:
            if e.code != 404:
                log.warning("evt=%s clip fetch HTTP %s", evt_id, e.code)
        except URLError as e:
            log.warning("evt=%s clip fetch URL error: %s", evt_id, e)
        time.sleep(delay)
        waited += delay
    log.error("evt=%s clip never became available after %ds", evt_id, waited)
    return False


# --- event handling ----------------------------------------------------------

def should_process(after: dict) -> tuple[bool, str]:
    """Returns (process?, reason). Reason is for log clarity only."""
    if after.get("camera") not in CAMERAS:
        return False, "camera-skip"
    if after.get("label") not in LABELS:
        return False, "label-skip"
    if REQUIRED_ZONE and REQUIRED_ZONE not in (after.get("entered_zones") or []):
        return False, "no-zone"
    if after.get("sub_label") in IGNORE_SUBLABELS:
        return False, "ignored-sublabel"
    return True, "match"


def process_event(client: mqtt.Client, evt_id: str) -> None:
    with _in_flight_lock:
        if evt_id in _in_flight:
            log.debug("evt=%s already in flight, skipping", evt_id)
            return
        _in_flight.add(evt_id)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            mp4 = Path(tmp) / f"{evt_id}.mp4"
            if not fetch_clip(evt_id, mp4):
                return
            gif = OUTPUT_DIR / f"{evt_id}.gif"
            t0 = time.time()
            try:
                transcode_to_gif(mp4, gif)
            except subprocess.CalledProcessError as e:
                log.error("evt=%s ffmpeg failed rc=%s", evt_id, e.returncode)
                gif.unlink(missing_ok=True)
                return
            took_ms = int((time.time() - t0) * 1000)
            size = gif.stat().st_size
            log.info("evt=%s gif written: %d bytes in %d ms", evt_id, size, took_ms)
            payload = json.dumps({
                "id": evt_id,
                "url": f"/local/frigate_gifs/{evt_id}.gif",
                "size_bytes": size,
                "transcode_ms": took_ms,
            })
            client.publish(f"frigate-gifs/ready/{evt_id}", payload, qos=0, retain=False)
    finally:
        with _in_flight_lock:
            _in_flight.discard(evt_id)


def on_message(client: mqtt.Client, _userdata, msg: mqtt.MQTTMessage) -> None:
    if msg.topic != "frigate/events":
        return
    try:
        payload = json.loads(msg.payload)
    except json.JSONDecodeError:
        return
    if payload.get("type") != "end":
        return
    after = payload.get("after") or {}
    ok, reason = should_process(after)
    if not ok:
        log.debug("evt=%s skipped: %s", after.get("id"), reason)
        return
    evt_id = after.get("id")
    if not evt_id:
        return
    log.info("evt=%s match (camera=%s label=%s sub_label=%s)",
             evt_id, after.get("camera"), after.get("label"), after.get("sub_label"))
    # offload to a worker thread so MQTT loop stays responsive
    threading.Thread(target=process_event, args=(client, evt_id), daemon=True).start()


# --- cleanup -----------------------------------------------------------------

def cleanup_old_gifs() -> None:
    cutoff = time.time() - GIF_RETENTION_HOURS * 3600
    removed = 0
    for gif in OUTPUT_DIR.glob("*.gif"):
        try:
            if gif.stat().st_mtime < cutoff:
                gif.unlink()
                removed += 1
        except FileNotFoundError:
            pass
    if removed:
        log.info("cleanup: removed %d gif(s) older than %dh", removed, GIF_RETENTION_HOURS)


def cleanup_loop() -> None:
    while True:
        try:
            cleanup_old_gifs()
        except Exception:
            log.exception("cleanup loop error")
        time.sleep(3600)


# --- main --------------------------------------------------------------------

def on_connect(client: mqtt.Client, _userdata, _flags, rc, _props=None) -> None:
    if rc == 0:
        client.subscribe("frigate/events")
        log.info("connected to %s:%d, subscribed to frigate/events", MQTT_HOST, MQTT_PORT)
    else:
        log.error("mqtt connect rc=%s", rc)


def main() -> None:
    log.info(
        "starting: frigate=%s mqtt=%s:%d cameras=%s labels=%s zone=%s ignore=%s "
        "gif=%dx_@%dfps cap=%ds out=%s retain=%dh",
        FRIGATE_URL, MQTT_HOST, MQTT_PORT, CAMERAS, LABELS, REQUIRED_ZONE,
        IGNORE_SUBLABELS, GIF_WIDTH, GIF_FPS, GIF_MAX_SECONDS, OUTPUT_DIR, GIF_RETENTION_HOURS,
    )
    cleanup_old_gifs()
    threading.Thread(target=cleanup_loop, daemon=True).start()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="frigate-gif-sidecar")
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)

    # graceful shutdown
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    while not stop.is_set():
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            client.loop_forever(retry_first_connection=True)
        except Exception:
            log.exception("mqtt loop crashed, reconnecting in 5s")
            time.sleep(5)


if __name__ == "__main__":
    main()
