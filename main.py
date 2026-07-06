"""
frigate-gif-sidecar

Listens to frigate/events on MQTT. When a doorbell person event ENDS inside the
'property' zone (and isn't recognized as Josh), pulls the Frigate clip.mp4,
runs a two-pass palette-optimized ffmpeg transcode to a low-fps, 720p GIF, drops
it where Home Assistant can serve it at /local/frigate_gifs/<id>.gif, and
publishes `frigate-gifs/ready/<id>` so HA can update the live notification.

Design notes:
- We pull a FIXED window [start, start+GIF_WINDOW_SECONDS] from Frigate's
  continuous *recordings* (not the event's clip.mp4). The event clip only exists
  after the event ENDS, so a visitor who lingers 50s used to delay the GIF by 50s.
  The recordings window is available a fixed ~15-20s after the person appears,
  regardless of dwell time. We fire on the first event message where the person
  has entered the required zone, then retry-fetch the window until it finalizes.
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
# Web-playable clips for the /doorbell-clip iframe player go here (served at
# /local/frigate_clips). MUST be under /local: HA serves /local with byte-range
# (206) which <video> needs; Frigate's notifications proxy returns 200 with NO
# range support, which mobile browsers can't play (broken-media icon).
CLIP_DIR = OUTPUT_DIR.parent / "frigate_clips"
CAMERAS = set(os.environ.get("CAMERAS", "doorbell").split(","))
LABELS = set(os.environ.get("LABELS", "person").split(","))
REQUIRED_ZONE = os.environ.get("REQUIRED_ZONE", "property")
IGNORE_SUBLABELS = set(s.strip() for s in os.environ.get("IGNORE_SUBLABELS", "Josh").split(",") if s.strip())
GIF_FPS = int(os.environ.get("GIF_FPS", "8"))
GIF_WIDTH = int(os.environ.get("GIF_WIDTH", "480"))
GIF_MAX_SECONDS = int(os.environ.get("GIF_MAX_SECONDS", "6"))  # cap clip length we transcode
# Length of the recordings window we pull, measured from the event's start_time.
# Defaults to GIF_MAX_SECONDS (no point grabbing more than we transcode).
GIF_WINDOW_SECONDS = int(os.environ.get("GIF_WINDOW_SECONDS", str(os.environ.get("GIF_MAX_SECONDS", "6"))))
# Wall-clock playback speedup. 1.0 = real time. 3.0 = the same frames render
# in 1/3 the time → motion appears 3× faster. Different from GIF_FPS, which
# controls how smoothly motion is sampled (and file size). Implemented as
# `setpts=PTS/<speedup>` in the ffmpeg filter chain.
GIF_SPEEDUP = float(os.environ.get("GIF_SPEEDUP", "3.0"))
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
CLIP_DIR.mkdir(parents=True, exist_ok=True)

# --- doorbell redirect helper -----------------------------------------------
# We also ship a tiny static HTML page that the notification clickAction points
# at. When tapped, the page reads ?event_id=<id> from its URL, stamps it into
# HA's input_text.doorbell_viewing_event_id via the REST API (using the auth
# token from localStorage, which is same-origin with HA), then redirects to the
# cameras dashboard. The dashboard's "Doorbell clip" card reads that helper to
# show THIS clip, not whatever's currently in the latest-event helper.
#
# Writing it from here (vs a separate sidecar) because we already have the
# cifs mount and the file is small + rarely changes.

_DOORBELL_REDIRECT_HTML = r"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<title>Doorbell — opening clip</title>
<meta name="viewport" content="width=device-width, initial-scale=1, user-scalable=no">
<style>
  html,body{margin:0;height:100%;background:#000;color:#bbb;
    font:14px -apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,sans-serif;
    display:grid;place-items:center;}
  .spinner{width:48px;height:48px;border:4px solid #333;border-top-color:#3a8;
    border-radius:50%;animation:s 1s linear infinite;margin:0 auto 16px;}
  @keyframes s{to{transform:rotate(360deg);}}
  .msg{text-align:center;opacity:.7;}
</style>
</head><body>
<div><div class="spinner"></div><div class="msg">Opening clip…</div></div>
<script>
(async () => {
  const params = new URLSearchParams(window.location.search);
  const eventId = params.get('event_id') || '';
  const target = '/doorbell-clip';

  let token = null;
  try {
    const stored = localStorage.getItem('hassTokens');
    if (stored) token = JSON.parse(stored).access_token;
  } catch (e) {}

  if (eventId && token) {
    try {
      await fetch('/api/services/input_text/set_value', {
        method: 'POST',
        headers: {
          'Authorization': 'Bearer ' + token,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          entity_id: 'input_text.doorbell_viewing_event_id',
          value: eventId
        })
      });
    } catch (e) {}
  }

  window.location.replace(target);
})();
</script>
</body></html>
"""


# Inline HTML5 clip player for the /doorbell-clip dashboard. Embedded in an
# `iframe` card (Lovelace won't render templates in a card's tap_action URL,
# so the dashboard can't build a per-event clip URL itself). This page reads
# input_text.doorbell_viewing_event_id at runtime and plays that exact event's
# clip via Frigate's UNAUTHENTICATED notifications proxy. Same-origin iframe
# inside HA, so localStorage.hassTokens is available for the state read.
_DOORBELL_CLIP_PLAYER_HTML = r"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  html,body{margin:0;height:100%;background:#000}
  #v{width:100%;height:100%;object-fit:contain;background:#000}
  #msg{color:#bbb;font-family:sans-serif;text-align:center;padding:1.2em;font-size:15px}
</style>
</head>
<body>
<video id="v" controls autoplay muted playsinline></video>
<div id="msg" style="display:none">No clip yet.</div>
<script>
(function(){
  var v=document.getElementById('v'), msg=document.getElementById('msg'), tries=0;
  function fail(t){ v.style.display='none'; msg.style.display='block'; if(t) msg.textContent=t; }
  // Fetch the current event id from a PUBLIC json (sidecar-written). No auth
  // token needed. Play the /local clip (byte-range 206 supported, unlike the
  // Frigate notifications proxy which returns 200 and breaks <video>).
  function load(){
    fetch('/local/doorbell-latest.json?t=' + Date.now(), {cache:'no-store'})
     .then(function(r){ return r.json(); })
     .then(function(j){
       var id = j && j.event_id;
       if(!id){ fail('No clip yet.'); return; }
       v.style.display='block'; msg.style.display='none';
       v.src = '/local/frigate_clips/' + id + '.mp4';
     })
     .catch(function(){ fail('No clip yet.'); });
  }
  // The clip is staged ~15-25s after the visitor appears; retry while it 404s.
  v.addEventListener('error', function(){
    if(tries++ < 12){ setTimeout(function(){ v.load(); }, 3000); }
    else { fail('Clip not ready - reload in a moment.'); }
  });
  load();
})();
</script>
</body>
</html>
"""


def install_www_pages() -> None:
    """Drop the static helper pages into HA's www folder. Called at startup;
    overwrites on every boot so updates roll out with sidecar redeploys (cheap).
    The cifs mount roots at the share's `config` so /ha_config/www maps to HA's
    `/config/www`, served at /local/."""
    pages = {
        "doorbell-redirect.html": _DOORBELL_REDIRECT_HTML,
        "doorbell-clip-player.html": _DOORBELL_CLIP_PLAYER_HTML,
    }
    for name, html in pages.items():
        target = Path("/ha_config/www") / name
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(html, encoding="utf-8")
            log.info("www page installed: %s (%d bytes)", target, target.stat().st_size)
        except Exception:
            log.exception("failed to install %s (continuing anyway)", name)


install_www_pages()


def write_latest_pointer(evt_id: str) -> None:
    """Publish the current event id to a PUBLIC json so the /doorbell-clip iframe
    player can fetch it WITHOUT an auth token. Written at match time (event start),
    so the player points at the current visitor; the clip.mp4 itself becomes
    available a little later (player retries)."""
    try:
        (Path("/ha_config/www") / "doorbell-latest.json").write_text(
            json.dumps({"event_id": evt_id, "ts": int(time.time())}), encoding="utf-8")
    except Exception:
        log.exception("failed to write doorbell-latest.json (continuing anyway)")

# Track which event IDs we've already kicked off (with the time we did so, for
# pruning). A noisy /events stream sends many new/update/end messages per event;
# we process the FIRST one that qualifies and ignore the rest for that id.
_handled: dict[str, float] = {}
_handled_lock = threading.Lock()


# --- ffmpeg ------------------------------------------------------------------

def _palette_filter() -> str:
    base = f"fps={GIF_FPS},scale={GIF_WIDTH}:-2:flags=lanczos"
    if GIF_SPEEDUP and abs(GIF_SPEEDUP - 1.0) > 1e-3:
        # setpts compresses the presentation timestamps of each frame, so the
        # same frames play in 1/SPEEDUP the time. Apply BEFORE palettegen so
        # palette stats reflect what will actually be encoded.
        return f"{base},setpts=PTS/{GIF_SPEEDUP}"
    return base


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

def fetch_mp4(url: str, dest: Path, evt_id: str) -> bool:
    """Retry-with-backoff GET on a Frigate mp4 URL. Returns True iff downloaded.

    Used against the recordings export endpoint, whose segments only finalize a
    few seconds after real time — so a fetch for a window that just ended 404/400s
    until Frigate has written it. We back off and retry up to CLIP_FETCH_TIMEOUT_S."""
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
            # 404/400 = window not finalized yet; expected during the backoff.
            if e.code not in (404, 400):
                log.warning("evt=%s clip fetch HTTP %s", evt_id, e.code)
        except URLError as e:
            log.warning("evt=%s clip fetch URL error: %s", evt_id, e)
        time.sleep(delay)
        waited += delay
    log.error("evt=%s clip never became available after %ds", evt_id, waited)
    return False


# --- event handling ----------------------------------------------------------

def sublabel_name(after: dict):
    """Frigate's event `sub_label` is a [name, score] pair (e.g. ['Josh', 0.98]),
    not a bare string — and it's None when unrecognized. Return just the name.

    Indexing [0] matters for correctness AND safety: `['Josh', 0.98] in {'Josh'}`
    raises `TypeError: unhashable type: 'list'`, which previously crashed the MQTT
    loop on every recognized face."""
    sl = after.get("sub_label")
    if isinstance(sl, (list, tuple)):
        return sl[0] if sl else None
    return sl


def should_process(after: dict) -> tuple[bool, str]:
    """Returns (process?, reason). Reason is for log clarity only."""
    if after.get("camera") not in CAMERAS:
        return False, "camera-skip"
    if after.get("label") not in LABELS:
        return False, "label-skip"
    if REQUIRED_ZONE and REQUIRED_ZONE not in (after.get("entered_zones") or []):
        return False, "no-zone"
    if sublabel_name(after) in IGNORE_SUBLABELS:
        return False, "ignored-sublabel"
    return True, "match"


def stage_player_clip(src_mp4: Path, evt_id: str) -> None:
    """Remux the fetched window mp4 into /local/frigate_clips/<id>.mp4 with
    +faststart so the dashboard iframe player can stream it. Served from /local,
    HA honours byte-range (206) which <video> requires — the Frigate notifications
    proxy does not, which is why direct-proxy playback showed a broken-media icon.
    -c copy = no re-encode (h264 already browser-friendly), just a container remux."""
    dest = CLIP_DIR / f"{evt_id}.mp4"
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
             "-i", str(src_mp4), "-c", "copy", "-movflags", "+faststart", str(dest)],
            check=True)
        log.info("evt=%s player clip staged: %s (%d bytes)", evt_id, dest, dest.stat().st_size)
    except subprocess.CalledProcessError as e:
        log.error("evt=%s player-clip remux failed rc=%s", evt_id, e.returncode)
        dest.unlink(missing_ok=True)


def process_event(client: mqtt.Client, evt_id: str, camera: str, start_ts: int) -> None:
    try:
        with tempfile.TemporaryDirectory() as tmp:
            mp4 = Path(tmp) / f"{evt_id}.mp4"
            # Frigate recordings export: build an mp4 for [start, start+window]
            # straight from continuous recordings — no need to wait for the event
            # to end. Available a fixed ~15-20s after the person appears.
            end_ts = start_ts + GIF_WINDOW_SECONDS
            url = f"{FRIGATE_URL}/api/{camera}/start/{start_ts}/end/{end_ts}/clip.mp4"
            if not fetch_mp4(url, mp4, evt_id):
                return
            stage_player_clip(mp4, evt_id)
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
    except Exception:
        log.exception("evt=%s processing failed", evt_id)


def on_message(client: mqtt.Client, _userdata, msg: mqtt.MQTTMessage) -> None:
    if msg.topic != "frigate/events":
        return
    try:
        payload = json.loads(msg.payload)
    except json.JSONDecodeError:
        return
    # Act on the FIRST message (new/update/end) where the visitor has entered the
    # required zone — we no longer wait for type=end, since we pull from recordings.
    if payload.get("type") not in ("new", "update", "end"):
        return
    after = payload.get("after") or {}
    ok, _reason = should_process(after)
    if not ok:
        return
    evt_id = after.get("id")
    start_time = after.get("start_time")
    if not evt_id or not start_time:
        return
    with _handled_lock:
        if evt_id in _handled:
            return
        _handled[evt_id] = time.time()
    log.info("evt=%s match (camera=%s label=%s sub_label=%s type=%s) — grabbing [start,+%ds] from recordings",
             evt_id, after.get("camera"), after.get("label"), sublabel_name(after),
             payload.get("type"), GIF_WINDOW_SECONDS)
    write_latest_pointer(evt_id)
    # offload to a worker thread so MQTT loop stays responsive
    threading.Thread(
        target=process_event,
        args=(client, evt_id, after.get("camera"), int(start_time)),
        daemon=True,
    ).start()


# --- cleanup -----------------------------------------------------------------

def cleanup_old_gifs() -> None:
    cutoff = time.time() - GIF_RETENTION_HOURS * 3600
    removed = 0
    for old in list(OUTPUT_DIR.glob("*.gif")) + list(CLIP_DIR.glob("*.mp4")):
        try:
            if old.stat().st_mtime < cutoff:
                old.unlink()
                removed += 1
        except FileNotFoundError:
            pass
    if removed:
        log.info("cleanup: removed %d old gif/clip file(s) older than %dh", removed, GIF_RETENTION_HOURS)
    # prune the de-dup map — an event id never recurs after a couple hours.
    with _handled_lock:
        stale = [k for k, t in _handled.items() if t < time.time() - 7200]
        for k in stale:
            _handled.pop(k, None)


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
        "gif=%dx_@%dfps speedup=%.1fx cap=%ds out=%s retain=%dh",
        FRIGATE_URL, MQTT_HOST, MQTT_PORT, CAMERAS, LABELS, REQUIRED_ZONE,
        IGNORE_SUBLABELS, GIF_WIDTH, GIF_FPS, GIF_SPEEDUP, GIF_MAX_SECONDS, OUTPUT_DIR, GIF_RETENTION_HOURS,
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
