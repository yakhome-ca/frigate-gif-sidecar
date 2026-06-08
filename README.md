# frigate-gif-sidecar

Phase 2 of the [doorbell notification pipeline](../../README.md). Builds the
**higher-quality animated GIF preview** that silently swaps into the active
notification ~20–40s after the initial snapshot buzz.

## What it does

```
            ┌──────────────────────────────────────────────────────────┐
 frigate ──►│ MQTT frigate/events  type=end                            │
            │   ├─ camera=doorbell                                      │
            │   ├─ label=person                                          │
            │   ├─ 'property' in entered_zones                          │
            │   └─ sub_label != 'Josh'                                  │
            └─────────────┬────────────────────────────────────────────┘
                          ▼
            GET /api/events/<id>/clip.mp4  (retry-with-backoff, ~45s budget)
                          ▼
            ffmpeg two-pass palette:
              pass 1: fps=5,scale=720,palettegen=stats_mode=diff
              pass 2: paletteuse=dither=bayer:bayer_scale=5
                          ▼
            write /output/<id>.gif      (≈ 1.5–2 MB for ~10s)
                          ▼
            MQTT publish: frigate-gifs/ready/<id>
                          ▼
   HA automation.doorbell_gif_ready_update
   silent same-tag update + image: /local/frigate_gifs/<id>.gif
```

## Why a sidecar (not HA shell_command)

Frigate clip transcodes take 2–10s of CPU. Doing that inside HA's container
holds the recorder/event loop while ffmpeg runs, plus HA's container intentionally
doesn't ship a writable `/tmp` palette workflow. A tiny sidecar is one container,
~150 lines of Python, and stays out of the way of HA.

## Pre-deploy checklist

HA runs in HA-OS in a Proxmox VM, so we write to its `/config/www` via the
**Samba share addon** (already installed) using Docker's built-in `cifs`
volume driver. No bind-mount, no host fstab changes.

1. **Copy `.env.example` → `.env`** and fill in:
   - `MQTT_PASS` — Frigate's MQTT user works as-is
   - `SMB_USER` / `SMB_PASS` — from the HA UI: Settings → Add-ons → Samba share →
     Configuration tab. The username defaults to `josh`.
   - `HA_HOST` — `192.168.10.103` for the yakhome install.

2. **Deploy via Komodo**: stack is `yakhome-ca/frigate-gif-sidecar`. Let Komodo
   clone, build the image, and bring it up.

3. The cifs volume mount needs the host kernel `cifs` module — Debian ships it
   by default, so home-debian is fine.

## Verifying it's working

After someone (not Josh) walks up to the doorbell:

```bash
# 1. The sidecar logged a match + ffmpeg + ready-publish:
docker logs frigate-gif-sidecar --tail 50

# 2. A GIF file exists (from any host that can hit HA's Samba share):
smbclient //$HA_HOST/config -U $SMB_USER -c 'ls www\frigate_gifs\*.gif'

# 3. The ready topic was published (run before walking up):
mosquitto_sub -h 192.168.10.103 -u mqtt -P "$PASS" -t 'frigate-gifs/ready/+' -v

# 4. The notification on your phone silently swapped from snapshot → GIF.
```

## Tuning

All in `.env`:

| Var | Default | Effect |
|---|---|---|
| `GIF_FPS` | `3` | Higher = smoother, larger file. 3fps feels "security camera" flippy in a good way. |
| `GIF_WIDTH` | `480` | Output width (height auto, preserves aspect). 480 sits under Android's notif-image soft budget without thumbnailing. |
| `GIF_MAX_SECONDS` | `8` | Cap on transcoded clip length. 8s catches arrival + dwell without ballooning the GIF. |
| `GIF_RETENTION_HOURS` | `24` | Auto-deleted from disk after this |
| `IGNORE_SUBLABELS` | `Josh` | Comma-separated faces to skip entirely |

## Falls back gracefully

If MQTT drops → paho auto-reconnect.
If clip.mp4 never finalizes within `CLIP_FETCH_TIMEOUT_S` → log + skip; the
notification stays on the still snapshot. No HA-side impact.
If ffmpeg fails → log rc + skip. Same.

The pipeline degrades to "snapshot only" — never breaks the buzz.
