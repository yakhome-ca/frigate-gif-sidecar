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

1. **Find HA's www host path** (so the sidecar writes where HA serves):

   ```bash
   docker inspect homeassistant \
     --format '{{ range .Mounts }}{{ if eq .Destination "/config" }}{{ .Source }}{{ end }}{{ end }}'
   ```

   Append `/www`. Put it in `.env` as `HA_WWW_PATH`.

   The sidecar will write to `$HA_WWW_PATH/frigate_gifs/` — HA serves that at
   `/local/frigate_gifs/<id>.gif`.

2. **Copy `.env.example` → `.env`**, fill in `HA_WWW_PATH` and `MQTT_PASS`. The
   Frigate MQTT user works as-is.

3. **Deploy via Komodo**: add this folder to your stacks repo, point a new
   Komodo stack at it, let Komodo build the image and bring it up.

## Verifying it's working

After someone (not Josh) walks up to the doorbell:

```bash
# 1. The sidecar logged a match + ffmpeg + ready-publish:
docker logs frigate-gif-sidecar --tail 50

# 2. A GIF file exists:
ls -lh $HA_WWW_PATH/frigate_gifs/ | tail

# 3. The ready topic was published (run before walking up):
mosquitto_sub -h 192.168.10.103 -u mqtt -P "$PASS" -t 'frigate-gifs/ready/+' -v

# 4. The notification on your phone silently swapped from snapshot → GIF.
```

## Tuning

All in `.env`:

| Var | Default | Effect |
|---|---|---|
| `GIF_FPS` | `5` | Higher = smoother, larger file |
| `GIF_WIDTH` | `720` | Output width (height auto, preserves aspect) |
| `GIF_MAX_SECONDS` | `12` | Cap on transcoded clip length |
| `GIF_RETENTION_HOURS` | `24` | Auto-deleted from disk after this |
| `IGNORE_SUBLABELS` | `Josh` | Comma-separated faces to skip entirely |

## Falls back gracefully

If MQTT drops → paho auto-reconnect.
If clip.mp4 never finalizes within `CLIP_FETCH_TIMEOUT_S` → log + skip; the
notification stays on the still snapshot. No HA-side impact.
If ffmpeg fails → log rc + skip. Same.

The pipeline degrades to "snapshot only" — never breaks the buzz.
