FROM python:3.12-slim

# ffmpeg from Debian repos; ca-certs for TLS against Frigate if you ever go HTTPS
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py /app/main.py

# Output dir is a CIFS-mounted HA Samba share — uid/gid 1000 in the compose
# matches HA-OS's Samba addon write-mapping, so we run as 1000 too for clean
# write permissions on the share.
ENV PYTHONUNBUFFERED=1
USER 1000:1000
CMD ["python", "-u", "main.py"]
