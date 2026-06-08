FROM python:3.12-slim

# ffmpeg from Debian repos; ca-certs for TLS against Frigate if you ever go HTTPS
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py /app/main.py

# Output dir is bind-mounted at runtime to HA's /config/www/frigate_gifs.
# The mkdir in main.py covers the case where the bind isn't created yet.
ENV PYTHONUNBUFFERED=1
USER nobody
CMD ["python", "-u", "main.py"]
