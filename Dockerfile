FROM node:22-bookworm-slim

ENV PYTHONUNBUFFERED=1 \
    AHD_PUBLIC_MODE=1 \
    VIRTUAL_ENV=/opt/ahd-venv \
    PATH="/opt/ahd-venv/bin:$PATH"

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-venv ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv "$VIRTUAL_ENV"

WORKDIR /app
COPY requirements.txt server.py download_engine.py index.html ./
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "server.py"]
