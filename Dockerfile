FROM node:20-slim

# ffmpeg + python for yt-dlp (resolves YouTube/Facebook links to direct streams)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg python3 python3-pip curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --break-system-packages --no-cache-dir yt-dlp

WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev
COPY . .

# Prefer the system yt-dlp installed above; the bundled bin/yt-dlp is a fallback.
ENV NODE_ENV=production
ENV PORT=10000
ENV YTDLP_PATH=/usr/local/bin/yt-dlp
EXPOSE 10000

CMD ["node", "server.js"]
