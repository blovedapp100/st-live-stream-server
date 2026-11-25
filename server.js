/**
 * Live Stream Restream Server
 * ============================
 * Upload a video (or paste a video/YouTube/Facebook link), pick one or more
 * RTMP destinations (YouTube / Facebook / Twitch / Kick / custom), and this
 * server re-streams it with FFmpeg. The stream runs on the SERVER, so closing
 * the phone app or turning off the phone's internet does NOT stop it.
 *
 * Endpoints:
 *   GET  /health                  - liveness check
 *   POST /api/jobs                - create + start a job (multipart upload OR JSON {videoUrl})
 *   GET  /api/jobs                - list jobs (stream keys NEVER included)
 *   GET  /api/jobs/:id            - one job: status, uptime, ffmpeg log tail
 *   POST /api/jobs/:id/stop       - kill ffmpeg, clean up, mark stopped
 *
 * Stream keys are held ONLY in memory for the running job. They are never
 * logged, never written to disk, and never returned by any GET endpoint.
 *
 * Streaming modes (env STREAM_MODE):
 *   multi (default) - one ffmpeg process per destination. One destination
 *                     failing cannot kill the others. Proven reliable.
 *   tee             - one ffmpeg process, -f tee to all destinations.
 *                     Cheaper on CPU but one dead destination can stall
 *                     the whole tee output.
 */

'use strict';

const express = require('express');
const multer = require('multer');
const { spawn, execFile } = require('child_process');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// ---------------------------------------------------------------- config ---
const PORT = parseInt(process.env.PORT || '3000', 10);
const UPLOAD_DIR = path.join(__dirname, 'uploads');
const FFMPEG = process.env.FFMPEG_PATH || 'ffmpeg';
const FFPROBE = process.env.FFPROBE_PATH || 'ffprobe';
// Bundled yt-dlp binary (server/bin/yt-dlp) wins; fall back to system yt-dlp.
const BUNDLED_YTDLP = path.join(__dirname, 'bin', 'yt-dlp');
const YTDLP = process.env.YTDLP_PATH ||
  (fs.existsSync(BUNDLED_YTDLP) ? BUNDLED_YTDLP : 'yt-dlp');
const STREAM_MODE = (process.env.STREAM_MODE || 'multi').toLowerCase() === 'tee'
  ? 'tee' : 'multi';
const MAX_LOG_LINES = 200;
const MAX_UPLOAD_BYTES = 2 * 1024 * 1024 * 1024; // 2 GB

fs.mkdirSync(UPLOAD_DIR, { recursive: true });

// RTMP ingest presets (server-side, so the app never hardcodes them).
const PRESETS = {
  youtube: 'rtmp://a.rtmp.youtube.com/live2',
  facebook: 'rtmps://live-api-s.facebook.com:443/rtmp',
  twitch: 'rtmp://live.twitch.tv/app',
  kick: 'rtmp://live.kick.com/app',
  custom: null,
};

const app = express();
app.use(express.json({ limit: '1mb' }));

// ------------------------------------------------------------- job store ---
// In-memory only. Stream keys live here and nowhere else.
const jobs = new Map(); // id -> job

function newJobId() {
  return crypto.randomBytes(6).toString('hex');
}

function publicJob(job) {
  return {
    id: job.id,
    title: job.title,
    loop: job.loop,
    sourceType: job.sourceType,
    videoUrl: job.sourceType === 'url' ? job.videoUrl : undefined,
    fileName: job.fileName || undefined,
    destinations: job.destinations.map((d) => ({
      platform: d.platform,
      rtmpUrl: d.rtmpUrl, // base URL only, WITHOUT the stream key
    })),
    status: job.status, // uploading | starting | live | completed | stopped | error
    mode: job.mode,
    error: job.error || undefined,
    startedAt: job.startedAt || null,
    stoppedAt: job.stoppedAt || null,
    uptimeSec: job.startedAt && job.status === 'live'
      ? Math.floor((Date.now() - job.startedAt) / 1000) : (job.uptimeSec || 0),
    logTail: job.log.slice(-30),
  };
}

function redact(job, text) {
  let out = String(text);
  for (const d of job.destinations || []) {
    if (d.streamKey) out = out.split(d.streamKey).join('***');
  }
  return out;
}

function pushLog(job, line) {
  const clean = redact(job, line).trim();
  if (!clean) return;
  job.log.push(clean);
  if (job.log.length > MAX_LOG_LINES) job.log.splice(0, job.log.length - MAX_LOG_LINES);
}

// ------------------------------------------------------------ upload setup --
const storage = multer.diskStorage({
  destination: UPLOAD_DIR,
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname || '') || '.mp4';
    cb(null, `${newJobId()}${ext}`);
  },
});
const upload = multer({
  storage,
  limits: { fileSize: MAX_UPLOAD_BYTES },
});

// --------------------------------------------------------- source resolving --
const DIRECT_EXT = /\.(mp4|m4v|mov|webm|mkv|m3u8)(\?|#|$)/i;

function isDirectMediaUrl(url) {
  return DIRECT_EXT.test(url) || url.includes('googlevideo.com');
}

/** Resolve a pasted link to a direct media URL ffmpeg can read. */
function resolveVideoUrl(videoUrl) {
  return new Promise((resolve, reject) => {
    if (isDirectMediaUrl(videoUrl)) return resolve(videoUrl);
    // YouTube / Facebook / other pages: ask yt-dlp for the best direct stream.
    execFile(YTDLP, [
      '-g',
      '-f', 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
      '--no-playlist',
      videoUrl,
    ], { timeout: 60000 }, (err, stdout, stderr) => {
      if (err) {
        const msg = (stderr || err.message || '').trim().split('\n').slice(-2).join(' ');
        return reject(new Error(`yt-dlp could not resolve link: ${msg || 'unknown error'}`));
      }
      const first = stdout.split('\n').map((s) => s.trim()).filter(Boolean)[0];
      if (!first) return reject(new Error('yt-dlp returned no stream URL'));
      resolve(first);
    });
  });
}

/** True if the file/URL has at least one audio stream (ffprobe). */
function hasAudio(source) {
  return new Promise((resolve) => {
    execFile(FFPROBE, [
      '-v', 'error',
      '-select_streams', 'a',
      '-show_entries', 'stream=index',
      '-of', 'csv=p=0',
      source,
    ], { timeout: 30000 }, (err, stdout) => {
      if (err) return resolve(false); // assume no audio; anullsrc fallback covers it
      resolve(stdout.trim().length > 0);
    });
  });
}

// ----------------------------------------------------------- ffmpeg build ---
/**
 * Build ffmpeg args for ONE destination.
 * - Keeps aspect ratio, never stretches (vertical video stays vertical).
 * - Forces even dimensions (yuv420p requires them) without changing ratio.
 * - Silent video gets a silent audio track (anullsrc) so RTMP ingest is happy.
 */
function buildFfmpegArgs({ source, loop, audio, rtmpUrl }) {
  const args = [];
  if (loop) args.push('-stream_loop', '-1');
  args.push('-re', '-i', source);
  if (!audio) {
    args.push('-f', 'lavfi', '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100');
  }
  args.push(
    '-map', '0:v:0',
    '-map', audio ? '0:a:0' : '1:a:0',
    '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuv420p',
    '-c:v', 'libx264',
    '-preset', 'veryfast',
    '-maxrate', '4500k',
    '-bufsize', '9000k',
    '-g', '60',
    '-c:a', 'aac',
    '-b:a', '128k',
    '-ar', '44100',
    '-f', 'flv',
    rtmpUrl,
  );
  return args;
}

function buildTeeArgs({ source, loop, audio, rtmpUrls }) {
  const args = [];
  if (loop) args.push('-stream_loop', '-1');
  args.push('-re', '-i', source);
  if (!audio) {
    args.push('-f', 'lavfi', '-i', 'anullsrc=channel_layout=stereo:sample_rate=44100');
  }
  const teeTargets = rtmpUrls.map((u) => `[f=flv]${u}`).join('|');
  args.push(
    '-map', '0:v:0',
    '-map', audio ? '0:a:0' : '1:a:0',
    '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2,format=yuv420p',
    '-c:v', 'libx264',
    '-preset', 'veryfast',
    '-maxrate', '4500k',
    '-bufsize', '9000k',
    '-g', '60',
    '-c:a', 'aac',
    '-b:a', '128k',
    '-ar', '44100',
    '-f', 'tee',
    teeTargets,
  );
  return args;
}

function fullRtmpUrl(dest) {
  // dest.rtmpUrl is the base ingest URL; streamKey is appended, never logged.
  const base = (dest.rtmpUrl || '').replace(/\/+$/, '');
  if (dest.streamKey) return `${base}/${dest.streamKey}`;
  return base;
}

// -------------------------------------------------------------- job runner --
async function startJob(job) {
  job.status = 'starting';
  pushLog(job, `Starting job "${job.title}" (${job.mode} mode, loop=${job.loop})`);
  try {
    let source;
    if (job.sourceType === 'upload') {
      source = job.filePath;
    } else {
      pushLog(job, 'Resolving pasted link…');
      source = await resolveVideoUrl(job.videoUrl);
      pushLog(job, 'Link resolved to a direct stream.');
    }
    job.resolvedSource = source;

    const audio = await hasAudio(source);
    pushLog(job, audio ? 'Source has audio.' : 'Source has no audio — adding silent audio track.');

    const spawnOne = (args, label) => {
      const proc = spawn(FFMPEG, args, { stdio: ['ignore', 'pipe', 'pipe'] });
      proc.stderr.on('data', (d) => {
        for (const line of String(d).split('\n')) pushLog(job, `[${label}] ${line}`);
      });
      proc.on('error', (err) => {
        pushLog(job, `[${label}] failed to start ffmpeg: ${err.message}`);
        if (job.status === 'live' || job.status === 'starting') {
          job.status = 'error';
          job.error = `ffmpeg failed to start (${label}): ${err.message}`;
        }
      });
      proc.on('exit', (code, signal) => {
        pushLog(job, `[${label}] ffmpeg exited code=${code} signal=${signal || '-'}`);
        if (job.status === 'live' || job.status === 'starting') {
          if (job.stopping) return; // intentional stop
          job.uptimeSec = job.startedAt ? Math.floor((Date.now() - job.startedAt) / 1000) : 0;
          if (code === 0 && !job.loop) {
            // Non-loop job played the whole video and finished cleanly.
            job.status = 'completed';
            job.stoppedAt = Date.now();
            pushLog(job, `[${label}] Stream finished (end of video, loop off).`);
            cleanupUpload(job);
            return;
          }
          job.status = 'error';
          job.error = `ffmpeg exited unexpectedly (${label}, code ${code})`;
        }
      });
      return proc;
    };

    if (job.mode === 'tee') {
      const urls = job.destinations.map(fullRtmpUrl);
      job.procs = [spawnOne(buildTeeArgs({
        source, loop: job.loop, audio, rtmpUrls: urls,
      }), 'tee')];
    } else {
      job.procs = job.destinations.map((d) => spawnOne(buildFfmpegArgs({
        source, loop: job.loop, audio, rtmpUrl: fullRtmpUrl(d),
      }), d.platform));
    }

    job.startedAt = Date.now();
    job.status = 'live';
    pushLog(job, `LIVE — streaming to ${job.destinations.length} destination(s).`);
  } catch (err) {
    job.status = 'error';
    job.error = err.message;
    pushLog(job, `ERROR: ${err.message}`);
  }
}

function cleanupUpload(job) {
  if (job.sourceType === 'upload' && job.filePath) {
    fs.unlink(job.filePath, () => {});
    job.filePath = null;
  }
}

function stopJob(job) {
  job.stopping = true;
  job.uptimeSec = job.startedAt ? Math.floor((Date.now() - job.startedAt) / 1000) : 0;
  for (const proc of job.procs || []) {
    try { proc.kill('SIGTERM'); } catch (_) { /* already dead */ }
    // Force-kill if it ignores SIGTERM.
    setTimeout(() => { try { proc.kill('SIGKILL'); } catch (_) { /* already dead */ } }, 3000);
  }
  job.procs = [];
  job.stoppedAt = Date.now();
  job.status = 'stopped';
  pushLog(job, 'Stopped by request.');
  // Clean up an uploaded source file; URL jobs have nothing on disk.
  cleanupUpload(job);
}

// --------------------------------------------------------------- endpoints --
app.get('/health', (req, res) => {
  res.json({
    ok: true,
    mode: STREAM_MODE,
    ffmpeg: FFMPEG,
    ytdlp: fs.existsSync(YTDLP) ? YTDLP : 'not found',
    jobs: jobs.size,
    uptimeSec: Math.floor(process.uptime()),
  });
});
app.get('/api/health', (req, res) => res.redirect('/health'));

function parseDestinations(body) {
  let dests = body.destinations;
  if (typeof dests === 'string') {
    try { dests = JSON.parse(dests); } catch (_) { dests = null; }
  }
  if (!Array.isArray(dests) || dests.length === 0) {
    throw new Error('destinations is required: [{platform, rtmpUrl?, streamKey?}]');
  }
  return dests.map((d) => {
    const platform = String(d.platform || 'custom').toLowerCase();
    let rtmpUrl = d.rtmpUrl || PRESETS[platform] || null;
    if (!rtmpUrl) throw new Error(`platform "${platform}" needs an rtmpUrl (no preset)`);
    if (!d.streamKey && !PRESETS[platform]) {
      // Custom RTMP URL may already contain the key; allow it.
    }
    return { platform, rtmpUrl, streamKey: d.streamKey || '' };
  });
}

app.post('/api/jobs', upload.single('video'), async (req, res) => {
  try {
    const body = req.body || {};
    const title = String(body.title || 'Untitled stream').slice(0, 200);
    const loop = body.loop === true || body.loop === 'true';
    const destinations = parseDestinations(body);

    const job = {
      id: newJobId(),
      title,
      loop,
      destinations,
      status: 'uploading',
      mode: STREAM_MODE,
      log: [],
      procs: [],
      stopping: false,
      startedAt: null,
      stoppedAt: null,
      uptimeSec: 0,
      error: null,
    };

    if (req.file) {
      job.sourceType = 'upload';
      job.filePath = req.file.path;
      job.fileName = req.file.originalname;
      pushLog(job, `Uploaded ${req.file.originalname} (${req.file.size} bytes).`);
    } else if (body.videoUrl) {
      job.sourceType = 'url';
      job.videoUrl = String(body.videoUrl).trim();
      if (!/^https?:\/\//i.test(job.videoUrl)) {
        return res.status(400).json({ error: 'videoUrl must start with http:// or https://' });
      }
    } else {
      return res.status(400).json({
        error: 'Provide a video file (multipart field "video") or JSON {videoUrl}.',
      });
    }

    jobs.set(job.id, job);
    // Start async — the response returns immediately with status "starting".
    startJob(job).catch((err) => {
      job.status = 'error';
      job.error = err.message;
    });
    res.status(201).json(publicJob(job));
  } catch (err) {
    // Clean up a file multer already saved if validation failed afterwards.
    if (req.file) fs.unlink(req.file.path, () => {});
    res.status(400).json({ error: err.message });
  }
});

app.get('/api/jobs', (req, res) => {
  res.json([...jobs.values()].map(publicJob));
});

app.get('/api/jobs/:id', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'job not found' });
  res.json(publicJob(job));
});

app.post('/api/jobs/:id/stop', (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: 'job not found' });
  if (job.status === 'stopped') return res.json(publicJob(job));
  stopJob(job);
  res.json(publicJob(job));
});

// ------------------------------------------------------------------- boot --
app.listen(PORT, () => {
  console.log(`live-stream-server listening on :${PORT} (mode=${STREAM_MODE})`);
  console.log(`ffmpeg: ${FFMPEG} | yt-dlp: ${fs.existsSync(YTDLP) ? YTDLP : 'NOT FOUND — YouTube/Facebook links will fail, direct mp4/m3u8 still works'}`);
});
