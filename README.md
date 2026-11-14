# AHD Browser Downloader

This is product 1 of AHD. By default it serves the UI on your PC: direct-file downloads use the local queue and save in Windows Downloads. Queue history and resumable data are stored under `%LOCALAPPDATA%\AHD`. Set `AHD_PUBLIC_MODE=1` to enable the optional hosted demo described below.

## Setup on Windows

1. Install Python 3.11 or newer.
2. Double-click `setup.bat`. It creates a local Python environment and installs the open-source yt-dlp extractor package.
3. Install FFmpeg for qualities that provide separate video and audio tracks. AHD checks PATH and common Windows install folders. For a custom location, set the `AHD_FFMPEG` environment variable to the `ffmpeg.exe` path before starting AHD. For full current YouTube format discovery, install Deno 2.3+ (recommended) or Node.js 22+. AHD enables Node for yt-dlp automatically. The `yt-dlp[default]` dependency includes yt-dlp's EJS challenge scripts.
4. Double-click `run.bat`. Keep the command window open while using AHD; it opens `http://127.0.0.1:8765/`.
5. Paste a direct file URL and click **Find media**, then press **Download** on the result card to add it to the queue. For a supported video platform, click its shortcut, paste the link, and choose **Find media**. A medium popup shows the analysis progress; when ready, select an available quality and download. The extension's **AHD video** player button uses this same local extractor when `run.bat` is active. Set an optional per-download speed limit above the form. Pause, resume, cancel, and retry direct-file jobs from **Download queue & history**. AHD checks the URL suffix, response headers, and file signature—including image or media links without a filename extension—before trying a video-site extractor. For supported video-site pages, it lists the formats returned by the extractor. For ordinary public webpages, use **Find downloads on this page** to list static links and media.
6. Press Ctrl+C in the command window to stop AHD.

These components are free to use. Initial site-extractor routing covers YouTube, Instagram, TikTok, Facebook, Reddit, Pinterest, Vimeo, Twitch, Dailymotion, and X/Twitter. Actual support depends on the current extractor and what each page makes available. Extractor support changes as websites change, so run `setup.bat` again to update the installed yt-dlp package if a supported site stops working.

## Free hosted demo on Render

The included `Dockerfile` and `render.yaml` configure a small Render free web service. To deploy, push this project to a GitHub repository, create a Render account, choose **New → Blueprint**, select that repository, and set the Blueprint file path to `outputs/AHD/browser/render.yaml`. Render builds the Docker image with Python, Node.js 22, FFmpeg, and yt-dlp. The service uses temporary disk only; downloads are streamed to visitors rather than retained as shared files.

The public demo caps each file at 400 MB, allows one transfer at a time, and limits each client to 20 analyze requests per 10 minutes. It disables the local queue/history and browser extension API. This is suitable for a small proof of concept, not a reliable public launch: free web services sleep when idle, have no persistent filesystem, and the free workspace has a small monthly outbound bandwidth allowance. Every download consumes service bandwidth, so the allowance can run out quickly. See Render's [free instance limits](https://render.com/docs/free) and [outbound bandwidth rules](https://render.com/docs/outbound-bandwidth).

## Safety and current limits

- The service binds to `127.0.0.1`, requires a random per-run token, blocks private/local addresses in the direct-link inspector (including its redirects), and caps inspected HTML pages at 5 MB. The site extractor runs locally only for the listed public video-site hosts.
- AHD does not use your browser cookies or login. Private, login-only, DRM-protected, and blocked media is unavailable. The extractor is not configured to bypass those protections.
- Formats and resolutions depend on what the site exposes. AHD lists the available heights and shows 1080p when YouTube exposes it. If a quality has separate video and audio streams, FFmpeg is required to combine them; the UI marks these choices when FFmpeg is unavailable. YouTube may return fewer formats without a supported JavaScript runtime. See the [yt-dlp EJS setup guide](https://github.com/yt-dlp/yt-dlp/wiki/EJS).
- Direct-file jobs retry connection failures, resume from saved partial data when the server supports HTTP Range, and use up to four parallel ranges for sufficiently large files when supported. If parallel ranges are rejected, AHD falls back to one stream. Duplicate completed files are detected by SHA-256 and are not saved twice. Speed limits apply per download.
- Static page inspection reads ordinary HTML links and media tags; it does not run the webpage's JavaScript. Browser extensions are planned as product 2 for media rendered in webpages.
- The hosted demo remains a prototype. Do not advertise it as unlimited or guaranteed; pause deployment if abuse or unexpected bandwidth use appears.
