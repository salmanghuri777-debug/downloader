#!/usr/bin/env python3
"""AHD local browser downloader with a persistent local direct-download queue."""
from __future__ import annotations

import email.message
import html.parser
import http.client
import http.server
import importlib.util
import ipaddress
import json
import mimetypes
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from download_engine import DownloadEngine, JobError

HOST = "127.0.0.1"
PORT = 8765
PUBLIC_MODE = os.getenv("AHD_PUBLIC_MODE", "").lower() in {"1", "true", "yes"}
PUBLIC_MAX_BYTES = 400 * 1024 * 1024
PUBLIC_DOWNLOAD_SLOT = threading.BoundedSemaphore(1)
PUBLIC_RATE_LOCK = threading.Lock()
PUBLIC_REQUESTS: dict[str, list[float]] = {}
ROOT = Path(__file__).resolve().parent
TOKEN = secrets.token_urlsafe(32)
MAX_INSPECT_BYTES = 5 * 1024 * 1024
MAX_REQUEST_BYTES = 16 * 1024
TIMEOUT_SECONDS = 25
PROBE_TIMEOUT_SECONDS = 6
EXTRACT_TIMEOUT_SECONDS = 120
DOWNLOAD_TIMEOUT_SECONDS = 60 * 60
ALLOWED_HOST_HEADERS = {f"{HOST}:{PORT}"}
ALLOWED_EXTENSION_ORIGIN = "chrome-extension://dndidenfjhhjomhggmiodhbddnfjpibg"
EXTRACTIONS: dict[str, dict] = {}
EXTRACTIONS_LOCK = threading.Lock()
DOWNLOAD_ENGINE: DownloadEngine | None = None
EXTRACTION_HOSTS = {
    "youtube.com", "youtu.be", "instagram.com", "tiktok.com", "facebook.com", "fb.watch",
    "reddit.com", "redd.it", "pinterest.com", "pin.it", "vimeo.com", "twitch.tv",
    "dailymotion.com", "x.com", "twitter.com",
}
FILE_EXTENSIONS = {
    ".3gp", ".7z", ".aac", ".aiff", ".apk", ".avi", ".avif", ".bmp", ".csv", ".doc", ".docx", ".epub",
    ".flac", ".flv", ".gif", ".gz", ".heic", ".heif", ".ico", ".jpeg", ".jpg", ".m4a",
    ".m4v", ".mkv", ".mov", ".mp3", ".mp4", ".mpeg", ".mpg", ".oga", ".ogg", ".ogv",
    ".json", ".md", ".odg", ".odp", ".ods", ".odt", ".pdf", ".png", ".ppt", ".pptx", ".rar", ".rtf",
    ".svg", ".tar", ".tif", ".tiff", ".txt", ".wav", ".webm", ".webp", ".xls", ".xlsx", ".xml", ".zip",
}
DIRECT_FILE_EXTENSIONS = FILE_EXTENSIONS | {
    ".apk", ".epub", ".html", ".htm", ".json", ".md", ".odg", ".odp", ".ods", ".odt",
    ".rtf", ".xhtml", ".xml",
}


class DownloadError(Exception):
    pass


def validate_public_url(value: str) -> str:
    """Normalize a public HTTP(S) URL and reject local/private network targets."""
    try:
        parsed = urllib.parse.urlsplit(value.strip())
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
            raise DownloadError("Use a complete public HTTP or HTTPS URL.")
        if parsed.username or parsed.password:
            raise DownloadError("URLs containing usernames or passwords are not supported.")
        host = parsed.hostname.rstrip(".").lower()
        if host in {"localhost", "localhost.localdomain"} or host.endswith((".localhost", ".local", ".internal")):
            raise DownloadError("Local network URLs are blocked.")
        port = parsed.port or (443 if parsed.scheme.lower() == "https" else 80)
        try:
            addresses = {entry[4][0] for entry in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)}
        except OSError as exc:
            raise DownloadError("The host could not be resolved. Check the URL and your connection.") from exc
        if not addresses:
            raise DownloadError("The host did not resolve to an address.")
        for address in addresses:
            try:
                if not ipaddress.ip_address(address.split("%", 1)[0]).is_global:
                    raise DownloadError("Local and private network addresses are blocked.")
            except ValueError as exc:
                raise DownloadError("The host resolved to an invalid address.") from exc
        # Links copied out of Markdown/chat sometimes retain escapes such as
        # "\\_" or "\\&". Those backslashes corrupt opaque image query tokens.
        query = re.sub(r"(?i)%5c(?=[&_])", "", parsed.query).replace("\\_", "_").replace("\\&", "&")
        return urllib.parse.urlunsplit((parsed.scheme.lower(), parsed.netloc, parsed.path or "/", query, ""))
    except ValueError as exc:
        raise DownloadError("The URL is malformed.") from exc


class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        validate_public_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), SafeRedirectHandler())


def fetch(url: str, range_header: str | None = None, timeout: int = TIMEOUT_SECONDS):
    url = validate_public_url(url)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "image/avif,image/webp,image/apng,image/*,video/*,audio/*,*/*;q=0.8",
    }
    host = (urllib.parse.urlsplit(url).hostname or "").lower()
    if re.fullmatch(r"encrypted-tbn\d+\.gstatic\.com", host):
        headers["Referer"] = "https://www.google.com/"
    if range_header:
        headers["Range"] = range_header[:200]
    request = urllib.request.Request(url, headers=headers)
    try:
        return OPENER.open(request, timeout=timeout)
    except urllib.error.HTTPError as exc:
        detail = {401: "This file requires sign-in.", 403: "The site refused the download (403).", 404: "The file was not found (404).", 429: "The site is rate limiting requests. Try again later."}.get(exc.code, f"The site returned HTTP {exc.code}.")
        exc.close()
        raise DownloadError(detail) from exc
    except urllib.error.URLError as exc:
        reason = getattr(exc, "reason", None)
        if isinstance(reason, TimeoutError):
            raise DownloadError("The site took too long to respond.") from exc
        raise DownloadError("Could not connect to that file. The site may block direct downloads.") from exc
    except TimeoutError as exc:
        raise DownloadError("The site took too long to respond.") from exc
    except (OSError, http.client.HTTPException) as exc:
        raise DownloadError("The connection closed before the file could be read.") from exc


def safe_filename(value: str) -> str:
    name = value.replace("\\", "/").split("/")[-1]
    name = "".join("_" if ord(ch) < 32 or ch in '<>:"/\\|?*' else ch for ch in name).strip(" .")
    return name[:180] or "ahd-download"


def find_ffmpeg() -> Path | None:
    """Find FFmpeg on PATH, via AHD_FFMPEG, or in common Windows install folders."""
    candidates: list[Path] = []
    on_path = shutil.which("ffmpeg") or shutil.which("ffmpeg.exe")
    if on_path:
        candidates.append(Path(on_path))
    configured = os.environ.get("AHD_FFMPEG", "").strip().strip('"')
    if configured:
        configured_path = Path(configured).expanduser()
        if configured_path.is_dir():
            candidates.extend((configured_path / "ffmpeg.exe", configured_path / "ffmpeg"))
        else:
            candidates.append(configured_path)
    if os.name == "nt":
        install_roots = [
            Path("C:/ffmpeg/bin"),
            Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "ffmpeg" / "bin",
            Path(os.environ.get("ProgramFiles(x86)", "C:/Program Files (x86)")) / "ffmpeg" / "bin",
            Path(os.environ.get("ProgramData", "C:/ProgramData")) / "chocolatey" / "bin",
            Path(os.environ.get("LOCALAPPDATA", str(Path.home() / "AppData/Local"))) / "Microsoft/WinGet/Links",
            Path.home() / "scoop/apps/ffmpeg/current/bin",
            Path.home() / "ffmpeg/bin",
        ]
        for root in install_roots:
            candidates.extend((root / "ffmpeg.exe", root / "ffmpeg"))
    for candidate in candidates:
        try:
            if candidate.is_file():
                return candidate.resolve()
        except OSError:
            continue
    return None


def sniff_content_type(prefix: bytes) -> str | None:
    data = prefix[:4096]
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if data.startswith(b"BM"):
        return "image/bmp"
    if data.startswith((b"II*\x00", b"MM\x00*")):
        return "image/tiff"
    if data.startswith(b"\x00\x00\x01\x00"):
        return "image/x-icon"
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return "image/webp"
    if len(data) >= 12 and data[4:8] == b"ftyp":
        brand = data[8:12]
        if brand in {b"avif", b"avis"}:
            return "image/avif"
        if brand in {b"heic", b"heix", b"hevc", b"mif1"}:
            return "image/heic"
        return "video/mp4"
    if data.startswith(b"\x1aE\xdf\xa3"):
        return "video/webm"
    if data.startswith(b"ID3") or (len(data) > 1 and data[0] == 0xff and data[1] & 0xe0 == 0xe0):
        return "audio/mpeg"
    if data.startswith(b"fLaC"):
        return "audio/flac"
    if data.startswith(b"OggS"):
        return "audio/ogg"
    if data.startswith(b"RIFF") and data[8:12] == b"WAVE":
        return "audio/wav"
    if data.startswith(b"%PDF-"):
        return "application/pdf"
    if data.startswith(b"PK\x03\x04"):
        return "application/zip"
    stripped = data.lstrip(b"\xef\xbb\xbf\x00\t\r\n ").lower()
    if stripped.startswith((b"<!doctype html", b"<html", b"<head", b"<body")):
        return "text/html"
    if stripped.startswith(b"<svg") or (stripped.startswith(b"<?xml") and b"<svg" in stripped[:1024]):
        return "image/svg+xml"
    return None


def looks_like_html(prefix: bytes) -> bool:
    return sniff_content_type(prefix) == "text/html"


def add_missing_extension(filename: str, content_type: str) -> str:
    if Path(filename).suffix:
        return filename
    extension = mimetypes.guess_extension(content_type) or ""
    if extension in {".jpe", ".htm"}:
        extension = {".jpe": ".jpg", ".htm": ".html"}[extension]
    return f"{filename}{extension}"


def filename_from_response(response, final_url: str, detected_type: str | None = None) -> str:
    content_type = detected_type or response.headers.get_content_type()
    disposition = response.headers.get("Content-Disposition", "")
    message = email.message.Message()
    message["content-disposition"] = disposition
    remote_name = message.get_filename() or ""
    if remote_name:
        try:
            remote_name = remote_name.encode("latin-1").decode("utf-8")
        except (UnicodeError, AttributeError):
            pass
    candidate = safe_filename(remote_name)
    if candidate != "ahd-download":
        return add_missing_extension(candidate, content_type)
    path_name = urllib.parse.unquote(urllib.parse.urlsplit(final_url).path.rsplit("/", 1)[-1])
    candidate = safe_filename(path_name)
    if candidate != "ahd-download":
        return add_missing_extension(candidate, content_type)
    extension = mimetypes.guess_extension(content_type) or ""
    return f"download{extension}"


def response_size(response) -> int | None:
    try:
        value = response.headers.get("Content-Length")
        size = int(value) if value is not None else None
        return size if size is not None and size >= 0 else None
    except (TypeError, ValueError):
        return None


def is_site_extraction_host(url: str) -> bool:
    host = (urllib.parse.urlsplit(url).hostname or "").lower().rstrip(".")
    return any(host == domain or host.endswith("." + domain) for domain in EXTRACTION_HOSTS)


def is_direct_file_url(url: str) -> bool:
    suffix = Path(urllib.parse.urlsplit(url).path).suffix.lower()
    return suffix in DIRECT_FILE_EXTENSIONS


def probe_content_type(url: str) -> str | None:
    """HEAD-probe supported-platform URLs so direct image/file URLs use core downloads first."""
    request = urllib.request.Request(url, headers={"User-Agent": "AHD-Local/0.2", "Accept": "*/*"}, method="HEAD")
    try:
        with OPENER.open(request, timeout=PROBE_TIMEOUT_SECONDS) as response:
            return response.headers.get_content_type()
    except urllib.error.HTTPError as exc:
        exc.close()
        return None
    except (urllib.error.URLError, OSError, http.client.HTTPException, TimeoutError):
        return None


def has_direct_response(url: str) -> bool:
    if is_direct_file_url(url):
        return True
    content_type = probe_content_type(url)
    if content_type and content_type not in {"text/html", "application/xhtml+xml"}:
        return True
    try:
        response = fetch(url, "bytes=0-4095", timeout=PROBE_TIMEOUT_SECONDS)
        with response:
            prefix = response.read(4096)
            detected = sniff_content_type(prefix)
            if detected and detected != "text/html":
                return True
            return response.headers.get_content_type() not in {"text/html", "application/xhtml+xml"} and not looks_like_html(prefix)
    except DownloadError:
        return False


def clean_extractor_error(stderr: str) -> str:
    lowered = stderr.lower()
    if "requested format is not available" in lowered or "requested format not available" in lowered:
        return "That video quality is no longer available. Analyze the link again and choose one of the current quality options."
    if "no supported javascript runtime" in lowered or "javascript runtime" in lowered and "not found" in lowered:
        return "YouTube needs Deno 2.3+ or Node.js 22+ for full format access. Install one, restart AHD, and analyze the link again."
    if "account authentication is required" in lowered or "authentication is required" in lowered:
        if "reddit" in lowered:
            return "Reddit requires sign-in for this post. AHD does not use your login cookies, so it cannot access this item. Try a public post that opens without signing in."
        return "This media requires account sign-in. AHD does not use login cookies, so it cannot access this item. Try a public link that opens without signing in."
    if "sign in to confirm" in lowered or "login required" in lowered or "requires authentication" in lowered:
        return "This media requires account sign-in. AHD does not use login cookies, so it cannot access this item. Try a public link that opens without signing in."
    lines = [line.strip() for line in stderr.splitlines() if line.strip()]
    message = lines[-1] if lines else "The site extractor could not resolve this page."
    message = re.sub(r"https?://\S+", "[link]", message)
    return message[:420]


def estimate_format_size(media_format: dict, duration: float | int | None, separate_audio_kbps: float = 0) -> tuple[int | None, bool]:
    exact = media_format.get("filesize")
    approximate = media_format.get("filesize_approx")
    try:
        if exact is not None and int(exact) > 0:
            return int(exact), False
        if approximate is not None and int(approximate) > 0:
            return int(approximate), True
    except (TypeError, ValueError):
        pass
    try:
        seconds = float(duration or 0)
        video_kbps = float(media_format.get("tbr") or media_format.get("vbr") or 0)
        if media_format.get("acodec") in (None, "none"):
            video_kbps += separate_audio_kbps
        if seconds > 0 and video_kbps > 0:
            return int(seconds * video_kbps * 1000 / 8), True
    except (TypeError, ValueError):
        pass
    return None, False


def yt_dlp_js_runtime_args() -> list[str] | None:
    """Return yt-dlp runtime flags, or None when no current supported runtime exists."""
    deno = shutil.which("deno")
    if deno:
        try:
            result = subprocess.run([deno, "--version"], capture_output=True, text=True, timeout=5, stdin=subprocess.DEVNULL, check=False)
            match = re.search(r"(?:deno\s+)?(\d+)\.(\d+)", result.stdout.strip(), re.I)
            if result.returncode == 0 and match and (int(match.group(1)), int(match.group(2))) >= (2, 3):
                return []  # Deno is enabled by default.
        except (OSError, subprocess.TimeoutExpired):
            pass
    node = shutil.which("node")
    if not node:
        return None
    try:
        result = subprocess.run([node, "--version"], capture_output=True, text=True, timeout=5, stdin=subprocess.DEVNULL, check=False)
        match = re.search(r"(?:v)?(\d+)\.", result.stdout.strip())
        if result.returncode == 0 and match and int(match.group(1)) >= 22:
            return ["--js-runtimes", "node"]
    except (OSError, subprocess.TimeoutExpired):
        pass
    return None


def extract_site_info(url: str) -> dict:
    if importlib.util.find_spec("yt_dlp") is None:
        raise DownloadError("Site extraction needs yt-dlp. Run setup.bat, then install FFmpeg and a JavaScript runtime for full YouTube support.")
    runtime_args = yt_dlp_js_runtime_args()
    command = [
        sys.executable, "-m", "yt_dlp", "--ignore-config", "--no-update", "--no-warnings",
        "--no-playlist", "--retries", "5", "--extractor-retries", "3",
        *(runtime_args or []), "--dump-single-json", url,
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=EXTRACT_TIMEOUT_SECONDS, stdin=subprocess.DEVNULL, check=False)
    except subprocess.TimeoutExpired as exc:
        raise DownloadError("The site took too long to analyze. Try again later.") from exc
    except OSError as exc:
        raise DownloadError("AHD could not start yt-dlp. Run setup.bat again.") from exc
    if result.returncode != 0:
        raise DownloadError(clean_extractor_error(result.stderr))
    try:
        info = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise DownloadError("The extractor returned unreadable video information.") from exc
    available_formats = info.get("formats", [])
    separate_audio_kbps = 0.0
    for media_format in available_formats:
        if media_format.get("vcodec") in (None, "none") and media_format.get("acodec") not in (None, "none"):
            try:
                separate_audio_kbps = max(separate_audio_kbps, float(media_format.get("abr") or media_format.get("tbr") or 0))
            except (TypeError, ValueError):
                continue
    formats_by_height: dict[int, dict] = {}
    for media_format in available_formats:
        format_id = str(media_format.get("format_id") or "")
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,80}", format_id) or media_format.get("has_drm"):
            continue
        if media_format.get("vcodec") in (None, "none"):
            continue
        height = media_format.get("height")
        if not isinstance(height, int) or height < 1:
            height = 0
        extension = safe_filename(str(media_format.get("ext") or "file")).upper()
        has_audio = media_format.get("acodec") not in (None, "none")
        bitrate = float(media_format.get("tbr") or media_format.get("vbr") or 0)
        score = (has_audio, bitrate, extension == "MP4")
        previous = formats_by_height.get(height)
        if previous and previous["score"] >= score:
            continue
        selector = format_id if has_audio else f"{format_id}+bestaudio/best"
        label = f"{extension} · {height}p" if height else f"{extension} · video"
        if not has_audio:
            label += " · join audio"
        size_bytes, size_approximate = estimate_format_size(media_format, info.get("duration"), separate_audio_kbps)
        formats_by_height[height] = {
            "id": format_id,
            "selector": selector,
            "label": label,
            "height": height or None,
            "needs_merge": not has_audio,
            "sizeBytes": size_bytes,
            "sizeApproximate": size_approximate,
            "score": score,
        }
    formats = sorted(formats_by_height.values(), key=lambda item: (item["height"] or 0))
    if not formats:
        raise DownloadError("The page was recognized, but no public, downloadable video formats were found.")
    job_id = secrets.token_urlsafe(24)
    now = time.time()
    extraction = {"url": url, "title": safe_filename(str(info.get("title") or info.get("id") or "video")), "thumbnail": "", "duration": info.get("duration"), "formats": {item["id"]: item for item in formats}, "created": now}
    thumbnail = info.get("thumbnail")
    if thumbnail:
        try:
            extraction["thumbnail"] = validate_public_url(str(thumbnail))
        except DownloadError:
            pass
    with EXTRACTIONS_LOCK:
        for key in [key for key, value in EXTRACTIONS.items() if now - value["created"] > 30 * 60]:
            EXTRACTIONS.pop(key, None)
        EXTRACTIONS[job_id] = extraction
    public_formats = []
    for item in formats:
        public_formats.append({
            key: value for key, value in item.items()
            if key not in {"selector", "score", "needs_merge"}
        } | {"needsMerge": item["needs_merge"]})
    return {
        "mode": "site-video", "job": job_id, "title": extraction["title"],
        "thumbnail": extraction["thumbnail"], "duration": extraction["duration"],
        "formats": public_formats,
        "ffmpegAvailable": bool(find_ffmpeg()),
        "jsRuntimeAvailable": runtime_args is not None,
    }


def run_site_download(job_id: str, format_id: str):
    with EXTRACTIONS_LOCK:
        extraction = EXTRACTIONS.get(job_id)
        if not extraction or time.time() - extraction["created"] > 30 * 60:
            EXTRACTIONS.pop(job_id, None)
            raise DownloadError("This video result expired. Analyze the link again.")
        choice = extraction["formats"].get(format_id)
    if not choice:
        raise DownloadError("That quality is no longer available. Analyze the link again.")
    ffmpeg = find_ffmpeg()
    if choice["needs_merge"] and not ffmpeg:
        raise DownloadError("This quality has separate video and audio streams. Install FFmpeg to combine them, or choose a format with audio included.")
    temp_dir = tempfile.mkdtemp(prefix="ahd-video-")
    try:
        output_template = str(Path(temp_dir) / "%(title).180B.%(ext)s")
        command = [
            sys.executable, "-m", "yt_dlp", "--ignore-config", "--no-update", "--no-warnings",
            "--no-playlist", "--no-progress", "--no-write-info-json", "--no-write-description",
            "--no-write-thumbnail", "--no-write-subs", "--no-write-auto-subs", "--windows-filenames",
            "--retries", "5", "--fragment-retries", "5", "--extractor-retries", "3", "--concurrent-fragments", "4",
            *( ["--max-filesize", "400M"] if PUBLIC_MODE else [] ),
            *(["--ffmpeg-location", str(ffmpeg)] if ffmpeg else []),
            *(yt_dlp_js_runtime_args() or []),
            "--format", choice["selector"], "--output", output_template, extraction["url"],
        ]
        try:
            result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=DOWNLOAD_TIMEOUT_SECONDS, stdin=subprocess.DEVNULL, check=False)
        except subprocess.TimeoutExpired as exc:
            raise DownloadError("This download exceeded the one-hour limit.") from exc
        except OSError as exc:
            raise DownloadError("AHD could not start the site extractor.") from exc
        if result.returncode != 0:
            raise DownloadError(clean_extractor_error(result.stderr))
        files = [path for path in Path(temp_dir).iterdir() if path.is_file() and not path.name.endswith((".part", ".ytdl", ".temp"))]
        if not files:
            raise DownloadError("The extractor finished without producing a video file.")
        output = max(files, key=lambda path: path.stat().st_size)
        if PUBLIC_MODE and output.stat().st_size > PUBLIC_MAX_BYTES:
            raise DownloadError("This free demo allows files up to 400 MB.")
        return temp_dir, output
    except Exception:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise


class MediaParser(html.parser.HTMLParser):
    def __init__(self, page_url: str):
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.items: dict[str, dict] = {}
        self.title = ""
        self.thumbnail = ""
        self.duration = ""
        self._in_title = False

    def add(self, raw_url: str | None, kind: str, label: str = ""):
        if not raw_url:
            return
        try:
            url = validate_public_url(urllib.parse.urljoin(self.page_url, raw_url))
        except (DownloadError, ValueError):
            return
        if url not in self.items:
            name = urllib.parse.unquote(urllib.parse.urlsplit(url).path.rsplit("/", 1)[-1]) or kind
            self.items[url] = {"url": url, "name": safe_filename(name), "type": kind, "label": label}

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        tag = tag.lower()
        if tag == "title":
            self._in_title = True
        if tag == "meta":
            key = (attrs.get("property") or attrs.get("name") or "").lower()
            content = attrs.get("content", "").strip()
            if key in {"og:title", "twitter:title"} and content:
                self.title = content[:300]
            elif key in {"og:image", "og:image:url", "twitter:image"} and content:
                try:
                    self.thumbnail = validate_public_url(urllib.parse.urljoin(self.page_url, content))
                except DownloadError:
                    pass
            elif key in {"video:duration", "og:video:duration", "duration"} and content:
                self.duration = content[:20]
        if tag == "base" and attrs.get("href"):
            try:
                self.page_url = validate_public_url(urllib.parse.urljoin(self.page_url, attrs["href"]))
            except DownloadError:
                pass
        kind = {"img": "image", "video": "video", "audio": "audio", "source": "media source"}.get(tag)
        if kind:
            self.add(attrs.get("src"), kind)
            if attrs.get("poster"):
                self.add(attrs["poster"], "video poster")
            for candidate in (attrs.get("srcset") or "").split(","):
                parts = candidate.strip().split()
                if parts:
                    self.add(parts[0], kind, parts[1] if len(parts) > 1 else "")
        if tag == "a" and attrs.get("href"):
            href = attrs["href"]
            path = urllib.parse.urlsplit(urllib.parse.urljoin(self.page_url, href)).path.lower()
            link_type = (attrs.get("type") or "").lower()
            if attrs.get("download") is not None or Path(path).suffix in FILE_EXTENSIONS or link_type.startswith(("application/", "audio/", "image/", "video/")):
                self.add(href, "linked file", attrs.get("download", ""))

    def handle_endtag(self, tag):
        if tag.lower() == "title":
            self._in_title = False

    def handle_data(self, data):
        if self._in_title and data.strip() and not self.title:
            self.title = data.strip()[:300]


class AHDRequestHandler(http.server.BaseHTTPRequestHandler):
    server_version = "AHDLocal/0.2"

    def log_message(self, format, *args):
        # Do not write pasted URLs or tokens to console logs.
        return

    def _host_is_allowed(self) -> bool:
        host = self.headers.get("Host", "").lower()
        if not PUBLIC_MODE:
            return host in ALLOWED_HOST_HEADERS
        allowed = {value.strip().lower() for value in os.getenv("AHD_PUBLIC_HOSTS", "").split(",") if value.strip()}
        render_host = os.getenv("RENDER_EXTERNAL_HOSTNAME", "").lower()
        if render_host:
            allowed.add(render_host)
        return bool(allowed) and host in allowed

    def _origin_allowed(self, origin: str | None) -> bool:
        if origin == ALLOWED_EXTENSION_ORIGIN and not PUBLIC_MODE:
            return True
        if PUBLIC_MODE:
            return bool(origin) and origin == f"https://{self.headers.get('Host', '').lower()}" and self._host_is_allowed()
        return origin == f"http://{HOST}:{PORT}"

    def _public_rate_allowed(self) -> bool:
        if not PUBLIC_MODE:
            return True
        forwarded = self.headers.get("X-Forwarded-For", "").split(",", 1)[0].strip()
        try:
            key = str(ipaddress.ip_address(forwarded))
        except ValueError:
            key = self.client_address[0]
        now = time.time()
        with PUBLIC_RATE_LOCK:
            recent = [stamp for stamp in PUBLIC_REQUESTS.get(key, []) if now - stamp < 600]
            if len(recent) >= 20:
                PUBLIC_REQUESTS[key] = recent
                return False
            recent.append(now)
            PUBLIC_REQUESTS[key] = recent
        return True

    def _send_json(self, status: int, payload: dict):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        if self.headers.get("Origin") == ALLOWED_EXTENSION_ORIGIN:
            self.send_header("Access-Control-Allow-Origin", ALLOWED_EXTENSION_ORIGIN)
            self.send_header("Vary", "Origin")
        self.end_headers()
        self.wfile.write(data)

    def do_OPTIONS(self):
        if not self._host_is_allowed() or self.headers.get("Origin") != ALLOWED_EXTENSION_ORIGIN:
            self.send_error(403)
            return
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", ALLOWED_EXTENSION_ORIGIN)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-AHD-Token")
        self.send_header("Access-Control-Max-Age", "300")
        self.send_header("Access-Control-Allow-Private-Network", "true")
        self.send_header("Vary", "Origin")
        self.end_headers()

    def _send_download_error(self, status: int, message: str):
        safe_message = __import__("html").escape(message)
        body = ("<!doctype html><meta charset='utf-8'><title>AHD download issue</title>"
                "<meta name='viewport' content='width=device-width'><style>body{margin:3rem auto;max-width:38rem;padding:0 1rem;background:#0b1117;color:#edf4f7;font:16px/1.6 system-ui}h1{color:#ff9f9f}p{color:#aab9c0}</style>"
                f"<h1>AHD could not start this download</h1><p>{safe_message}</p>"
                "<p>Return to AHD and try another direct file link. Some sites require a sign-in or block direct downloads.</p>")
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
        self.wfile.write(data)

    def _authorized(self, token: str | None) -> bool:
        return bool(token) and secrets.compare_digest(token, TOKEN)

    def do_GET(self):
        if not self._host_is_allowed():
            self.send_error(403)
            return
        parsed = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        if parsed.path == "/health":
            self._send_json(200, {"ok": True})
        elif parsed.path == "/api/extension-token":
            if PUBLIC_MODE or self.headers.get("Origin") != ALLOWED_EXTENSION_ORIGIN:
                self.send_error(403)
            else:
                self._send_json(200, {"token": TOKEN})
        elif parsed.path == "/api/jobs":
            if PUBLIC_MODE:
                self.send_error(404)
            elif not self._authorized(query.get("token", [None])[0]):
                self.send_error(403)
            elif DOWNLOAD_ENGINE is None:
                self._send_json(503, {"error": "Download engine is unavailable."})
            else:
                self._send_json(200, {"jobs": DOWNLOAD_ENGINE.list_jobs()})
        elif parsed.path == "/":
            self._serve_index()
        elif parsed.path == "/download":
            token = query.get("token", [None])[0]
            target = query.get("url", [None])[0]
            if not self._authorized(token):
                self.send_error(403)
            elif not target:
                self._send_json(400, {"error": "Missing download URL."})
            else:
                if PUBLIC_MODE and not PUBLIC_DOWNLOAD_SLOT.acquire(blocking=False):
                    self._send_download_error(429, "The free demo is busy. Wait a moment and try again.")
                else:
                    try:
                        self._stream_download(target)
                    finally:
                        if PUBLIC_MODE:
                            PUBLIC_DOWNLOAD_SLOT.release()
        elif parsed.path == "/site-download":
            token = query.get("token", [None])[0]
            job_id = query.get("job", [None])[0]
            format_id = query.get("format", [None])[0]
            if not self._authorized(token):
                self.send_error(403)
            elif not job_id or not format_id:
                self._send_download_error(400, "Missing video job or quality selection.")
            else:
                if PUBLIC_MODE and not PUBLIC_DOWNLOAD_SLOT.acquire(blocking=False):
                    self._send_download_error(429, "The free demo is busy. Wait a moment and try again.")
                else:
                    try:
                        self._stream_site_download(job_id, format_id)
                    finally:
                        if PUBLIC_MODE:
                            PUBLIC_DOWNLOAD_SLOT.release()
        else:
            self.send_error(404)

    def do_POST(self):
        origin = self.headers.get("Origin")
        if not self._host_is_allowed() or not self._origin_allowed(origin):
            self.send_error(403)
            return
        if not self._authorized(self.headers.get("X-AHD-Token")):
            self.send_error(403)
            return
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path not in {"/api/inspect", "/api/resolve", "/api/jobs"} and not re.fullmatch(r"/api/jobs/[a-f0-9]{32}/(pause|resume|cancel)", parsed.path):
            self.send_error(404)
            return
        try:
            if PUBLIC_MODE and parsed.path.startswith("/api/jobs"):
                self.send_error(404)
                return
            if not self._public_rate_allowed():
                self._send_json(429, {"error": "Demo rate limit reached. Wait ten minutes and try again."})
                return
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > MAX_REQUEST_BYTES:
                raise DownloadError("The request is empty or too large.")
            payload = json.loads(self.rfile.read(length))
            if parsed.path == "/api/jobs":
                if DOWNLOAD_ENGINE is None:
                    self._send_json(503, {"error": "Download engine is unavailable."})
                    return
                job = DOWNLOAD_ENGINE.enqueue(str(payload.get("url", "")), payload.get("speedLimitKbps", 0))
                self._send_json(200, {"job": job})
                return
            action_match = re.fullmatch(r"/api/jobs/([a-f0-9]{32})/(pause|resume|cancel)", parsed.path)
            if action_match:
                if DOWNLOAD_ENGINE is None:
                    self._send_json(503, {"error": "Download engine is unavailable."})
                    return
                job = DOWNLOAD_ENGINE.action(action_match.group(1), action_match.group(2))
                self._send_json(200, {"job": job})
                return
            target = validate_public_url(str(payload.get("url", "")))
            try_site_extraction = self.path == "/api/resolve" and is_site_extraction_host(target) and not has_direct_response(target)
            if try_site_extraction:
                try:
                    self._send_json(200, extract_site_info(target))
                except DownloadError as exc:
                    self._send_json(422, {"error": str(exc)})
            else:
                self._inspect(target)
        except (ValueError, json.JSONDecodeError, DownloadError, JobError) as exc:
            self._send_json(400, {"error": str(exc)})

    def _serve_index(self):
        source = (ROOT / "index.html").read_text(encoding="utf-8").replace("__AHD_TOKEN__", TOKEN).replace("__AHD_PUBLIC_MODE__", "true" if PUBLIC_MODE else "false")
        data = source.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
        self.wfile.write(data)

    def _inspect(self, target: str):
        response = fetch(target)
        with response:
            final_url = validate_public_url(response.geturl())
            header_type = response.headers.get_content_type()
            prefix = response.read(4096)
            detected_type = sniff_content_type(prefix)
            suffix = Path(urllib.parse.urlsplit(final_url).path).suffix.lower()
            is_attachment = "attachment" in response.headers.get("Content-Disposition", "").lower()
            try:
                declared_size = int(response.headers.get("Content-Length", "-1"))
            except ValueError:
                declared_size = -1
            if PUBLIC_MODE and declared_size > PUBLIC_MAX_BYTES:
                self._send_download_error(413, "This free demo allows files up to 400 MB.")
                return
            is_html = detected_type == "text/html" or (detected_type is None and header_type in {"text/html", "application/xhtml+xml"})
            direct_html_document = suffix in {".html", ".htm", ".xhtml"}
            if is_attachment or (is_html and direct_html_document) or (not is_html and ((detected_type is not None) or not looks_like_html(prefix))):
                content_type = detected_type or header_type
                filename = filename_from_response(response, final_url, content_type)
                self._send_json(200, {"title": filename, "thumbnail": "", "duration": "", "items": [{"url": final_url, "name": filename, "type": content_type, "label": "Direct file", "sizeBytes": response_size(response)}]})
                return
            data = prefix + response.read(MAX_INSPECT_BYTES - len(prefix) + 1)
        if len(data) > MAX_INSPECT_BYTES:
            raise DownloadError("This webpage is too large to inspect safely (limit 5 MB). Paste a direct file link instead.")
        parser = MediaParser(final_url)
        try:
            parser.feed(data.decode("utf-8", errors="replace"))
        except Exception as exc:
            raise DownloadError("Could not inspect this page's links.") from exc
        self._send_json(200, {"title": parser.title or final_url, "thumbnail": parser.thumbnail, "duration": parser.duration, "items": list(parser.items.values())[:250]})

    def _stream_download(self, target: str):
        try:
            response = fetch(target, self.headers.get("Range"))
        except DownloadError as exc:
            self._send_download_error(502, str(exc))
            return
        with response:
            final_url = validate_public_url(response.geturl())
            header_type = response.headers.get_content_type()
            range_requested = bool(self.headers.get("Range"))
            prefix = b"" if range_requested else response.read(4096)
            detected_type = sniff_content_type(prefix) if prefix else None
            content_type = detected_type or header_type
            path_suffix = Path(urllib.parse.urlsplit(final_url).path).suffix.lower()
            is_attachment = "attachment" in response.headers.get("Content-Disposition", "").lower()
            if (content_type in {"text/html", "application/xhtml+xml"} or looks_like_html(prefix)) and not is_attachment and path_suffix not in {".html", ".htm", ".xhtml"}:
                self._send_download_error(415, "This link returned a webpage, not a direct media file. Use “Find files on this page”. Sites such as video platforms need their own maintained extractor; this local prototype does not extract their streams yet.")
                return
            filename = filename_from_response(response, final_url, content_type)
            encoded_name = urllib.parse.quote(filename, safe="")
            self.send_response(response.status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Disposition", f"attachment; filename=\"download\"; filename*=UTF-8''{encoded_name}")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            for header in ("Content-Length", "Content-Range", "Accept-Ranges"):
                value = response.headers.get(header)
                if value:
                    self.send_header(header, value)
            self.end_headers()
            try:
                sent = 0
                if prefix:
                    sent += len(prefix)
                    self.wfile.write(prefix)
                while True:
                    chunk = response.read(128 * 1024)
                    if not chunk:
                        break
                    sent += len(chunk)
                    if PUBLIC_MODE and sent > PUBLIC_MAX_BYTES:
                        self.close_connection = True
                        return
                    self.wfile.write(chunk)
            except (BrokenPipeError, ConnectionResetError):
                return

    def _stream_site_download(self, job_id: str, format_id: str):
        try:
            temp_dir, output = run_site_download(job_id, format_id)
        except DownloadError as exc:
            self._send_download_error(502, str(exc))
            return
        try:
            size = output.stat().st_size
            start, end, status = 0, size - 1, 200
            range_header = self.headers.get("Range", "")
            if range_header:
                match = re.fullmatch(r"bytes=(\d+)-(\d*)", range_header.strip())
                if not match:
                    self.send_response(416)
                    self.send_header("Content-Range", f"bytes */{size}")
                    self.end_headers()
                    return
                start = int(match.group(1))
                end = min(int(match.group(2)) if match.group(2) else size - 1, size - 1)
                if start > end or start >= size:
                    self.send_response(416)
                    self.send_header("Content-Range", f"bytes */{size}")
                    self.end_headers()
                    return
                status = 206
            filename = safe_filename(output.name)
            encoded_name = urllib.parse.quote(filename, safe="")
            self.send_response(status)
            self.send_header("Content-Type", mimetypes.guess_type(filename)[0] or "application/octet-stream")
            self.send_header("Content-Disposition", f"attachment; filename=\"download\"; filename*=UTF-8''{encoded_name}")
            self.send_header("Content-Length", str(end - start + 1))
            self.send_header("Accept-Ranges", "bytes")
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.end_headers()
            try:
                with output.open("rb") as source:
                    source.seek(start)
                    remaining = end - start + 1
                    while remaining:
                        chunk = source.read(min(128 * 1024, remaining))
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                        remaining -= len(chunk)
            except (BrokenPipeError, ConnectionResetError):
                return
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


class LocalServer(http.server.ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    global DOWNLOAD_ENGINE
    local_data = Path(os.environ.get("LOCALAPPDATA", str(Path.home() / ".local" / "share"))) / "AHD"
    if PUBLIC_MODE:
        DOWNLOAD_ENGINE = None
    else:
        try:
            DOWNLOAD_ENGINE = DownloadEngine(
                local_data,
                Path.home() / "Downloads",
                validate_public_url,
                fetch,
                filename_from_response,
                sniff_content_type,
                safe_filename,
            )
        except OSError as exc:
            print(f"AHD could not initialize its download history or queue: {exc}")
            input("Press Enter to close…")
            return 1
    bind_host = "0.0.0.0" if PUBLIC_MODE else HOST
    bind_port = int(os.environ.get("PORT", str(PORT))) if PUBLIC_MODE else PORT
    try:
        server = LocalServer((bind_host, bind_port), AHDRequestHandler)
    except OSError as exc:
        print(f"AHD could not start on {HOST}:{PORT}: {exc}")
        input("Press Enter to close…")
        return 1
    url = f"http://{HOST}:{PORT}/" if not PUBLIC_MODE else f"port {bind_port}"
    if not PUBLIC_MODE:
        print(f"AHD Browser Downloader is running at {url}")
        print(f"Downloads are saved to {Path.home() / 'Downloads'}; queue and history data are stored locally in {local_data}.")
        print("Keep this window open while using AHD. Press Ctrl+C to stop.")
        threading.Timer(0.8, lambda: __import__("webbrowser").open(url)).start()
    else:
        print(f"AHD public demo listening on port {bind_port}")
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        print("\nAHD stopped.")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
