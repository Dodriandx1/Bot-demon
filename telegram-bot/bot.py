import os
import sys
import time
import asyncio
import subprocess
import psutil
import httpx
import gc
import yt_dlp
import glob
import http.server
import socketserver
import threading
import re
import platform
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from pyrogram import Client, filters, enums
from pyrogram.types import Message, CallbackQuery
from pyrogram.errors import MessageNotModified

# ─── CONFIG ─────────────────────────────────────────────────────────────────
API_ID   = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
MEGA_EMAIL    = os.environ.get("MEGA_EMAIL", "")
MEGA_PASSWORD = os.environ.get("MEGA_PASSWORD", "")

DOWNLOAD_DIR = "/tmp/downloads/"
os.makedirs(DOWNLOAD_DIR, mode=0o777, exist_ok=True)

start_time = time.time()

active_tasks: dict = {}
last_updates: dict = {}
download_queue: asyncio.Queue = asyncio.Queue()

BOT_SIGNATURE = "✪ Bot By → @The_canst & @Ryota_YT"

# ─── KEEP-ALIVE ──────────────────────────────────────────────────────────────
def keep_alive():
    class Handler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Bot is Running")
        def log_message(self, *args):
            pass

    port = int(os.environ.get("PORT", 8000))
    with socketserver.TCPServer(("", port), Handler) as httpd:
        httpd.serve_forever()

threading.Thread(target=keep_alive, daemon=True).start()

# ─── UTILITIES ───────────────────────────────────────────────────────────────
def get_readable_size(size):
    if size is None or size == 0:
        return "0B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return "0B"

def get_readable_time(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        m, s = divmod(seconds, 60)
        return f"{m}m {s}s"
    else:
        h, rem = divmod(seconds, 3600)
        m, s = divmod(rem, 60)
        return f"{h}h {m}m {s}s"

def make_bar(percentage: float, width: int = 13) -> str:
    """Build the ⬢◉◌ style progress bar."""
    filled = int(percentage / 100 * width)
    if filled >= width:
        return "⬢" * width
    bar = "⬢" * filled + "◉" + "◌" * (width - filled - 1)
    return bar

def extract_thumbnail(video_path: str):
    thumb = video_path + ".jpg"
    try:
        subprocess.run(
            [
                "ffmpeg", "-i", video_path,
                "-ss", "00:00:01",
                "-vframes", "1",
                "-vf", "scale=320:-1",
                "-q:v", "2",
                thumb, "-y"
            ],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=15, check=True
        )
        return thumb if os.path.exists(thumb) else None
    except Exception:
        return None

async def safe_edit(msg: Message, text: str):
    try:
        await msg.edit_text(text, parse_mode=None)
    except MessageNotModified:
        pass
    except Exception:
        pass

# ─── PROGRESS PANELS ─────────────────────────────────────────────────────────
def download_panel(uname: str, percentage: float, status: str,
                   done_bytes: int, total_bytes: int,
                   speed_bps: float, elapsed: float, eta: float,
                   engine: str, mode: str, task_id: str) -> str:
    bar = make_bar(percentage)
    speed_str = get_readable_size(speed_bps) + "/s"
    done_str  = get_readable_size(done_bytes)
    total_str = get_readable_size(total_bytes)
    return (
        f"╭ Task By → 「{uname}」\n"
        f"┊ [{bar}] {percentage:.2f}%\n"
        f"┊ Status   : {status}\n"
        f"┊ Done     : {done_str}\n"
        f"┊ Total    : {total_str}\n"
        f"┊ Speed    : {speed_str}\n"
        f"┊ ETA      : {get_readable_time(eta)}\n"
        f"┊ Past     : {get_readable_time(elapsed)}\n"
        f"┊ Engine   : {engine}\n"
        f"╰ Mode     : {mode}\n"
        f"⋗ Stop : /cancel_{task_id}\n\n"
        f"{BOT_SIGNATURE}"
    )

def encoding_panel(uname: str, percentage: float,
                   done_bytes: int, total_bytes: int,
                   fps: float, elapsed: float, eta: float,
                   task_id: str) -> str:
    bar = make_bar(percentage)
    return (
        f"╭ Task By → 「{uname}」\n"
        f"┊ [{bar}] {percentage:.2f}%\n"
        f"┊ Status   : Encoding\n"
        f"┊ Done     : {get_readable_size(done_bytes)}\n"
        f"┊ Total    : {get_readable_size(total_bytes)}\n"
        f"┊ Speed    : {fps:.2f} fps\n"
        f"┊ ETA      : {get_readable_time(eta)}\n"
        f"┊ Past     : {get_readable_time(elapsed)}\n"
        f"┊ Engine   : HandBrake\n"
        f"╰ Mode     : #HB\n"
        f"⋗ Stop : /cancel_{task_id}\n\n"
        f"{BOT_SIGNATURE}"
    )

def upload_panel(uname: str, percentage: float,
                 done_bytes: int, total_bytes: int,
                 speed_bps: float, elapsed: float, eta: float,
                 task_id: str) -> str:
    bar = make_bar(percentage)
    return (
        f"╭ Task By → 「{uname}」\n"
        f"┊ [{bar}] {percentage:.2f}%\n"
        f"┊ Status   : Upload\n"
        f"┊ Done     : {get_readable_size(done_bytes)}\n"
        f"┊ Total    : {get_readable_size(total_bytes)}\n"
        f"┊ Speed    : {get_readable_size(speed_bps)}/s\n"
        f"┊ ETA      : {get_readable_time(eta)}\n"
        f"┊ Past     : {get_readable_time(elapsed)}\n"
        f"┊ Engine   : Pyrogram\n"
        f"╰ Mode     : #TLGUP\n"
        f"⋗ Stop : /cancel_{task_id}\n\n"
        f"{BOT_SIGNATURE}"
    )

# ─── PROGRESS CALLBACKS ──────────────────────────────────────────────────────
async def download_progress(current: int, total: int, msg: Message,
                             start_t: float, uname: str, task_id: str,
                             engine: str, mode: str):
    if active_tasks.get(task_id) == "CANCELLED":
        raise Exception("USER_CANCELLED")

    now = time.time()
    if now - last_updates.get(task_id + "_dl", 0) < 3 and current < total:
        return
    last_updates[task_id + "_dl"] = now

    elapsed = now - start_t
    pct = (current / total * 100) if total > 0 else 0
    speed = current / elapsed if elapsed > 0 else 0
    eta = (total - current) / speed if speed > 0 else 0

    panel = download_panel(uname, pct, "Download", current, total,
                           speed, elapsed, eta, engine, mode, task_id)
    await safe_edit(msg, panel)


async def upload_progress(current: int, total: int, msg: Message,
                           start_t: float, uname: str, task_id: str):
    if active_tasks.get(task_id) == "CANCELLED":
        raise Exception("USER_CANCELLED")

    now = time.time()
    if now - last_updates.get(task_id + "_up", 0) < 3 and current < total:
        return
    last_updates[task_id + "_up"] = now

    elapsed = now - start_t
    pct = (current / total * 100) if total > 0 else 0
    speed = current / elapsed if elapsed > 0 else 0
    eta = (total - current) / speed if speed > 0 else 0

    panel = upload_panel(uname, pct, current, total, speed, elapsed, eta, task_id)
    await safe_edit(msg, panel)

# ─── SMART UPLOAD ────────────────────────────────────────────────────────────
async def upload_smart_file(client: Client, message: Message, path: str,
                             msg: Message, uname: str, task_id: str,
                             title: str = ""):
    fname = os.path.basename(path)
    display = title.strip() if title.strip() else fname
    caption = f"🎬 <b>{display}</b>\n\n{BOT_SIGNATURE}"
    lower = fname.lower()
    start_t = time.time()

    if lower.endswith((".mp4", ".mkv", ".webm", ".avi", ".mov")):
        thumb = extract_thumbnail(path)
        try:
            await client.send_video(
                chat_id=message.chat.id,
                video=path,
                thumb=thumb,
                supports_streaming=True,
                caption=caption,
                parse_mode=enums.ParseMode.HTML,
                progress=upload_progress,
                progress_args=(msg, start_t, uname, task_id)
            )
        except Exception:
            await client.send_document(
                chat_id=message.chat.id,
                document=path,
                caption=caption,
                parse_mode=enums.ParseMode.HTML,
                progress=upload_progress,
                progress_args=(msg, start_t, uname, task_id)
            )
        finally:
            if thumb and os.path.exists(thumb):
                try: os.remove(thumb)
                except: pass

    elif lower.endswith((".jpg", ".jpeg", ".png", ".webp")):
        await client.send_photo(
            chat_id=message.chat.id,
            photo=path,
            caption=caption,
            parse_mode=enums.ParseMode.HTML,
            progress=upload_progress,
            progress_args=(msg, start_t, uname, task_id)
        )

    elif lower.endswith((".mp3", ".m4a", ".wav", ".flac", ".ogg")):
        await client.send_audio(
            chat_id=message.chat.id,
            audio=path,
            caption=caption,
            parse_mode=enums.ParseMode.HTML,
            progress=upload_progress,
            progress_args=(msg, start_t, uname, task_id)
        )

    else:
        await client.send_document(
            chat_id=message.chat.id,
            document=path,
            caption=caption,
            parse_mode=enums.ParseMode.HTML,
            progress=upload_progress,
            progress_args=(msg, start_t, uname, task_id)
        )

# ─── PROBE VIDEO INFO ────────────────────────────────────────────────────────
def probe_video(input_path: str) -> dict:
    """Returns dict with 'codec', 'duration', 'audio_codec'."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_name",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                input_path,
            ],
            capture_output=True, text=True
        )
        lines = [l.strip() for l in result.stdout.strip().splitlines() if l.strip()]
        codec = lines[0] if lines else "unknown"
        duration = float(lines[1]) if len(lines) > 1 else 0.0

        audio = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-select_streams", "a:0",
                "-show_entries", "stream=codec_name",
                "-of", "default=noprint_wrappers=1:nokey=1",
                input_path,
            ],
            capture_output=True, text=True
        )
        audio_codec = audio.stdout.strip().splitlines()[0].strip() if audio.stdout.strip() else "unknown"
        return {"codec": codec, "duration": duration, "audio_codec": audio_codec}
    except Exception:
        return {"codec": "unknown", "duration": 0.0, "audio_codec": "unknown"}

# ─── ENCODE HELPER ───────────────────────────────────────────────────────────
async def encode_video(input_path: str, output_path: str,
                        msg: Message, uname: str, task_id: str) -> bool:
    """
    Smart encode:
      • H.264 + AAC/MP3  → remux only (stream copy, near-instant)
      • H.264 + other audio → copy video, re-encode audio only (fast)
      • Other codec      → full re-encode with ultrafast preset (fastest possible)
    """
    info = await asyncio.to_thread(probe_video, input_path)
    codec       = info["codec"]
    total_dur   = info["duration"]
    audio_codec = info["audio_codec"]
    input_size  = os.path.getsize(input_path)

    # Decide strategy
    is_h264 = codec == "h264"
    is_aac_compat = audio_codec in ("aac", "mp3", "mp4a")

    if is_h264 and is_aac_compat:
        # ── Remux only: copy everything, no re-encode ──────────────────────
        cmd = ["ffmpeg", "-i", input_path,
               "-c:v", "copy", "-c:a", "copy",
               "-movflags", "+faststart",
               "-y", output_path]
        engine_label = "Remux (stream copy)"
    elif is_h264:
        # ── Copy video, re-encode audio only ──────────────────────────────
        cmd = ["ffmpeg", "-i", input_path,
               "-c:v", "copy",
               "-c:a", "aac", "-b:a", "128k",
               "-movflags", "+faststart",
               "-y", output_path]
        engine_label = "Copy+AAC"
    else:
        # ── Full re-encode with ultrafast preset ──────────────────────────
        cmd = ["ffmpeg", "-i", input_path,
               "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
               "-c:a", "aac", "-b:a", "128k",
               "-movflags", "+faststart",
               "-progress", "pipe:1", "-nostats",
               "-y", output_path]
        engine_label = "libx264 ultrafast"

    print(f"[encode] codec={codec} audio={audio_codec} → {engine_label}")

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL
    )

    start_t = time.time()
    fps_val = 0.0
    time_done = 0.0

    while True:
        if active_tasks.get(task_id) == "CANCELLED":
            proc.kill()
            return False
        line_bytes = await proc.stdout.readline()
        if not line_bytes:
            break
        line = line_bytes.decode().strip()
        if line.startswith("fps="):
            try: fps_val = float(line.split("=")[1])
            except: pass
        if line.startswith("out_time_ms="):
            try: time_done = int(line.split("=")[1]) / 1_000_000
            except: pass

        now = time.time()
        if now - last_updates.get(task_id + "_enc", 0) >= 2:
            last_updates[task_id + "_enc"] = now
            elapsed = now - start_t
            pct = min((time_done / total_dur * 100) if total_dur > 0 else 0, 99)
            eta = ((total_dur - time_done) / fps_val * 25) if fps_val > 0 else 0
            panel = encoding_panel(uname, pct,
                                   int(input_size * pct / 100), input_size,
                                   fps_val, elapsed, eta, task_id)
            await safe_edit(msg, panel)

    await proc.wait()
    return proc.returncode == 0 and os.path.exists(output_path)

# ─── CORE DOWNLOAD ───────────────────────────────────────────────────────────
async def procesar_descarga(client: Client, message: Message,
                             url: str, uname: str, uid: int, queue_label: str):
    mirrors = {
        "flashwish.com": "streamwish.com",
        "callistanise.com": "vidhide.com",
        "swishdesu.com": "streamwish.com",
        "filelions.com": "streamwish.com",
        "filelions.to": "streamwish.com",
        "vidhidepro.com": "vidhide.com",
        "vidhideplus.com": "vidhide.com",
    }
    for mirror, main in mirrors.items():
        if mirror in url.lower():
            url = url.replace(mirror, main)
            break

    VIDEO_HOSTS = [
        "streamwish", "voe", "vidhide", "filemoon", "mixdrop",
        "mp4upload", "streamtape", "flashwish", "callistanise",
        "filelions", "swishdesu",
    ]

    is_mega      = "mega.nz" in url
    is_mf        = "mediafire.com" in url
    is_video_host = any(h in url.lower() for h in VIDEO_HOSTS)
    is_social    = url.startswith("http") and not is_mega and not is_mf and not is_video_host

    task_id = f"{uid}_{int(time.time())}"
    active_tasks[task_id] = "RUNNING"

    msg = await message.reply_text(
        f"╭ Task By → 「{uname}」\n"
        f"┊ [{make_bar(0)}] 0.00%\n"
        f"┊ Status   : Analyzing...\n"
        f"╰ Queue    : {queue_label}\n\n"
        f"{BOT_SIGNATURE}"
    )

    path = None
    encoded_path = None
    video_title = ""

    try:
        # ── MEGA ──────────────────────────────────────────────────────────
        if is_mega:
            engine = "yt-dlp"
            mode = "#MEGA"
            start_t = time.time()
            loop = asyncio.get_running_loop()

            await safe_edit(msg,
                f"╭ Task By → 「{uname}」\n"
                f"┊ [{make_bar(0)}] 0.00%\n"
                f"┊ Status   : Connecting to MEGA...\n"
                f"╰ Mode     : {mode}\n\n"
                f"{BOT_SIGNATURE}"
            )

            def ydl_hook_mega(d):
                if d["status"] == "downloading":
                    curr  = d.get("downloaded_bytes", 0)
                    total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
                    if total > 0:
                        asyncio.run_coroutine_threadsafe(
                            download_progress(curr, total, msg, start_t,
                                              uname, task_id, engine, mode),
                            loop
                        )

            captured_mega = {"title": ""}

            def run_mega_ydl():
                opts = {
                    "outtmpl": f"{DOWNLOAD_DIR}{task_id}_%(title)s.%(ext)s",
                    "progress_hooks": [ydl_hook_mega],
                    "quiet": True,
                    "no_warnings": True,
                }
                if MEGA_EMAIL and MEGA_PASSWORD:
                    opts["username"] = MEGA_EMAIL
                    opts["password"] = MEGA_PASSWORD
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    if info:
                        captured_mega["title"] = info.get("title", "") or info.get("webpage_url_basename", "")

            await asyncio.to_thread(run_mega_ydl)

            files = glob.glob(f"{DOWNLOAD_DIR}{task_id}_*")
            if not files:
                raise Exception("MEGA: no se pudo descargar. Verifica que el enlace sea público o las credenciales sean correctas.")
            path = max(files, key=os.path.getctime)
            video_title = captured_mega["title"] or os.path.splitext(os.path.basename(path))[0]

        # ── MEDIAFIRE ─────────────────────────────────────────────────────
        elif is_mf:
            engine = "httpx"
            mode = "#MediaFire"
            async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as h:
                r = await h.get(url)
                soup = BeautifulSoup(r.text, "html.parser")
                btn = soup.find("a", {"id": "downloadButton"})
                if not btn:
                    raise Exception("Mediafire: botón de descarga no encontrado.")
                d_link = btn.get("href")
                filename = d_link.split("/")[-1].split("?")[0]
                path = os.path.join(DOWNLOAD_DIR, filename)
                video_title = os.path.splitext(filename)[0]

                start_t = time.time()
                async with h.stream("GET", d_link) as resp:
                    total = int(resp.headers.get("content-length", 0))
                    with open(path, "wb") as f:
                        curr = 0
                        async for chunk in resp.aiter_bytes(chunk_size=65536):
                            if active_tasks.get(task_id) == "CANCELLED":
                                raise Exception("USER_CANCELLED")
                            f.write(chunk)
                            curr += len(chunk)
                            await download_progress(curr, total, msg, start_t,
                                                    uname, task_id, engine, mode)

        # ── yt-dlp (social / video hosts) ─────────────────────────────────
        elif is_social or is_video_host:
            engine = "yt-dlp"
            mode = "#VideoHoster" if is_video_host else "#SocialMedia"
            start_t = time.time()
            loop = asyncio.get_running_loop()

            def ydl_hook(d):
                if d["status"] == "downloading":
                    curr  = d.get("downloaded_bytes", 0)
                    total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
                    if total > 0:
                        asyncio.run_coroutine_threadsafe(
                            download_progress(curr, total, msg, start_t,
                                              uname, task_id, engine, mode),
                            loop
                        )

            captured = {"title": ""}

            def run_ydl():
                opts = {
                    "outtmpl": f"{DOWNLOAD_DIR}{task_id}_%(playlist_index)s%(title)s.%(ext)s",
                    "noplaylist": False,
                    "progress_hooks": [ydl_hook],
                    "quiet": True,
                    "no_warnings": True,
                }
                if is_video_host:
                    opts["format"] = "best"
                    opts["http_headers"] = {
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/120.0.0.0 Safari/537.36"
                        ),
                        "Referer": url,
                    }
                else:
                    opts["format"] = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"

                if os.path.exists("cookies.txt"):
                    opts["cookiefile"] = "cookies.txt"

                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    if info:
                        captured["title"] = info.get("title", "") or info.get("webpage_url_basename", "")

            await safe_edit(msg,
                f"╭ Task By → 「{uname}」\n"
                f"┊ [{make_bar(0)}] 0.00%\n"
                f"┊ Status   : Extracting...\n"
                f"╰ Mode     : {mode}\n\n"
                f"{BOT_SIGNATURE}"
            )
            await asyncio.to_thread(run_ydl)
            video_title = captured["title"]

            files = sorted(glob.glob(f"{DOWNLOAD_DIR}{task_id}_*"), key=os.path.getsize)

            if not files:
                raise Exception("No se pudo descargar. Posible bloqueo de IP o enlace inválido.")

            # ── album / playlist ──────────────────────────────────────────
            if len(files) > 1:
                await safe_edit(msg,
                    f"╭ Task By → 「{uname}」\n"
                    f"┊ Status   : Uploading album...\n"
                    f"╰ Files    : {len(files)}\n\n"
                    f"{BOT_SIGNATURE}"
                )
                from pyrogram.types import InputMediaPhoto, InputMediaVideo
                group = []
                for f in files:
                    fl = f.lower()
                    if fl.endswith((".jpg", ".jpeg", ".png", ".webp")):
                        group.append(InputMediaPhoto(f))
                    elif fl.endswith((".mp4", ".mkv", ".webm")):
                        group.append(InputMediaVideo(f))
                for i in range(0, len(group), 10):
                    await client.send_media_group(message.chat.id, group[i:i+10])
                await msg.delete()
                return
            else:
                path = files[0]

        else:
            raise Exception("Enlace no soportado.")

        # ── ENCODE (videos mp4/mkv) ────────────────────────────────────────
        encoded_path = None
        if path and os.path.exists(path):
            lower_path = path.lower()
            if lower_path.endswith((".mp4", ".mkv", ".webm", ".avi", ".mov")):
                # Probe first so we can show the right status label
                info = await asyncio.to_thread(probe_video, path)
                is_h264 = info["codec"] == "h264"
                is_aac  = info["audio_codec"] in ("aac", "mp3", "mp4a")

                if is_h264 and is_aac and lower_path.endswith(".mp4"):
                    # Already perfect — skip encode entirely, just get thumbnail
                    print(f"[encode] skipped — already H.264+AAC MP4")
                else:
                    encoded_path = path + "_out.mp4"
                    status_label = "Remuxing..." if is_h264 else "Encoding..."
                    await safe_edit(msg,
                        encoding_panel(uname, 0, 0, os.path.getsize(path),
                                       0, 0, 0, task_id)
                    )
                    ok = await encode_video(path, encoded_path, msg, uname, task_id)
                    if ok and os.path.exists(encoded_path):
                        os.remove(path)
                        path = encoded_path
                    else:
                        if encoded_path and os.path.exists(encoded_path):
                            os.remove(encoded_path)
                        encoded_path = None

        # ── UPLOAD ────────────────────────────────────────────────────────
        if path and os.path.exists(path):
            start_up = time.time()
            await safe_edit(msg, upload_panel(uname, 0, 0, os.path.getsize(path),
                                               0, 0, 0, task_id))
            await upload_smart_file(client, message, path, msg, uname, task_id, title=video_title)
            try:
                await msg.delete()
            except Exception:
                pass

    except Exception as e:
        err = "🛑 Tarea cancelada." if "USER_CANCELLED" in str(e) else f"❌ Error: {str(e)[:200]}"
        await safe_edit(msg,
            f"╭ Task By → 「{uname}」\n"
            f"┊ {err}\n"
            f"╰──────────────\n\n"
            f"{BOT_SIGNATURE}"
        )
    finally:
        active_tasks.pop(task_id, None)
        last_updates.pop(task_id + "_dl", None)
        last_updates.pop(task_id + "_up", None)
        last_updates.pop(task_id + "_enc", None)

        for f in glob.glob(f"{DOWNLOAD_DIR}{task_id}_*"):
            try: os.remove(f)
            except: pass
        for p in [path, encoded_path]:
            if p and os.path.exists(p):
                try: os.remove(p)
                except: pass
        gc.collect()

# ─── QUEUE WORKER ────────────────────────────────────────────────────────────
async def queue_worker():
    while True:
        client, message, url, uname, uid, label = await download_queue.get()
        try:
            await procesar_descarga(client, message, url, uname, uid, label)
        except Exception as e:
            print(f"[worker] error: {e}")
        finally:
            download_queue.task_done()

# ─── BOT CLIENT ──────────────────────────────────────────────────────────────
bot = Client("bot_session",
             api_id=API_ID,
             api_hash=API_HASH,
             bot_token=BOT_TOKEN,
             workdir="/tmp")

# ─── /start ──────────────────────────────────────────────────────────────────
@bot.on_message(filters.command("start"))
async def cmd_start(client: Client, message: Message):
    print(f"[CMD] /start recibido de {message.from_user.id}")
    uid = message.from_user.id
    name = message.from_user.first_name
    await message.reply_text(
        f"🚀 ¡Hola, {name}!\n\n"
        f"Soy un bot descargador de medios. Envíame un enlace de:\n"
        f"YouTube • TikTok • Instagram • Twitter/X • Facebook\n"
        f"Mega.nz • Mediafire • y muchos más 💾\n\n"
        f"🆔 Tu ID: {uid}\n\n"
        f"{BOT_SIGNATURE}"
    )

# ─── /cancel_<task_id> ───────────────────────────────────────────────────────
@bot.on_message(filters.regex(r"^/cancel_(\S+)"))
async def cmd_cancel(client: Client, message: Message):
    task_id = message.matches[0].group(1)
    if task_id in active_tasks:
        active_tasks[task_id] = "CANCELLED"
        await message.reply_text("🛑 Cancelando descarga...")
    else:
        await message.reply_text("⚠️ No hay ninguna tarea activa con ese ID.")

# ─── /stat ───────────────────────────────────────────────────────────────────
@bot.on_message(filters.command(["stat", "Stat", "STAT"]))
async def cmd_stat(client: Client, message: Message):
    uptime  = get_readable_time(time.time() - start_time)
    ram     = psutil.virtual_memory()
    disk    = psutil.disk_usage("/tmp")
    cpu     = psutil.cpu_percent(interval=0.5)
    server  = platform.node()
    plat    = platform.system() + " " + platform.release()
    cpu_info = platform.processor() or platform.machine()

    await message.reply_text(
        f"╭─ Status Panel\n"
        f"┊ 🕐 Time on  : {uptime}\n"
        f"┊ 🧠 RAM      : {ram.percent:.1f}% ({get_readable_size(ram.used)}/{get_readable_size(ram.total)})\n"
        f"┊ 💾 Storage  : {get_readable_size(disk.used)}/{get_readable_size(disk.total)}\n"
        f"┊ 🖥️ Server   : {server}\n"
        f"┊ ⚙️ Platform : {plat}\n"
        f"┊ 🔧 CPU      : {cpu}% — {cpu_info}\n"
        f"╰─ Engine    : yt-dlp / Pyrogram\n\n"
        f"{BOT_SIGNATURE}"
    )

# ─── /reset ──────────────────────────────────────────────────────────────────
@bot.on_message(filters.command(["reset", "Reset", "RESET"]))
async def cmd_reset(client: Client, message: Message):
    msg = await message.reply_text("♻️ Reiniciando...")

    # Cancel all active tasks
    cancelled = len(active_tasks)
    for tid in list(active_tasks.keys()):
        active_tasks[tid] = "CANCELLED"

    freed = "Nada que limpiar"
    total_freed = 0
    for f in glob.glob(f"{DOWNLOAD_DIR}*"):
        try:
            size = os.path.getsize(f)
            os.remove(f)
            total_freed += size
        except Exception:
            pass

    if total_freed > 0:
        freed = get_readable_size(total_freed)

    gc.collect()
    new_uptime = "0s"

    await msg.edit_text(
        f"╭─「 Reset Completado ✅ 」\n"
        f"┊ 🔄 Estado    : Online\n"
        f"┊ 🕐 Uptime    : {new_uptime}\n"
        f"┊ 🧹 Liberado  : {freed}\n"
        f"┊ ⛔ Descargas : {'Canceladas' if cancelled > 0 else 'Ninguna activa'}\n"
        f"╰─ Engine     : CRDWV2\n\n"
        f"{BOT_SIGNATURE}"
    )

# ─── LINK HANDLER ────────────────────────────────────────────────────────────
@bot.on_message(
    filters.text
    & ~filters.command(["start", "stat", "Stat", "STAT",
                        "reset", "Reset", "RESET"])
    & ~filters.regex(r"^/cancel_")
)
async def handle_links(client: Client, message: Message):
    urls = re.findall(r"https?://[^\s]+", message.text.strip())
    if not urls:
        return

    uname = message.from_user.first_name
    uid   = message.from_user.id

    for i, url in enumerate(urls, 1):
        label = f"Cola: {i}/{len(urls)}"
        await download_queue.put((client, message, url, uname, uid, label))

    q = download_queue.qsize()
    if len(urls) == 1:
        await message.reply_text(
            f"📥 Enlace añadido a la cola.\n"
            f"🚦 Tareas en espera: {q}\n\n"
            f"{BOT_SIGNATURE}"
        )
    else:
        await message.reply_text(
            f"📥 {len(urls)} enlaces añadidos a la cola.\n"
            f"🚦 Total en espera: {q}\n\n"
            f"{BOT_SIGNATURE}"
        )

# ─── MAIN ────────────────────────────────────────────────────────────────────
async def main():
    async with bot:
        me = await bot.get_me()
        print(f"Bot iniciado ✓ — @{me.username} (ID: {me.id})")
        asyncio.get_event_loop().create_task(queue_worker())
        await asyncio.Event().wait()

if __name__ == "__main__":
    bot.run(main())
