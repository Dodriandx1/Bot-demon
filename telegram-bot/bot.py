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
import json
import struct
import base64
import platform
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from pyrogram import Client, filters, enums
from pyrogram.types import Message, CallbackQuery
from pyrogram.errors import MessageNotModified
from Crypto.Cipher import AES
from Crypto.Util import Counter

# ─── CONFIG ─────────────────────────────────────────────────────────────────
API_ID   = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
MEGA_EMAIL    = os.environ.get("MEGA_EMAIL", "")
MEGA_PASSWORD = os.environ.get("MEGA_PASSWORD", "")

DOWNLOAD_DIR = "/tmp/downloads/"
os.makedirs(DOWNLOAD_DIR, mode=0o777, exist_ok=True)

start_time = time.time()

active_tasks: dict = {}       # task_id → "RUNNING" | "CANCELLED"
_task_handles: dict = {}      # task_id → asyncio.Task  (for hard cancel)
_ydl_stop: dict = {}          # task_id → threading.Event (for yt-dlp thread)
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

def get_video_meta(video_path: str) -> dict:
    """Get width, height, duration for send_video."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height",
                "-show_entries", "format=duration",
                "-of", "csv=p=0",
                video_path,
            ],
            capture_output=True, text=True, timeout=10
        )
        lines = [l.strip() for l in result.stdout.strip().splitlines() if l.strip()]
        width, height, duration = 0, 0, 0
        for line in lines:
            parts = line.split(",")
            if len(parts) == 2:
                try:
                    width, height = int(parts[0]), int(parts[1])
                except ValueError:
                    pass
            elif len(parts) == 1:
                try:
                    duration = int(float(parts[0]))
                except ValueError:
                    pass
        return {"width": width, "height": height, "duration": duration}
    except Exception:
        return {"width": 0, "height": 0, "duration": 0}

# ─── MEGA DOWNLOADER (authenticated, no third-party libs) ────────────────────
_MEGA_API = "https://g.api.mega.co.nz/cs"
_MEGA_CHUNK = 1 * 1024 * 1024   # 1 MB chunks → fast download

def _mega_b64decode(s: str) -> bytes:
    s = s.replace('-', '+').replace('_', '/')
    s += '=' * (-len(s) % 4)
    return base64.b64decode(s)

def _mega_b64encode(b: bytes) -> str:
    return base64.b64encode(b).decode().replace('+', '-').replace('/', '_').rstrip('=')

def _a32(b: bytes) -> list:
    b += b'\x00' * (-len(b) % 4)
    return list(struct.unpack('>' + 'I' * (len(b) // 4), b))

def _a32_bytes(a: list) -> bytes:
    return struct.pack('>' + 'I' * len(a), *a)

def _aes_cbc_enc_a32(data: list, key: list) -> list:
    c = AES.new(_a32_bytes(key), AES.MODE_CBC, iv=b'\x00' * 16)
    return _a32(c.encrypt(_a32_bytes(data)))

def _aes_cbc_dec_a32(data: list, key: list) -> list:
    c = AES.new(_a32_bytes(key), AES.MODE_CBC, iv=b'\x00' * 16)
    return _a32(c.decrypt(_a32_bytes(data)))

def _mega_prepare_key(password: str) -> list:
    """MEGA password → AES key (65536 rounds, same as the official client)."""
    pw = _a32(password.encode('utf-8'))
    pkey = [0x93C467E3, 0x7DB0C7A4, 0xD1BE3F81, 0x0152CB56]
    for _ in range(0x10000):
        for i in range(0, len(pw), 4):
            block = (pw[i:i+4] + [0, 0, 0, 0])[:4]
            pkey = _aes_cbc_enc_a32(pkey, block)
    return pkey

def _mega_stringhash(s: str, aes_key: list) -> str:
    """MEGA e-mail → user-hash string."""
    h = [0, 0, 0, 0]
    for i, v in enumerate(_a32(s.encode('utf-8'))):
        h[i % 4] ^= v
    for _ in range(0x4000):
        h = _aes_cbc_enc_a32(h, aes_key)
    return _mega_b64encode(_a32_bytes([h[0], h[2]]))

def _mega_parse_url(url: str):
    m = re.search(r'mega\.nz/file/([^#\s]+)#([^\s&]+)', url)
    if m:
        return m.group(1), m.group(2)
    m = re.search(r'mega\.nz/#!([^!\s]+)!([^\s&]+)', url)
    if m:
        return m.group(1), m.group(2)
    return None, None

def _mega_decode_attrs(at_b64: str, aes_key_bytes: bytes) -> str:
    """Decrypt MEGA file attributes → filename."""
    raw = _mega_b64decode(at_b64)
    raw += b'\x00' * (-len(raw) % 16)
    plain = AES.new(aes_key_bytes, AES.MODE_CBC, iv=b'\x00' * 16).decrypt(raw)
    plain = plain.decode('utf-8', errors='ignore').rstrip('\x00')
    if plain.startswith('MEGA'):
        try:
            return json.loads(plain[4:]).get('n', '')
        except Exception:
            pass
    return ''

async def _mega_api(payload: list, sid: str = '') -> list:
    params = {'id': 1}
    if sid:
        params['sid'] = sid
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(_MEGA_API, params=params, json=payload)
    return r.json()

def _mega_mpi_read(buf: bytes, pos: int):
    """Read a MEGA multi-precision integer. Returns (value_int, new_pos)."""
    bits = (buf[pos] << 8) | buf[pos + 1]
    byte_len = (bits + 7) // 8
    val = int.from_bytes(buf[pos + 2: pos + 2 + byte_len], 'big')
    return val, pos + 2 + byte_len

async def mega_login(email: str, password: str) -> dict:
    """
    Authenticate with MEGA using email+password.
    Implements MEGA's password-key derivation, user-hash, RSA session decryption.
    Returns {'sid': session_id, 'master_key': [4 ints]}.
    """
    # CPU-heavy key derivation — run in thread so bot stays responsive
    pw_key = await asyncio.to_thread(_mega_prepare_key, password)
    uh     = _mega_stringhash(email.lower(), pw_key)

    data = await _mega_api([{"a": "us", "user": email.lower(), "uh": uh}])
    resp = data[0]
    if isinstance(resp, int):
        codes = {-2: "Contraseña incorrecta.", -3: "Demasiados intentos, espera.",
                 -9: "Cuenta no encontrada."}
        raise Exception(f"MEGA login {resp}: {codes.get(resp, 'error desconocido')}")

    # Decrypt master key with password key
    enc_mk = _a32(_mega_b64decode(resp['k']))
    mk     = _aes_cbc_dec_a32(enc_mk, pw_key)

    # --- Session ID ---
    # Simple case: tsid (temporary session id, no RSA needed)
    if 'tsid' in resp:
        tsid_bytes = _mega_b64decode(resp['tsid'])
        # Verify: encrypt first 16 bytes with master key → should match last 16 bytes
        verify = AES.new(_a32_bytes(mk), AES.MODE_CBC, iv=b'\x00'*16).encrypt(tsid_bytes[:16])
        if verify == tsid_bytes[16:]:
            return {'sid': resp['tsid'], 'master_key': mk}

    # Full RSA case: decrypt csid using private key stored in privk
    if 'csid' in resp and 'privk' in resp:
        try:
            # Decrypt private key blob with master key (AES-128-CBC)
            privk_enc  = _mega_b64decode(resp['privk'])
            privk_enc += b'\x00' * (-len(privk_enc) % 16)
            privk      = AES.new(_a32_bytes(mk), AES.MODE_CBC, iv=b'\x00'*16).decrypt(privk_enc)

            # Parse RSA private key: four MPIs → p, q, d, u
            pos  = 0
            p, pos = _mega_mpi_read(privk, pos)
            q, pos = _mega_mpi_read(privk, pos)
            d, pos = _mega_mpi_read(privk, pos)
            _u, pos = _mega_mpi_read(privk, pos)
            n = p * q

            # Decrypt csid with RSA: m = csid^d mod n
            csid_bytes = _mega_b64decode(resp['csid'])
            csid_int   = int.from_bytes(csid_bytes, 'big')
            m          = pow(csid_int, d, n)
            # Session is the first 43 bytes of the decrypted value
            m_bytes    = m.to_bytes((m.bit_length() + 7) // 8, 'big')
            sid        = _mega_b64encode(m_bytes[:43])
            return {'sid': sid, 'master_key': mk}
        except Exception as e:
            raise Exception(f"MEGA RSA session decrypt falló: {e}")

    raise Exception("MEGA login: respuesta inesperada del servidor.")

async def mega_download(url: str, dest_dir: str, task_id: str, progress_cb=None):
    """
    Download a MEGA file with account credentials when available,
    falling back to anonymous access for public links.
    Returns (local_path, title_without_ext).
    """
    handle, key_b64 = _mega_parse_url(url)
    if not handle:
        raise ValueError("URL de MEGA no válida. Debe ser mega.nz/file/HANDLE#KEY")

    # Try authenticated session first (higher quota, up to 2 GB+)
    sid = ''
    if MEGA_EMAIL and MEGA_PASSWORD:
        try:
            session = await mega_login(MEGA_EMAIL, MEGA_PASSWORD)
            sid = session['sid']
            print(f"[MEGA] Sesión autenticada OK — sid={sid[:8]}...")
        except Exception as e:
            print(f"[MEGA] Login falló ({e}), intentando modo anónimo...")

    # Fold URL key → AES-128 key + CTR IV
    raw = _mega_b64decode(key_b64)
    if len(raw) < 32:
        raise ValueError("Clave MEGA inválida en el URL.")
    k   = struct.unpack('>8I', raw[:32])
    aes_key_bytes = struct.pack('>4I',
        k[0]^k[4], k[1]^k[5], k[2]^k[6], k[3]^k[7])
    iv_int = (k[4] << 96) | (k[5] << 64)

    # Get download URL from MEGA API
    payload = [{"a": "g", "g": 1, "p": handle}]
    data    = await _mega_api(payload, sid=sid)
    item    = data[0]
    if isinstance(item, int):
        codes = {-2: "Enlace inválido o expirado.", -9: "Objeto no encontrado.",
                 -16: "Cuota de descarga excedida.", -18: "Recurso no disponible."}
        raise Exception(f"MEGA error {item}: {codes.get(item, 'desconocido')}")

    dl_url = item.get('g')
    total  = item.get('s', 0)
    if not dl_url:
        raise Exception("MEGA no devolvió URL de descarga.")

    filename = _mega_decode_attrs(item.get('at', ''), aes_key_bytes) or f"mega_{handle}"
    dest_path = os.path.join(dest_dir, f"{task_id}_{filename}")

    # Stream + decrypt AES-128-CTR
    ctr    = Counter.new(128, initial_value=iv_int, little_endian=False)
    cipher = AES.new(aes_key_bytes, AES.MODE_CTR, counter=ctr)

    async with httpx.AsyncClient(timeout=None, follow_redirects=True) as hclient:
        async with hclient.stream('GET', dl_url) as resp:
            downloaded = 0
            with open(dest_path, 'wb') as f:
                async for chunk in resp.aiter_bytes(_MEGA_CHUNK):
                    # This await point lets CancelledError propagate immediately
                    await asyncio.sleep(0)
                    orig_len = len(chunk)
                    # AES block must be multiple of 16; pad last chunk if needed
                    if orig_len % 16:
                        chunk += b'\x00' * (16 - orig_len % 16)
                    decrypted = cipher.decrypt(chunk)[:orig_len]
                    f.write(decrypted)
                    downloaded += orig_len
                    if progress_cb and total > 0:
                        await progress_cb(downloaded, total)

    title = filename.rsplit('.', 1)[0] if '.' in filename else filename
    return dest_path, title

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
    lower = fname.lower()
    # Choose emoji based on file type
    if lower.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif")):
        icon = "🖼️"
    elif lower.endswith((".mp3", ".m4a", ".wav", ".flac", ".ogg")):
        icon = "🎵"
    else:
        icon = "🎬"
    caption = f"{icon} <b>{display}</b>\n\n{BOT_SIGNATURE}"
    start_t = time.time()

    if lower.endswith((".mp4", ".mkv", ".webm", ".avi", ".mov")):
        thumb = extract_thumbnail(path)
        meta  = get_video_meta(path)
        try:
            await client.send_video(
                chat_id=message.chat.id,
                video=path,
                thumb=thumb,
                supports_streaming=True,
                width=meta["width"] or None,
                height=meta["height"] or None,
                duration=meta["duration"] or None,
                caption=caption,
                parse_mode=enums.ParseMode.HTML,
                progress=upload_progress,
                progress_args=(msg, start_t, uname, task_id)
            )
        except Exception as e:
            print(f"[upload] send_video failed ({e}), retrying without meta...")
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
            except Exception as e2:
                print(f"[upload] send_video retry failed ({e2}), sending as document")
                await client.send_document(
                    chat_id=message.chat.id,
                    document=path,
                    thumb=thumb,
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

    elif lower.endswith(".gif"):
        await client.send_animation(
            chat_id=message.chat.id,
            animation=path,
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

    try:
        while True:
            if active_tasks.get(task_id) == "CANCELLED":
                proc.kill()
                await proc.wait()
                return False
            line_bytes = await proc.stdout.readline()  # CancelledError arrives here
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
    except (asyncio.CancelledError, Exception):
        # Kill ffmpeg immediately on any cancellation or error
        try:
            proc.kill()
            await proc.wait()
        except Exception:
            pass
        raise   # re-raise so the outer handler knows

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

    # Register current asyncio Task so /cancel can forcefully cancel it
    _current = asyncio.current_task()
    if _current:
        _task_handles[task_id] = _current

    # threading.Event for yt-dlp (runs in a thread, can't use CancelledError)
    _stop_evt = threading.Event()
    _ydl_stop[task_id] = _stop_evt

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
            engine = "direct"
            mode = "#MEGA"
            start_t = time.time()

            await safe_edit(msg,
                f"╭ Task By → 「{uname}」\n"
                f"┊ [{make_bar(0)}] 0.00%\n"
                f"┊ Status   : Connecting to MEGA...\n"
                f"╰ Mode     : {mode}\n\n"
                f"{BOT_SIGNATURE}"
            )

            async def _mega_progress(curr, total):
                await download_progress(curr, total, msg, start_t,
                                        uname, task_id, engine, mode)

            path, video_title = await mega_download(
                url, DOWNLOAD_DIR, task_id, progress_cb=_mega_progress
            )

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
                        async for chunk in resp.aiter_bytes(chunk_size=1024*1024):
                            await asyncio.sleep(0)   # let CancelledError in immediately
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
                # Hard stop: raise inside the yt-dlp thread when cancelled
                if _stop_evt.is_set() or active_tasks.get(task_id) == "CANCELLED":
                    raise Exception("USER_CANCELLED")
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
                base_opts = {
                    "outtmpl": f"{DOWNLOAD_DIR}{task_id}_%(playlist_index)s%(title)s.%(ext)s",
                    "noplaylist": False,
                    "progress_hooks": [ydl_hook],
                    "quiet": True,
                    "no_warnings": True,
                }
                if is_video_host:
                    base_opts["format"] = "best"
                    base_opts["http_headers"] = {
                        "User-Agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/120.0.0.0 Safari/537.36"
                        ),
                        "Referer": url,
                    }
                else:
                    # Prefer mp4 video, but fall back to any format
                    # (handles image-only posts on Instagram, Twitter, etc.)
                    base_opts["format"] = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"

                if os.path.exists("cookies.txt"):
                    base_opts["cookiefile"] = "cookies.txt"

                def _extract(opts):
                    with yt_dlp.YoutubeDL(opts) as ydl:
                        info = ydl.extract_info(url, download=True)
                        if info:
                            captured["title"] = (
                                info.get("title", "")
                                or info.get("webpage_url_basename", "")
                            )

                try:
                    _extract(base_opts)
                except Exception as _e:
                    if "USER_CANCELLED" in str(_e):
                        raise  # propagate cancel immediately, no retry
                    # Retry without format restriction — handles photo-only posts
                    fallback = dict(base_opts)
                    fallback.pop("format", None)
                    _extract(fallback)

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
                from pyrogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument
                album_caption = f"🖼️ <b>{video_title}</b>\n\n{BOT_SIGNATURE}" if video_title else BOT_SIGNATURE
                group = []
                for idx, f in enumerate(files):
                    fl = f.lower()
                    cap = album_caption if idx == 0 else None
                    parse = enums.ParseMode.HTML if cap else None
                    if fl.endswith((".jpg", ".jpeg", ".png", ".webp")):
                        group.append(InputMediaPhoto(f, caption=cap, parse_mode=parse))
                    elif fl.endswith((".mp4", ".mkv", ".webm", ".avi", ".mov")):
                        group.append(InputMediaVideo(f, caption=cap, parse_mode=parse, supports_streaming=True))
                    elif fl.endswith(".gif"):
                        group.append(InputMediaDocument(f, caption=cap, parse_mode=parse))
                if group:
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

    except (Exception, asyncio.CancelledError) as e:
        is_cancel = isinstance(e, asyncio.CancelledError) or "USER_CANCELLED" in str(e)
        err = "🛑 Tarea cancelada." if is_cancel else f"❌ Error: {str(e)[:200]}"
        try:
            await safe_edit(msg,
                f"╭ Task By → 「{uname}」\n"
                f"┊ {err}\n"
                f"╰──────────────\n\n"
                f"{BOT_SIGNATURE}"
            )
        except Exception:
            pass
    finally:
        active_tasks.pop(task_id, None)
        _task_handles.pop(task_id, None)
        _ydl_stop.pop(task_id, None)
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
            # Run each download in its OWN asyncio Task so that
            # asyncio.current_task() inside procesar_descarga is per-download,
            # and task.cancel() only kills that one download.
            dl_task = asyncio.create_task(
                procesar_descarga(client, message, url, uname, uid, label)
            )
            await dl_task
        except (Exception, asyncio.CancelledError) as e:
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
        # 1. Set the flag (checked by all progress callbacks and MEGA loop)
        active_tasks[task_id] = "CANCELLED"
        # 2. Signal yt-dlp thread to stop immediately
        if task_id in _ydl_stop:
            _ydl_stop[task_id].set()
        # 3. Cancel the asyncio Task (stops httpx streams and Pyrogram uploads)
        task_handle = _task_handles.get(task_id)
        if task_handle and not task_handle.done():
            task_handle.cancel()
        await message.reply_text("🛑 Tarea cancelada.")
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
