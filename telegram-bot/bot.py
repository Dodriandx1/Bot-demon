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

# ─── CONFIGURACIÓN PRINCIPAL ────────────────────────────────────────────────
API_ID    = int(os.environ["API_ID"])
API_HASH  = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

# Credenciales de MEGA (Opcional)
MEGA_EMAIL    = os.environ.get("MEGA_EMAIL", "")
MEGA_PASSWORD = os.environ.get("MEGA_PASSWORD", "")

# Credenciales para yt-dlp (Instagram, Facebook, Twitter, etc)
SOCIAL_USERNAME = os.environ.get("SOCIAL_USERNAME", "")
SOCIAL_PASSWORD = os.environ.get("SOCIAL_PASSWORD", "")

# ─── CREDENCIALES DE STREAMING (Nuevas) ─────────────────────────────────────
# Cada plataforma puede tener sus propias credenciales separadas
NETFLIX_EMAIL     = os.environ.get("NETFLIX_EMAIL", "")
NETFLIX_PASSWORD  = os.environ.get("NETFLIX_PASSWORD", "")

DISNEY_EMAIL      = os.environ.get("DISNEY_EMAIL", "")
DISNEY_PASSWORD   = os.environ.get("DISNEY_PASSWORD", "")

PRIME_EMAIL       = os.environ.get("PRIME_EMAIL", "")
PRIME_PASSWORD    = os.environ.get("PRIME_PASSWORD", "")

CRUNCHYROLL_EMAIL    = os.environ.get("CRUNCHYROLL_EMAIL", "")
CRUNCHYROLL_PASSWORD = os.environ.get("CRUNCHYROLL_PASSWORD", "")

HBO_EMAIL         = os.environ.get("HBO_EMAIL", "")
HBO_PASSWORD      = os.environ.get("HBO_PASSWORD", "")

APPLE_EMAIL       = os.environ.get("APPLE_EMAIL", "")
APPLE_PASSWORD    = os.environ.get("APPLE_PASSWORD", "")

PARAMOUNT_EMAIL   = os.environ.get("PARAMOUNT_EMAIL", "")
PARAMOUNT_PASSWORD= os.environ.get("PARAMOUNT_PASSWORD", "")

PLUTO_EMAIL       = os.environ.get("PLUTO_EMAIL", "")
PLUTO_PASSWORD    = os.environ.get("PLUTO_PASSWORD", "")

PEACOCK_EMAIL     = os.environ.get("PEACOCK_EMAIL", "")
PEACOCK_PASSWORD  = os.environ.get("PEACOCK_PASSWORD", "")

YOUTUBE_COOKIES   = os.environ.get("YOUTUBE_COOKIES_FILE", "cookies.txt")

# Tu ID de Owner (Dueño Absoluto)
ADMIN_ID = 7815099965  
AUTH_FILE = "authorized_users.json"

DOWNLOAD_DIR = "/tmp/downloads/"
os.makedirs(DOWNLOAD_DIR, mode=0o777, exist_ok=True)

start_time = time.time()

# Diccionarios de control de tareas
active_tasks: dict = {}       
_task_handles: dict = {}      
_ydl_stop: dict = {}          
last_updates: dict = {}

download_queue: asyncio.Queue = asyncio.Queue()

BOT_SIGNATURE = "✪ Bot By → @The_canst & @kaiser_yt"


# ─── MAPA DE PLATAFORMAS DE STREAMING ───────────────────────────────────────
# Cada entrada define: dominios que activan esta plataforma,
# icono visual, nombre legible y cómo construir las opciones de yt-dlp
STREAMING_PLATFORMS = {
    "netflix": {
        "domains": ["netflix.com"],
        "icon": "🔴",
        "name": "Netflix",
        "mode": "#Netflix",
        # Netflix necesita L3 Widevine; yt-dlp + N_m3u8DL-RE o shaka-packager
        # Las credenciales se pasan como netrc o como parámetro
        "email": NETFLIX_EMAIL,
        "password": NETFLIX_PASSWORD,
        "ydl_extra": {
            "format": "bestvideo[height<=1080]+bestaudio/best",
            "allow_unplayable_formats": True,
            "fixup": "never",
        },
    },
    "disney": {
        "domains": ["disneyplus.com", "disney+.com", "hotstar.com"],
        "icon": "🔵",
        "name": "Disney+",
        "mode": "#DisneyPlus",
        "email": DISNEY_EMAIL,
        "password": DISNEY_PASSWORD,
        "ydl_extra": {
            "format": "bestvideo[height<=1080]+bestaudio/best",
            "allow_unplayable_formats": True,
            "fixup": "never",
        },
    },
    "prime": {
        "domains": ["primevideo.com", "amazon.com/video", "amazon.com/dp"],
        "icon": "🩵",
        "name": "Amazon Prime",
        "mode": "#PrimeVideo",
        "email": PRIME_EMAIL,
        "password": PRIME_PASSWORD,
        "ydl_extra": {
            "format": "bestvideo[height<=1080]+bestaudio/best",
            "allow_unplayable_formats": True,
            "fixup": "never",
        },
    },
    "crunchyroll": {
        "domains": ["crunchyroll.com"],
        "icon": "🟠",
        "name": "Crunchyroll",
        "mode": "#Crunchyroll",
        "email": CRUNCHYROLL_EMAIL,
        "password": CRUNCHYROLL_PASSWORD,
        "ydl_extra": {
            "format": "bestvideo[height<=1080]+bestaudio/best",
        },
    },
    "hbo": {
        "domains": ["max.com", "hbomax.com", "hbo.com"],
        "icon": "🟣",
        "name": "HBO Max",
        "mode": "#HBOMax",
        "email": HBO_EMAIL,
        "password": HBO_PASSWORD,
        "ydl_extra": {
            "format": "bestvideo[height<=1080]+bestaudio/best",
            "allow_unplayable_formats": True,
        },
    },
    "apple": {
        "domains": ["tv.apple.com"],
        "icon": "⚪",
        "name": "Apple TV+",
        "mode": "#AppleTV",
        "email": APPLE_EMAIL,
        "password": APPLE_PASSWORD,
        "ydl_extra": {
            "format": "bestvideo[height<=1080]+bestaudio/best",
            "allow_unplayable_formats": True,
        },
    },
    "paramount": {
        "domains": ["paramountplus.com", "paramount+.com"],
        "icon": "🔷",
        "name": "Paramount+",
        "mode": "#ParamountPlus",
        "email": PARAMOUNT_EMAIL,
        "password": PARAMOUNT_PASSWORD,
        "ydl_extra": {
            "format": "bestvideo[height<=1080]+bestaudio/best",
        },
    },
    "pluto": {
        "domains": ["pluto.tv"],
        "icon": "🟡",
        "name": "Pluto TV",
        "mode": "#PlutoTV",
        "email": PLUTO_EMAIL,
        "password": PLUTO_PASSWORD,
        "ydl_extra": {
            "format": "best",
        },
    },
    "peacock": {
        "domains": ["peacocktv.com"],
        "icon": "🦚",
        "name": "Peacock",
        "mode": "#Peacock",
        "email": PEACOCK_EMAIL,
        "password": PEACOCK_PASSWORD,
        "ydl_extra": {
            "format": "bestvideo[height<=1080]+bestaudio/best",
        },
    },
    "youtube_premium": {
        "domains": ["youtube.com/watch", "youtu.be", "youtube.com/shorts"],
        "icon": "📺",
        "name": "YouTube",
        "mode": "#YouTube",
        "email": "",
        "password": "",
        "ydl_extra": {
            "format": "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        },
    },
    "twitch": {
        "domains": ["twitch.tv"],
        "icon": "💜",
        "name": "Twitch",
        "mode": "#Twitch",
        "email": "",
        "password": "",
        "ydl_extra": {
            "format": "best",
        },
    },
    "vimeo": {
        "domains": ["vimeo.com"],
        "icon": "🎞️",
        "name": "Vimeo",
        "mode": "#Vimeo",
        "email": "",
        "password": "",
        "ydl_extra": {
            "format": "bestvideo[height<=1080]+bestaudio/best",
        },
    },
    "dailymotion": {
        "domains": ["dailymotion.com", "dai.ly"],
        "icon": "🎬",
        "name": "Dailymotion",
        "mode": "#Dailymotion",
        "email": "",
        "password": "",
        "ydl_extra": {
            "format": "bestvideo[height<=1080]+bestaudio/best",
        },
    },
    "deezer": {
        "domains": ["deezer.com"],
        "icon": "🎵",
        "name": "Deezer",
        "mode": "#Deezer",
        "email": "",
        "password": "",
        "ydl_extra": {
            "format": "best",
        },
    },
    "soundcloud": {
        "domains": ["soundcloud.com"],
        "icon": "🎶",
        "name": "SoundCloud",
        "mode": "#SoundCloud",
        "email": "",
        "password": "",
        "ydl_extra": {
            "format": "bestaudio/best",
        },
    },
    "spotify": {
        "domains": ["open.spotify.com"],
        "icon": "🟢",
        "name": "Spotify",
        "mode": "#Spotify",
        "email": "",
        "password": "",
        # Spotify no tiene descarga directa vía yt-dlp;
        # se usa spotDL como motor alternativo
        "use_spotdl": True,
        "ydl_extra": {},
    },
    "bilibili": {
        "domains": ["bilibili.com", "b23.tv"],
        "icon": "💙",
        "name": "Bilibili",
        "mode": "#Bilibili",
        "email": "",
        "password": "",
        "ydl_extra": {
            "format": "bestvideo[height<=1080]+bestaudio/best",
        },
    },
    "niconico": {
        "domains": ["nicovideo.jp", "nico.ms"],
        "icon": "⬜",
        "name": "NicoNico",
        "mode": "#NicoNico",
        "email": "",
        "password": "",
        "ydl_extra": {
            "format": "best",
        },
    },
    "rumble": {
        "domains": ["rumble.com"],
        "icon": "🟤",
        "name": "Rumble",
        "mode": "#Rumble",
        "email": "",
        "password": "",
        "ydl_extra": {
            "format": "best",
        },
    },
    "odysee": {
        "domains": ["odysee.com", "lbry.tv"],
        "icon": "🌊",
        "name": "Odysee",
        "mode": "#Odysee",
        "email": "",
        "password": "",
        "ydl_extra": {
            "format": "bestvideo[height<=1080]+bestaudio/best",
        },
    },
}


def detect_streaming_platform(url: str):
    """
    Detecta si la URL pertenece a una plataforma de streaming conocida.
    Retorna (key, platform_dict) o (None, None).
    """
    url_lower = url.lower()
    for key, plat in STREAMING_PLATFORMS.items():
        for domain in plat["domains"]:
            if domain in url_lower:
                return key, plat
    return None, None


# ─── SISTEMA DE AUTORIZACIÓN Y RANGOS ────────────────────────────────────────
authorized_users = {}

if os.path.exists(AUTH_FILE):
    with open(AUTH_FILE, "r") as f:
        try:
            data = json.load(f)
            if isinstance(data, list):
                for uid in data:
                    authorized_users[str(uid)] = {"role": "user", "username": "", "name": ""}
            elif isinstance(data, dict):
                authorized_users = data
        except Exception:
            pass

def save_auth_users():
    with open(AUTH_FILE, "w") as f:
        json.dump(authorized_users, f)

def is_auth(uid: int) -> bool:
    return uid == ADMIN_ID or str(uid) in authorized_users

def is_admin(uid: int) -> bool:
    if uid == ADMIN_ID: 
        return True
    user_data = authorized_users.get(str(uid))
    return user_data and user_data.get("role") == "admin"


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


# ─── UTILIDADES GENERALES ────────────────────────────────────────────────────
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
    filled = int(percentage / 100 * width)
    if filled >= width:
        return "⬢" * width
    bar = "⬢" * filled + "◉" + "◌" * (width - filled - 1)
    return bar

def extract_thumbnail(video_path: str):
    thumb = video_path + ".jpg"
    try:
        subprocess.run(
            ["ffmpeg", "-i", video_path, "-ss", "00:00:01", "-vframes", "1",
             "-vf", "scale=320:-1", "-q:v", "2", thumb, "-y"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15, check=True
        )
        return thumb if os.path.exists(thumb) else None
    except Exception:
        return None

def get_video_meta(video_path: str) -> dict:
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height",
             "-show_entries", "format=duration",
             "-of", "csv=p=0", video_path],
            capture_output=True, text=True, timeout=10
        )
        lines = [l.strip() for l in result.stdout.strip().splitlines() if l.strip()]
        width, height, duration = 0, 0, 0
        for line in lines:
            parts = line.split(",")
            if len(parts) == 2:
                try: width, height = int(parts[0]), int(parts[1])
                except ValueError: pass
            elif len(parts) == 1:
                try: duration = int(float(parts[0]))
                except ValueError: pass
        return {"width": width, "height": height, "duration": duration}
    except Exception:
        return {"width": 0, "height": 0, "duration": 0}


# ─── MOTOR MEGA NATIVO ───────────────────────────────────────────────────────
_MEGA_API = "https://g.api.mega.co.nz/cs"
_MEGA_CHUNK = 1 * 1024 * 1024

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
    pw = _a32(password.encode('utf-8'))
    pkey = [0x93C467E3, 0x7DB0C7A4, 0xD1BE3F81, 0x0152CB56]
    for _ in range(0x10000):
        for i in range(0, len(pw), 4):
            block = (pw[i:i+4] + [0, 0, 0, 0])[:4]
            pkey = _aes_cbc_enc_a32(pkey, block)
    return pkey

def _mega_stringhash(s: str, aes_key: list) -> str:
    h = [0, 0, 0, 0]
    for i, v in enumerate(_a32(s.encode('utf-8'))):
        h[i % 4] ^= v
    for _ in range(0x4000):
        h = _aes_cbc_enc_a32(h, aes_key)
    return _mega_b64encode(_a32_bytes([h[0], h[2]]))

def _mega_parse_url(url: str):
    m = re.search(r'mega\.nz/file/([^#\s]+)#([^\s&]+)', url)
    if m: return m.group(1), m.group(2)
    m = re.search(r'mega\.nz/#!([^!\s]+)!([^\s&]+)', url)
    if m: return m.group(1), m.group(2)
    return None, None

def _mega_decode_attrs(at_b64: str, aes_key_bytes: bytes) -> str:
    raw = _mega_b64decode(at_b64)
    raw += b'\x00' * (-len(raw) % 16)
    plain = AES.new(aes_key_bytes, AES.MODE_CBC, iv=b'\x00' * 16).decrypt(raw)
    plain = plain.decode('utf-8', errors='ignore').rstrip('\x00')
    if plain.startswith('MEGA'):
        try: return json.loads(plain[4:]).get('n', '')
        except Exception: pass
    return ''

async def _mega_api(payload: list, sid: str = '') -> list:
    params = {'id': 1}
    if sid: params['sid'] = sid
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(_MEGA_API, params=params, json=payload)
    return r.json()

def _mega_mpi_read(buf: bytes, pos: int):
    bits = (buf[pos] << 8) | buf[pos + 1]
    byte_len = (bits + 7) // 8
    val = int.from_bytes(buf[pos + 2: pos + 2 + byte_len], 'big')
    return val, pos + 2 + byte_len

async def mega_login(email: str, password: str) -> dict:
    pw_key = await asyncio.to_thread(_mega_prepare_key, password)
    uh     = _mega_stringhash(email.lower(), pw_key)
    data = await _mega_api([{"a": "us", "user": email.lower(), "uh": uh}])
    resp = data[0]
    if isinstance(resp, int):
        codes = {-2: "Contraseña incorrecta.", -3: "Demasiados intentos.", -9: "Cuenta no encontrada."}
        raise Exception(f"MEGA login {resp}: {codes.get(resp, 'error desconocido')}")
    enc_mk = _a32(_mega_b64decode(resp['k']))
    mk     = _aes_cbc_dec_a32(enc_mk, pw_key)
    if 'tsid' in resp:
        tsid_bytes = _mega_b64decode(resp['tsid'])
        verify = AES.new(_a32_bytes(mk), AES.MODE_CBC, iv=b'\x00'*16).encrypt(tsid_bytes[:16])
        if verify == tsid_bytes[16:]:
            return {'sid': resp['tsid'], 'master_key': mk}
    if 'csid' in resp and 'privk' in resp:
        try:
            privk_enc  = _mega_b64decode(resp['privk'])
            privk_enc += b'\x00' * (-len(privk_enc) % 16)
            privk      = AES.new(_a32_bytes(mk), AES.MODE_CBC, iv=b'\x00'*16).decrypt(privk_enc)
            pos  = 0
            p, pos = _mega_mpi_read(privk, pos)
            q, pos = _mega_mpi_read(privk, pos)
            d, pos = _mega_mpi_read(privk, pos)
            _u, pos = _mega_mpi_read(privk, pos)
            n = p * q
            csid_bytes = _mega_b64decode(resp['csid'])
            csid_int   = int.from_bytes(csid_bytes, 'big')
            m          = pow(csid_int, d, n)
            m_bytes    = m.to_bytes((m.bit_length() + 7) // 8, 'big')
            sid        = _mega_b64encode(m_bytes[:43])
            return {'sid': sid, 'master_key': mk}
        except Exception as e:
            raise Exception(f"MEGA RSA session decrypt falló: {e}")
    raise Exception("MEGA login: respuesta inesperada del servidor.")

async def mega_download(url: str, dest_dir: str, task_id: str, progress_cb=None):
    handle, key_b64 = _mega_parse_url(url)
    if not handle:
        raise ValueError("URL de MEGA no válida.")
    sid = ''
    if MEGA_EMAIL and MEGA_PASSWORD:
        try:
            session = await mega_login(MEGA_EMAIL, MEGA_PASSWORD)
            sid = session['sid']
        except Exception:
            pass
    raw = _mega_b64decode(key_b64)
    if len(raw) < 32: 
        raise ValueError("Clave MEGA inválida en el URL.")
    k   = struct.unpack('>8I', raw[:32])
    aes_key_bytes = struct.pack('>4I', k[0]^k[4], k[1]^k[5], k[2]^k[6], k[3]^k[7])
    iv_int = (k[4] << 96) | (k[5] << 64)
    payload = [{"a": "g", "g": 1, "p": handle}]
    data    = await _mega_api(payload, sid=sid)
    item    = data[0]
    if isinstance(item, int):
        codes = {-2: "Enlace inválido o expirado.", -9: "Objeto no encontrado.",
                 -16: "Cuota de descarga excedida.", -18: "Recurso no disponible."}
        raise Exception(f"MEGA error {item}: {codes.get(item, 'desconocido')}")
    dl_url = item.get('g')
    total  = item.get('s', 0)
    if not dl_url: raise Exception("MEGA no devolvió URL de descarga.")
    filename = _mega_decode_attrs(item.get('at', ''), aes_key_bytes) or f"mega_{handle}"
    dest_path = os.path.join(dest_dir, f"{task_id}_{filename}")
    ctr    = Counter.new(128, initial_value=iv_int, little_endian=False)
    cipher = AES.new(aes_key_bytes, AES.MODE_CTR, counter=ctr)
    async with httpx.AsyncClient(timeout=None, follow_redirects=True) as hclient:
        async with hclient.stream('GET', dl_url) as resp:
            downloaded = 0
            with open(dest_path, 'wb') as f:
                async for chunk in resp.aiter_bytes(_MEGA_CHUNK):
                    await asyncio.sleep(0)
                    orig_len = len(chunk)
                    if orig_len % 16: chunk += b'\x00' * (16 - orig_len % 16)
                    decrypted = cipher.decrypt(chunk)[:orig_len]
                    f.write(decrypted)
                    downloaded += orig_len
                    if progress_cb and total > 0: await progress_cb(downloaded, total)
    title = filename.rsplit('.', 1)[0] if '.' in filename else filename
    return dest_path, title


# ─── PANELES DE PROGRESO ─────────────────────────────────────────────────────
async def safe_edit(msg: Message, text: str):
    try: await msg.edit_text(text, parse_mode=None)
    except MessageNotModified: pass
    except Exception: pass

def download_panel(uname, percentage, status, done_bytes, total_bytes,
                   speed_bps, elapsed, eta, engine, mode, task_id) -> str:
    bar = make_bar(percentage)
    return (
        f"╭ Task By → 「{uname}」\n"
        f"┊ [{bar}] {percentage:.2f}%\n"
        f"┊ Status   : {status}\n"
        f"┊ Done     : {get_readable_size(done_bytes)}\n"
        f"┊ Total    : {get_readable_size(total_bytes)}\n"
        f"┊ Speed    : {get_readable_size(speed_bps)}/s\n"
        f"┊ ETA      : {get_readable_time(eta)}\n"
        f"┊ Past     : {get_readable_time(elapsed)}\n"
        f"┊ Engine   : {engine}\n"
        f"╰ Mode     : {mode}\n"
        f"⋗ Stop : /cancel_{task_id}\n\n"
        f"{BOT_SIGNATURE}"
    )

def encoding_panel(uname, percentage, done_bytes, total_bytes,
                   fps, elapsed, eta, task_id) -> str:
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

def upload_panel(uname, percentage, done_bytes, total_bytes,
                 speed_bps, elapsed, eta, task_id) -> str:
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

async def download_progress(current, total, msg, start_t, uname, task_id, engine, mode):
    if active_tasks.get(task_id) == "CANCELLED": 
        raise asyncio.CancelledError("USER_CANCELLED")
    now = time.time()
    if now - last_updates.get(task_id + "_dl", 0) < 3 and current < total: 
        return
    last_updates[task_id + "_dl"] = now
    elapsed = now - start_t
    pct = (current / total * 100) if total > 0 else 0
    speed = current / elapsed if elapsed > 0 else 0
    eta = (total - current) / speed if speed > 0 else 0
    panel = download_panel(uname, pct, "Download", current, total, speed, elapsed, eta, engine, mode, task_id)
    await safe_edit(msg, panel)

async def upload_progress(current, total, msg, start_t, uname, task_id):
    if active_tasks.get(task_id) == "CANCELLED": 
        raise asyncio.CancelledError("USER_CANCELLED")
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


# ─── SUBIDA INTELIGENTE ──────────────────────────────────────────────────────
async def upload_smart_file(client, message, path, msg, uname, task_id, title=""):
    fname = os.path.basename(path)
    display = title.strip() if title.strip() else fname
    lower = fname.lower()
    if lower.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp")): icon = "🖼️"
    elif lower.endswith((".mp3", ".m4a", ".wav", ".flac", ".ogg")): icon = "🎵"
    else: icon = "🎬"
    caption = f"{icon} <b>{display}</b>\n\n{BOT_SIGNATURE}"
    start_t = time.time()

    if lower.endswith((".mp4", ".mkv", ".webm", ".avi", ".mov")):
        thumb = extract_thumbnail(path)
        meta  = get_video_meta(path)
        try:
            await client.send_video(
                chat_id=message.chat.id, video=path, thumb=thumb, supports_streaming=True,
                width=meta["width"] or None, height=meta["height"] or None,
                duration=meta["duration"] or None, caption=caption, parse_mode=enums.ParseMode.HTML,
                progress=upload_progress, progress_args=(msg, start_t, uname, task_id)
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            try:
                await client.send_video(
                    chat_id=message.chat.id, video=path, thumb=thumb, supports_streaming=True,
                    caption=caption, parse_mode=enums.ParseMode.HTML,
                    progress=upload_progress, progress_args=(msg, start_t, uname, task_id)
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                await client.send_document(
                    chat_id=message.chat.id, document=path, thumb=thumb, caption=caption,
                    parse_mode=enums.ParseMode.HTML, progress=upload_progress,
                    progress_args=(msg, start_t, uname, task_id)
                )
        finally:
            if thumb and os.path.exists(thumb):
                try: os.remove(thumb)
                except Exception: pass
    elif lower.endswith((".jpg", ".jpeg", ".png", ".webp", ".bmp")):
        await client.send_photo(
            chat_id=message.chat.id, photo=path, caption=caption, parse_mode=enums.ParseMode.HTML,
            progress=upload_progress, progress_args=(msg, start_t, uname, task_id)
        )
    elif lower.endswith(".gif"):
        await client.send_animation(
            chat_id=message.chat.id, animation=path, caption=caption, parse_mode=enums.ParseMode.HTML,
            progress=upload_progress, progress_args=(msg, start_t, uname, task_id)
        )
    elif lower.endswith((".mp3", ".m4a", ".wav", ".flac", ".ogg")):
        await client.send_audio(
            chat_id=message.chat.id, audio=path, caption=caption, parse_mode=enums.ParseMode.HTML,
            progress=upload_progress, progress_args=(msg, start_t, uname, task_id)
        )
    else:
        await client.send_document(
            chat_id=message.chat.id, document=path, caption=caption, parse_mode=enums.ParseMode.HTML,
            progress=upload_progress, progress_args=(msg, start_t, uname, task_id)
        )


# ─── RECODIFICADOR DE VIDEO ───────────────────────────────────────────────────
def probe_video(input_path: str) -> dict:
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=codec_name",
             "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", input_path],
            capture_output=True, text=True
        )
        lines = [l.strip() for l in result.stdout.strip().splitlines() if l.strip()]
        codec = lines[0] if lines else "unknown"
        duration = float(lines[1]) if len(lines) > 1 else 0.0
        audio = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "a:0",
             "-show_entries", "stream=codec_name",
             "-of", "default=noprint_wrappers=1:nokey=1", input_path],
            capture_output=True, text=True
        )
        audio_codec = audio.stdout.strip().splitlines()[0].strip() if audio.stdout.strip() else "unknown"
        return {"codec": codec, "duration": duration, "audio_codec": audio_codec}
    except Exception:
        return {"codec": "unknown", "duration": 0.0, "audio_codec": "unknown"}

async def encode_video(input_path, output_path, msg, uname, task_id) -> bool:
    info = await asyncio.to_thread(probe_video, input_path)
    codec = info["codec"]
    total_dur = info["duration"]
    audio_codec = info["audio_codec"]
    input_size  = os.path.getsize(input_path)
    is_h264 = codec == "h264"
    is_aac_compat = audio_codec in ("aac", "mp3", "mp4a")

    if is_h264 and is_aac_compat:
        cmd = ["ffmpeg", "-i", input_path, "-c:v", "copy", "-c:a", "copy", "-movflags", "+faststart", "-y", output_path]
    elif is_h264:
        cmd = ["ffmpeg", "-i", input_path, "-c:v", "copy", "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", "-y", output_path]
    else:
        cmd = ["ffmpeg", "-i", input_path, "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
               "-c:a", "aac", "-b:a", "128k", "-movflags", "+faststart", "-progress", "pipe:1", "-nostats", "-y", output_path]

    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
    start_t = time.time()
    fps_val = 0.0
    time_done = 0.0
    try:
        while True:
            if active_tasks.get(task_id) == "CANCELLED":
                proc.kill(); await proc.wait(); return False
            line_bytes = await proc.stdout.readline()
            if not line_bytes: break
            line = line_bytes.decode().strip()
            if line.startswith("fps="):
                try: fps_val = float(line.split("=")[1])
                except Exception: pass
            if line.startswith("out_time_ms="):
                try: time_done = int(line.split("=")[1]) / 1_000_000
                except Exception: pass
            now = time.time()
            if now - last_updates.get(task_id + "_enc", 0) >= 2:
                last_updates[task_id + "_enc"] = now
                elapsed = now - start_t
                pct = min((time_done / total_dur * 100) if total_dur > 0 else 0, 99)
                eta = ((total_dur - time_done) / fps_val * 25) if fps_val > 0 else 0
                panel = encoding_panel(uname, pct, int(input_size * pct / 100), input_size, fps_val, elapsed, eta, task_id)
                await safe_edit(msg, panel)
    except (asyncio.CancelledError, Exception):
        try: proc.kill(); await proc.wait()
        except Exception: pass
        raise
    await proc.wait()
    return proc.returncode == 0 and os.path.exists(output_path)


# ─── MOTOR SPOTDL (SPOTIFY) ──────────────────────────────────────────────────
async def spotify_download(url: str, dest_dir: str, task_id: str, msg: Message, uname: str) -> tuple:
    """
    Descarga canciones/álbumes/playlists de Spotify usando spotDL.
    spotDL busca el audio en YouTube y lo descarga; no requiere cuenta Premium.
    Retorna (lista_de_archivos, titulo).
    """
    await safe_edit(msg, (
        f"╭ Task By → 「{uname}」\n"
        f"┊ [{make_bar(0)}] 0.00%\n"
        f"┊ Status   : Conectando a Spotify...\n"
        f"╰ Mode     : #Spotify\n\n"
        f"{BOT_SIGNATURE}"
    ))

    cmd = [
        "spotdl", "download", url,
        "--output", f"{dest_dir}{task_id}_{{title}}.{{ext}}",
        "--format", "mp3",
        "--bitrate", "320k",
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )

    title_captured = ""
    start_t = time.time()

    try:
        while True:
            if active_tasks.get(task_id) == "CANCELLED":
                proc.kill(); await proc.wait()
                raise asyncio.CancelledError("USER_CANCELLED")
            line_bytes = await proc.stdout.readline()
            if not line_bytes:
                break
            line = line_bytes.decode(errors="ignore").strip()
            # spotDL imprime "Downloaded X" cuando termina cada canción
            match = re.search(r'Downloaded\s+"?(.+?)"?\s', line)
            if match:
                title_captured = match.group(1)
                now = time.time()
                await safe_edit(msg, (
                    f"╭ Task By → 「{uname}」\n"
                    f"┊ [{make_bar(50)}] Descargando...\n"
                    f"┊ Status   : {title_captured[:40]}\n"
                    f"┊ Past     : {get_readable_time(now - start_t)}\n"
                    f"╰ Mode     : #Spotify\n\n"
                    f"{BOT_SIGNATURE}"
                ))
    except asyncio.CancelledError:
        raise
    except Exception:
        pass

    await proc.wait()
    files = sorted(glob.glob(f"{dest_dir}{task_id}_*"))
    return files, title_captured or "Spotify"


# ─── MOTOR YT-DLP UNIFICADO CON SOPORTE STREAMING ───────────────────────────
def build_ydl_opts(task_id: str, url: str, loop, msg, start_t, uname,
                   engine: str, mode: str, platform_info: dict, stop_evt) -> dict:
    """
    Construye las opciones de yt-dlp adaptadas a la plataforma.
    Maneja cookies, credenciales y opciones especiales por plataforma.
    """
    def ydl_hook(d):
        if stop_evt.is_set() or active_tasks.get(task_id) == "CANCELLED":
            raise ValueError("USER_CANCELLED")
        if d["status"] == "downloading":
            curr  = d.get("downloaded_bytes", 0)
            total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
            if total > 0:
                asyncio.run_coroutine_threadsafe(
                    download_progress(curr, total, msg, start_t, uname, task_id, engine, mode),
                    loop
                )

    opts = {
        "outtmpl": f"{DOWNLOAD_DIR}{task_id}_%(title)s.%(ext)s",
        "noplaylist": False,
        "progress_hooks": [ydl_hook],
        "quiet": True,
        "no_warnings": True,
        "merge_output_format": "mp4",
    }

    # Aplicar opciones específicas de la plataforma
    if platform_info:
        opts.update(platform_info.get("ydl_extra", {}))
        email = platform_info.get("email", "")
        password = platform_info.get("password", "")
        if email and password:
            opts["username"] = email
            opts["password"] = password
    else:
        # Opciones genéricas para redes sociales / otros
        if SOCIAL_USERNAME and SOCIAL_PASSWORD:
            opts["username"] = SOCIAL_USERNAME
            opts["password"] = SOCIAL_PASSWORD
        opts["format"] = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"

    # Cookies globales (útil para YouTube Premium, plataformas con sesión)
    if os.path.exists(YOUTUBE_COOKIES):
        opts["cookiefile"] = YOUTUBE_COOKIES

    # Archivos de cookies específicos por plataforma (si existen)
    # Convención: cookies_netflix.txt, cookies_disney.txt, etc.
    if platform_info:
        plat_key = next((k for k, v in STREAMING_PLATFORMS.items() if v is platform_info), "")
        specific_cookie = f"cookies_{plat_key}.txt"
        if os.path.exists(specific_cookie):
            opts["cookiefile"] = specific_cookie

    return opts


# ─── NÚCLEO DE DESCARGA ──────────────────────────────────────────────────────
async def procesar_descarga(client: Client, message: Message, url: str, uname: str, uid: int, queue_label: str):

    mirrors = {
        "flashwish.com": "streamwish.com", "callistanise.com": "vidhide.com",
        "swishdesu.com": "streamwish.com", "filelions.com": "streamwish.com",
        "filelions.to": "streamwish.com", "vidhidepro.com": "vidhide.com",
        "vidhideplus.com": "vidhide.com",
    }
    for mirror, main in mirrors.items():
        if mirror in url.lower():
            url = url.replace(mirror, main)
            break

    VIDEO_HOSTS = [
        "streamwish", "voe", "vidhide", "filemoon", "mixdrop",
        "mp4upload", "streamtape", "flashwish", "callistanise", "filelions", "swishdesu"
    ]
    IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif")

    # ── Detección de tipo de URL ──────────────────────────────────────────────
    is_mega        = "mega.nz" in url
    is_mf          = "mediafire.com" in url
    is_tiktok      = "tiktok.com" in url.lower()
    is_video_host  = any(h in url.lower() for h in VIDEO_HOSTS)
    is_direct_img  = any(url.lower().split("?")[0].endswith(ext) for ext in IMAGE_EXTS)

    # Detectar plataforma de streaming antes de marcar como social
    plat_key, platform_info = detect_streaming_platform(url)
    is_streaming   = platform_info is not None

    is_spotify     = is_streaming and platform_info.get("use_spotdl", False)

    is_social      = (
        url.startswith("http") and
        not any([is_mega, is_mf, is_video_host, is_direct_img, is_tiktok, is_streaming])
    )

    task_id = f"{uid}_{int(time.time())}"
    active_tasks[task_id] = "RUNNING"
    _current = asyncio.current_task()
    if _current: _task_handles[task_id] = _current
    _stop_evt = threading.Event()
    _ydl_stop[task_id] = _stop_evt

    # Panel inicial
    plat_name = platform_info["name"] if platform_info else "Analizando"
    plat_icon = platform_info["icon"] if platform_info else "⬇️"
    msg = await message.reply_text(
        f"╭ Task By → 「{uname}」\n"
        f"┊ [{make_bar(0)}] 0.00%\n"
        f"┊ Status   : {plat_icon} {plat_name}...\n"
        f"╰ Queue    : {queue_label}\n\n"
        f"{BOT_SIGNATURE}"
    )

    path = None
    encoded_path = None
    video_title = ""

    try:
        # ── 1. MEGA ──────────────────────────────────────────────────────────
        if is_mega:
            engine, mode, start_t = "direct", "#MEGA", time.time()
            await safe_edit(msg, f"╭ Task By → 「{uname}」\n┊ [{make_bar(0)}] 0.00%\n┊ Status   : Connecting to MEGA...\n╰ Mode     : {mode}\n\n{BOT_SIGNATURE}")
            async def _mega_progress(curr, total): await download_progress(curr, total, msg, start_t, uname, task_id, engine, mode)
            path, video_title = await mega_download(url, DOWNLOAD_DIR, task_id, progress_cb=_mega_progress)

        # ── 2. MEDIAFIRE ─────────────────────────────────────────────────────
        elif is_mf:
            engine, mode = "httpx", "#MediaFire"
            async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as h:
                r = await h.get(url)
                soup = BeautifulSoup(r.text, "html.parser")
                btn = soup.find("a", {"id": "downloadButton"})
                if not btn: raise Exception("Mediafire: botón de descarga no encontrado.")
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
                            await asyncio.sleep(0)
                            if active_tasks.get(task_id) == "CANCELLED": raise asyncio.CancelledError("USER_CANCELLED")
                            f.write(chunk)
                            curr += len(chunk)
                            await download_progress(curr, total, msg, start_t, uname, task_id, engine, mode)

        # ── 3. IMÁGENES DIRECTAS ─────────────────────────────────────────────
        elif is_direct_img:
            engine, mode = "httpx", "#Image"
            filename = url.split("/")[-1].split("?")[0]
            if not filename or "." not in filename: filename = "image.jpg"
            path = os.path.join(DOWNLOAD_DIR, f"{task_id}_{filename}")
            video_title = filename
            start_t = time.time()
            async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as h:
                async with h.stream("GET", url) as resp:
                    total = int(resp.headers.get("content-length", 0))
                    with open(path, "wb") as f:
                        curr = 0
                        async for chunk in resp.aiter_bytes(chunk_size=1024*1024):
                            await asyncio.sleep(0)
                            if active_tasks.get(task_id) == "CANCELLED": raise asyncio.CancelledError("USER_CANCELLED")
                            f.write(chunk)
                            curr += len(chunk)
                            await download_progress(curr, total, msg, start_t, uname, task_id, engine, mode)

        # ── 4. TIKTOK ────────────────────────────────────────────────────────
        elif is_tiktok:
            engine, mode, start_t = "TikWM API", "#TikTok", time.time()
            await safe_edit(msg, f"╭ Task By → 「{uname}」\n┊ [{make_bar(0)}] 0.00%\n┊ Status   : Conectando a TikTok API...\n╰ Mode     : {mode}\n\n{BOT_SIGNATURE}")
            api_req = f"https://www.tikwm.com/api/?url={url}&hd=1"
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as h:
                tk_data = (await h.get(api_req)).json()
            if tk_data.get("code") == 0:
                data_obj = tk_data.get("data", {})
                video_title = data_obj.get("title", "TikTok")
                images = data_obj.get("images")
                video_url = data_obj.get("hdplay") or data_obj.get("play")
                if images and isinstance(images, list):
                    await safe_edit(msg, f"╭ Task By → 「{uname}」\n┊ Status   : Descargando álbum de fotos...\n╰ Mode     : {mode}\n\n{BOT_SIGNATURE}")
                    files = []
                    for idx, img_url in enumerate(images):
                        if active_tasks.get(task_id) == "CANCELLED": raise asyncio.CancelledError("USER_CANCELLED")
                        img_path = os.path.join(DOWNLOAD_DIR, f"{task_id}_{idx}.jpg")
                        async with httpx.AsyncClient() as h:
                            with open(img_path, "wb") as f: f.write((await h.get(img_url)).content)
                        files.append(img_path)
                    from pyrogram.types import InputMediaPhoto
                    album_caption = f"🖼️ <b>{video_title}</b>\n\n{BOT_SIGNATURE}" if video_title else BOT_SIGNATURE
                    group = []
                    for idx, f in enumerate(files):
                        cap = album_caption if idx == 0 else None
                        group.append(InputMediaPhoto(f, caption=cap, parse_mode=enums.ParseMode.HTML))
                    if group:
                        for i in range(0, len(group), 10): await client.send_media_group(message.chat.id, group[i:i+10])
                    await msg.delete()
                    for f in files:
                        try: os.remove(f)
                        except: pass
                    return
                elif video_url:
                    path = os.path.join(DOWNLOAD_DIR, f"{task_id}.mp4")
                    async with httpx.AsyncClient(timeout=120.0) as h:
                        async with h.stream("GET", video_url) as r:
                            total = int(r.headers.get("content-length", 0))
                            with open(path, "wb") as f:
                                curr = 0
                                async for chunk in r.aiter_bytes(chunk_size=1024*1024):
                                    if active_tasks.get(task_id) == "CANCELLED": raise asyncio.CancelledError("USER_CANCELLED")
                                    f.write(chunk)
                                    curr += len(chunk)
                                    await download_progress(curr, total, msg, start_t, uname, task_id, engine, mode)
                else: raise Exception("La API de TikTok no devolvió contenido multimedia.")
            else: raise Exception("Fallo en la extracción del enlace de TikTok.")

        # ── 5. SPOTIFY (spotDL) ──────────────────────────────────────────────
        elif is_spotify:
            files, video_title = await spotify_download(url, DOWNLOAD_DIR, task_id, msg, uname)
            if not files:
                raise Exception("spotDL no encontró ningún archivo descargado.")
            if len(files) > 1:
                await safe_edit(msg, f"╭ Task By → 「{uname}」\n┊ Status   : Subiendo {len(files)} canciones...\n╰ Mode     : #Spotify\n\n{BOT_SIGNATURE}")
                from pyrogram.types import InputMediaAudio
                album_caption = f"🎵 <b>{video_title}</b>\n\n{BOT_SIGNATURE}" if video_title else BOT_SIGNATURE
                group = []
                for idx, f in enumerate(files):
                    cap = album_caption if idx == 0 else None
                    group.append(InputMediaAudio(f, caption=cap, parse_mode=enums.ParseMode.HTML if cap else None))
                for i in range(0, len(group), 10):
                    await client.send_media_group(message.chat.id, group[i:i+10])
                await msg.delete()
                for f in files:
                    try: os.remove(f)
                    except: pass
                return
            else:
                path = files[0]

        # ── 6. STREAMING + REDES SOCIALES + VIDEO HOSTS (yt-dlp unificado) ──
        elif is_streaming or is_social or is_video_host:
            engine = "yt-dlp"
            if is_streaming:
                mode = platform_info["mode"]
            elif is_video_host:
                mode = "#VideoHoster"
            else:
                mode = "#SocialMedia"

            start_t = time.time()
            loop = asyncio.get_running_loop()
            captured = {"title": ""}

            plat_label = platform_info["name"] if platform_info else "..."
            await safe_edit(msg, (
                f"╭ Task By → 「{uname}」\n"
                f"┊ [{make_bar(0)}] 0.00%\n"
                f"┊ Status   : Extrayendo desde {plat_label}...\n"
                f"╰ Mode     : {mode}\n\n"
                f"{BOT_SIGNATURE}"
            ))

            def run_ydl():
                opts = build_ydl_opts(
                    task_id, url, loop, msg, start_t, uname,
                    engine, mode, platform_info, _stop_evt
                )

                def _extract(o):
                    with yt_dlp.YoutubeDL(o) as ydl:
                        info = ydl.extract_info(url, download=True)
                        if info:
                            captured["title"] = (
                                info.get("title", "") or
                                info.get("webpage_url_basename", "")
                            )

                try:
                    _extract(opts)
                except Exception as _e:
                    err_str = str(_e)
                    if "USER_CANCELLED" in err_str or isinstance(_e, ValueError):
                        raise asyncio.CancelledError("USER_CANCELLED")
                    # Fallback: quitar formato específico y reintentar
                    fallback = dict(opts)
                    fallback.pop("format", None)
                    fallback.pop("allow_unplayable_formats", None)
                    _extract(fallback)

            await asyncio.to_thread(run_ydl)
            video_title = captured["title"]

            files = sorted(glob.glob(f"{DOWNLOAD_DIR}{task_id}_*"), key=os.path.getsize)

            # Fallback universal (og:image para páginas sin video extraíble)
            if not files:
                await safe_edit(msg, f"╭ Task By → 「{uname}」\n┊ Status   : Buscando Imagen de Respaldo...\n╰ Mode     : #UniversalFallback\n\n{BOT_SIGNATURE}")
                try:
                    headers = {"User-Agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"}
                    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as h:
                        resp = await h.get(url, headers=headers)
                        soup = BeautifulSoup(resp.text, "html.parser")
                        meta_img = soup.find("meta", property="og:image") or soup.find("meta", attrs={"name": "twitter:image"})
                        if meta_img and meta_img.get("content"):
                            img_url = meta_img["content"]
                            if "pinimg.com" in img_url: img_url = re.sub(r'/(236x|474x|736x)/', '/originals/', img_url)
                            img_path = os.path.join(DOWNLOAD_DIR, f"{task_id}_fallback.jpg")
                            r = await h.get(img_url, headers=headers)
                            with open(img_path, "wb") as f: f.write(r.content)
                            files = [img_path]
                            if soup.title: video_title = soup.title.string.strip()
                except Exception: pass

            if not files:
                raise Exception(
                    "No se pudo descargar. Posible causa:\n"
                    "• Enlace privado o de pago (necesitas cuenta Premium)\n"
                    "• DRM activo (Widevine L1 requerido)\n"
                    "• IP bloqueada por la plataforma\n"
                    "• Formato no soportado por yt-dlp"
                )

            if len(files) > 1:
                await safe_edit(msg, f"╭ Task By → 「{uname}」\n┊ Status   : Subiendo álbum ({len(files)} archivos)...\n╰ Mode     : {mode}\n\n{BOT_SIGNATURE}")
                from pyrogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument
                album_caption = f"🎬 <b>{video_title}</b>\n\n{BOT_SIGNATURE}" if video_title else BOT_SIGNATURE
                group = []
                for idx, f in enumerate(files):
                    fl = f.lower()
                    cap = album_caption if idx == 0 else None
                    parse = enums.ParseMode.HTML if cap else None
                    if fl.endswith((".jpg", ".jpeg", ".png", ".webp", ".bmp")): group.append(InputMediaPhoto(f, caption=cap, parse_mode=parse))
                    elif fl.endswith((".mp4", ".mkv", ".webm", ".avi", ".mov")): group.append(InputMediaVideo(f, caption=cap, parse_mode=parse, supports_streaming=True))
                    elif fl.endswith(".gif"): group.append(InputMediaDocument(f, caption=cap, parse_mode=parse))
                if group:
                    for i in range(0, len(group), 10): await client.send_media_group(message.chat.id, group[i:i+10])
                await msg.delete()
                for f in files: os.remove(f)
                return
            else:
                path = files[0]

        else:
            raise Exception("Enlace no soportado por el sistema.")

        # ── CODIFICACIÓN DE VIDEO ─────────────────────────────────────────────
        encoded_path = None
        if path and os.path.exists(path):
            lower_path = path.lower()
            if lower_path.endswith((".mp4", ".mkv", ".webm", ".avi", ".mov")):
                info = await asyncio.to_thread(probe_video, path)
                is_h264 = info["codec"] == "h264"
                is_aac  = info["audio_codec"] in ("aac", "mp3", "mp4a")
                if not (is_h264 and is_aac and lower_path.endswith(".mp4")):
                    encoded_path = path + "_out.mp4"
                    await safe_edit(msg, encoding_panel(uname, 0, 0, os.path.getsize(path), 0, 0, 0, task_id))
                    if await encode_video(path, encoded_path, msg, uname, task_id) and os.path.exists(encoded_path):
                        os.remove(path)
                        path = encoded_path
                    else:
                        if encoded_path and os.path.exists(encoded_path): os.remove(encoded_path)
                        encoded_path = None

        # ── SUBIDA A TELEGRAM ─────────────────────────────────────────────────
        if path and os.path.exists(path):
            start_up = time.time()
            await safe_edit(msg, upload_panel(uname, 0, 0, os.path.getsize(path), 0, 0, 0, task_id))
            await upload_smart_file(client, message, path, msg, uname, task_id, title=video_title)
            try: await msg.delete()
            except Exception: pass

    except (Exception, asyncio.CancelledError) as e:
        is_cancel = isinstance(e, asyncio.CancelledError) or "USER_CANCELLED" in str(e)
        err = "🛑 Tarea cancelada exitosamente." if is_cancel else f"❌ Error: {str(e)[:300]}"
        try: await safe_edit(msg, f"╭ Task By → 「{uname}」\n┊ {err}\n╰──────────────\n\n{BOT_SIGNATURE}")
        except Exception: pass

    finally:
        active_tasks.pop(task_id, None); _task_handles.pop(task_id, None); _ydl_stop.pop(task_id, None)
        keys_to_delete = [k for k in last_updates.keys() if k.startswith(task_id)]
        for k in keys_to_delete: last_updates.pop(k, None)
        for f in glob.glob(f"{DOWNLOAD_DIR}{task_id}_*"):
            try: os.remove(f)
            except: pass
        for p in [path, encoded_path]:
            if p and os.path.exists(p):
                try: os.remove(p)
                except: pass
        gc.collect()


# ─── COLA DE TRABAJO ─────────────────────────────────────────────────────────
async def queue_worker():
    while True:
        client, message, url, uname, uid, label = await download_queue.get()
        try:
            await asyncio.create_task(procesar_descarga(client, message, url, uname, uid, label))
        except Exception as e:
            print(f"[worker] error: {e}")
        finally:
            download_queue.task_done()


# ─── INICIALIZACIÓN DEL BOT ───────────────────────────────────────────────────
bot = Client("bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workdir="/tmp")


# ─── COMANDOS ────────────────────────────────────────────────────────────────
@bot.on_message(filters.command("coms"))
async def cmd_coms(client: Client, message: Message):
    if not is_auth(message.from_user.id): return
    text = (
        "📋 Comandos disponibles:\n\n"
        "🔓 Para todos los usuarios autorizados:\n"
        "• Envía un link de cualquier plataforma soportada\n"
        "• /start — Iniciar el bot\n"
        "• /platforms — Ver plataformas soportadas\n"
        "• /cancel — Cancelar descarga activa\n\n"
        "🔐 Solo administradores:\n"
        "• /id  — Autorizar a un usuario\n"
        "• /users — Ver usuarios autorizados\n"
        "• /stat — Ver estadísticas del servidor\n"
        "• /reset — Reiniciar el bot\n"
        "• /admin — Dar rango admin (responde un mensaje)\n"
        "• /remadmin — Quitar rango admin (responde un mensaje)\n"
        "• /cancelarID — Quitar autorización (responde un mensaje)\n"
        "• /coms — Ver esta lista de comandos\n\n"
        f"{BOT_SIGNATURE}"
    )
    await message.reply_text(text)

@bot.on_message(filters.command("platforms"))
async def cmd_platforms(client: Client, message: Message):
    if not is_auth(message.from_user.id): return
    lines = ["🌐 Plataformas soportadas:\n"]
    for key, p in STREAMING_PLATFORMS.items():
        cred_status = "🔑 Con cuenta" if (p.get("email") or p.get("use_spotdl")) else "🌍 Libre"
        lines.append(f"{p['icon']} {p['name']} — {cred_status}")
    lines += [
        "\n📦 También soportado:",
        "• Mega.nz, Mediafire",
        "• TikTok, Instagram, Twitter/X",
        "• Facebook, Pinterest",
        "• Hosts de video (StreamWish, VidHide, etc.)",
        "• Imágenes directas (jpg, png, webp...)",
        f"\n{BOT_SIGNATURE}"
    ]
    await message.reply_text("\n".join(lines))

@bot.on_message(filters.command("start"))
async def cmd_start(client: Client, message: Message):
    uid = message.from_user.id
    name = message.from_user.first_name
    auth_status = "✅ Autorizado" if is_auth(uid) else "⛔ No Autorizado"
    await message.reply_text(
        f"🚀 ¡Hola, {name}!\n\n"
        f"Soy un bot descargador de medios privado.\n"
        f"Soporto YouTube, Netflix, Disney+, Prime Video, HBO Max,\n"
        f"Crunchyroll, Apple TV+, Paramount+, Spotify, TikTok,\n"
        f"Instagram, Twitter/X, Facebook, Mega.nz, Mediafire y más.\n\n"
        f"🆔 **Tu ID:** `{uid}`\n"
        f"🔐 **Estado:** {auth_status}\n\n"
        f"Usa /platforms para ver todas las plataformas disponibles.\n\n"
        f"{BOT_SIGNATURE}"
    )

@bot.on_message(filters.command(["stat", "Stat", "STAT"]))
async def cmd_stat(client: Client, message: Message):
    if not is_auth(message.from_user.id): return
    uptime  = get_readable_time(time.time() - start_time)
    ram     = psutil.virtual_memory()
    disk    = psutil.disk_usage("/tmp")
    cpu     = psutil.cpu_percent(interval=0.5)
    server  = platform.node()
    plat_os = platform.system() + " " + platform.release()
    cpu_info = platform.processor() or platform.machine()
    await message.reply_text(
        f"╭─ Status Panel\n"
        f"┊ 🕐 Time on  : {uptime}\n"
        f"┊ 🧠 RAM      : {ram.percent:.1f}% ({get_readable_size(ram.used)}/{get_readable_size(ram.total)})\n"
        f"┊ 💾 Storage  : {get_readable_size(disk.used)}/{get_readable_size(disk.total)}\n"
        f"┊ 🖥️ Server   : {server}\n"
        f"┊ ⚙️ Platform : {plat_os}\n"
        f"┊ 🔧 CPU      : {cpu}% — {cpu_info}\n"
        f"╰─ Engine    : yt-dlp / Pyrogram / spotDL\n\n"
        f"{BOT_SIGNATURE}"
    )

@bot.on_message(filters.regex(r"^/cancel_([0-9]+_[0-9]+)"))
async def cmd_cancel_id(client: Client, message: Message):
    if not is_auth(message.from_user.id): return
    task_id = message.matches[0].group(1)
    if task_id in active_tasks:
        active_tasks[task_id] = "CANCELLED"
        if task_id in _ydl_stop: _ydl_stop[task_id].set()
        if _task_handles.get(task_id) and not _task_handles[task_id].done(): _task_handles[task_id].cancel()
        await message.reply_text("🛑 Deteniendo proceso de inmediato...")
    else:
        await message.reply_text("⚠️ No hay ninguna tarea activa con ese ID.")

@bot.on_message(filters.command(["cancel", "cancelar"]))
async def cmd_cancel_global(client: Client, message: Message):
    uid = message.from_user.id
    if not is_auth(uid): return
    canceled_any = False
    for tid in list(active_tasks.keys()):
        if tid.startswith(f"{uid}_"):
            active_tasks[tid] = "CANCELLED"
            if tid in _ydl_stop: _ydl_stop[tid].set()
            if _task_handles.get(tid) and not _task_handles[tid].done(): _task_handles[tid].cancel()
            canceled_any = True
    if canceled_any:
        await message.reply_text("🛑 Todas tus descargas han sido canceladas.")
    else:
        await message.reply_text("⚠️ No tienes tareas activas en este momento.")

@bot.on_message(filters.command("id") & filters.reply)
async def cmd_auth_user(client: Client, message: Message):
    if not is_admin(message.from_user.id): return
    target = message.reply_to_message.from_user
    if target:
        authorized_users[str(target.id)] = {"role": "user", "username": target.username or "", "name": target.first_name or ""}
        save_auth_users()
        await message.reply_text(f"✅ **Acceso concedido.**\nEl usuario [{target.first_name}](tg://user?id={target.id}) (`{target.id}`) ahora puede usar el bot.")

@bot.on_message(filters.command(["cancelarID", "cancelarid", "Removeid", "removeid"]) & filters.reply)
async def cmd_remove_auth(client: Client, message: Message):
    if not is_admin(message.from_user.id): return
    target = message.reply_to_message.from_user
    if target and str(target.id) in authorized_users:
        del authorized_users[str(target.id)]
        save_auth_users()
        await message.reply_text(f"❌ **Acceso revocado.**\nEl usuario [{target.first_name}](tg://user?id={target.id}) ya no tiene permiso.")

@bot.on_message(filters.command("admin") & filters.reply)
async def cmd_set_admin(client: Client, message: Message):
    if not is_admin(message.from_user.id): return
    target = message.reply_to_message.from_user
    if target:
        authorized_users[str(target.id)] = {"role": "admin", "username": target.username or "", "name": target.first_name or ""}
        save_auth_users()
        await message.reply_text(f"🛡 **Rango Admin otorgado** a [{target.first_name}](tg://user?id={target.id}).")

@bot.on_message(filters.command("remadmin") & filters.reply)
async def cmd_rem_admin(client: Client, message: Message):
    if not is_admin(message.from_user.id): return
    target = message.reply_to_message.from_user
    if target and str(target.id) in authorized_users:
        authorized_users[str(target.id)]["role"] = "user"
        save_auth_users()
        await message.reply_text(f"👤 **Rango Admin retirado** a [{target.first_name}](tg://user?id={target.id}).")

@bot.on_message(filters.command("users"))
async def cmd_users(client: Client, message: Message):
    if not is_admin(message.from_user.id): return
    text = "╭─ Usuarios autorizados\n┊ 👑 Owners : @The_canst, @Ryota_YT\n"
    count = 1
    for str_uid, info in authorized_users.items():
        role_icon = " 🛡 admin" if info.get("role") == "admin" else ""
        username = info.get("username")
        display_name = f"@{username}" if username else f"id:{str_uid}"
        text += f"┊ {count}. {display_name} ({str_uid}){role_icon}\n"
        count += 1
    text += f"╰─ Total : {count - 1}\n\n{BOT_SIGNATURE}"
    await message.reply_text(text)

@bot.on_message(filters.command(["reset", "Reset", "RESET"]))
async def cmd_reset(client: Client, message: Message):
    if not is_admin(message.from_user.id): return
    msg = await message.reply_text("♻️ Reiniciando...")
    cancelled = len(active_tasks)
    for tid in list(active_tasks.keys()): active_tasks[tid] = "CANCELLED"
    freed = 0
    for f in glob.glob(f"{DOWNLOAD_DIR}*"):
        try:
            size = os.path.getsize(f)
            os.remove(f)
            freed += size
        except Exception: pass
    gc.collect()
    await msg.edit_text(
        f"╭─「 Reset Completado ✅ 」\n"
        f"┊ 🔄 Estado    : Online\n"
        f"┊ 🕐 Uptime    : 0s\n"
        f"┊ 🧹 Liberado  : {get_readable_size(freed)}\n"
        f"┊ ⛔ Descargas : {'Canceladas' if cancelled > 0 else 'Ninguna activa'}\n"
        f"╰─ Engine      : CRDWV2\n\n"
        f"{BOT_SIGNATURE}"
    )


# ─── RECEPTOR DE ENLACES PRINCIPAL ───────────────────────────────────────────
EXCLUDED_COMMANDS = [
    "start", "stat", "Stat", "STAT", "reset", "Reset", "RESET",
    "id", "Removeid", "removeid", "cancelarID", "cancelarid",
    "coms", "cancel", "cancelar", "admin", "remadmin", "users", "platforms"
]

@bot.on_message(
    filters.text &
    ~filters.command(EXCLUDED_COMMANDS) &
    ~filters.regex(r"^/cancel_")
)
async def handle_links(client: Client, message: Message):
    uid = message.from_user.id
    if not is_auth(uid):
        await message.reply_text("⛔ **Acceso denegado.**\nNo estás autorizado para usar este bot.")
        return
    urls = re.findall(r"https?://[^\s]+", message.text.strip())
    if not urls: return
    uname = message.from_user.first_name
    for i, url in enumerate(urls, 1):
        label = f"Cola: {i}/{len(urls)}"
        await download_queue.put((client, message, url, uname, uid, label))
    q = download_queue.qsize()
    if len(urls) == 1:
        await message.reply_text(f"📥 Enlace añadido a la cola.\n🚦 Tareas en espera: {q}\n\n{BOT_SIGNATURE}")
    else:
        await message.reply_text(f"📥 {len(urls)} enlaces añadidos a la cola.\n🚦 Total en espera: {q}\n\n{BOT_SIGNATURE}")


# ─── BUCLE PRINCIPAL ─────────────────────────────────────────────────────────
async def main():
    async with bot:
        me = await bot.get_me()
        print(f"Bot iniciado ✓ — @{me.username} (ID: {me.id})")
        asyncio.get_event_loop().create_task(queue_worker())
        await asyncio.Event().wait()

if __name__ == "__main__":
    bot.run(main())
