"""
بوت تحميل فيديوهات متطور — أزرار ملونة + دعم واسع للمواقع
المحركات: yt-dlp (1700+ موقع) ← تقليد متصفح للمواقع المحمية ← gallery-dl (صور/ألبومات) ← كاشف فيديو في أي صفحة
"""
import os, re, time, uuid, shutil, asyncio, tempfile, sqlite3, threading, logging, json, base64
from concurrent.futures import ThreadPoolExecutor
import socket, ipaddress, hashlib, subprocess, urllib.request, urllib.error, copy, inspect
from collections import Counter
from dataclasses import dataclass, field
from urllib.parse import urlparse, urljoin

import yt_dlp
from yt_dlp.extractor import list_extractor_classes
from pyrogram import Client, filters, idle
from pyrogram.errors import MessageNotModified, FloodWait
from pyrogram.enums import MessageEntityType
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton, InputMediaPhoto

try:
    from pyrogram.enums import ButtonStyle as S
    COLORS = True
except ImportError:
    COLORS = False
    class S: PRIMARY = SUCCESS = DANGER = None

try:
    import gallery_dl.extractor as gdx
    HAS_GALLERY = shutil.which("gallery-dl") is not None
except ImportError:
    gdx, HAS_GALLERY = None, False

IMPERSONATE = None   # تقليد متصفح Chrome لتخطي حماية Cloudflare وأخواتها
try:
    import curl_cffi  # noqa: F401
    from yt_dlp.networking.impersonate import ImpersonateTarget
    IMPERSONATE = ImpersonateTarget.from_str("chrome")
except Exception:
    pass

# yt-dlp بيحاول يكتب الكوكيز تاني في الملف عند كل تحميل؛ مع تحميلات متزامنة ده بيبوّظ الملف، فنمنع الكتابة.
try:
    from yt_dlp.cookies import YoutubeDLCookieJar as _CJ
    _CJ.save = lambda self, *a, **k: None
except Exception:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("vidbot")
PROCESS_STARTED_AT = time.monotonic()

try:    # event loop أسرع (لازم يتفعّل قبل إنشاء الـ Client)
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    HAS_UVLOOP = True
except Exception:
    HAS_UVLOOP = False

# ───────────── الإعدادات ─────────────
_missing = [k for k in ("API_ID", "API_HASH", "BOT_TOKEN") if not os.environ.get(k, "").strip()]
if _missing:
    raise SystemExit("❌ متغيرات ناقصة: " + ", ".join(_missing) + " — ضيفها في Variables وأعد النشر.")
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
# مفيش حدود تطبيقية للتحميل: الدفعات والقوائم والروابط والتزامن بلا سقف.
MAX_PLAYLIST = MAX_GALLERY = 0
TRANSFER_PROFILE = os.getenv("TRANSFER_PROFILE", "fast").strip().lower()
CONN = int(os.getenv("CONNECTIONS", "24"))
FRAGMENTS = int(os.getenv("FRAGMENTS", "24"))
UPLOAD_WORKERS = int(os.getenv("UPLOAD_WORKERS", "16"))
if TRANSFER_PROFILE == "fast":
    # يضمن تطبيق السرعة الجديدة حتى لو كانت متغيرات Railway القديمة أقل.
    CONN, FRAGMENTS, UPLOAD_WORKERS = max(CONN, 24), max(FRAGMENTS, 24), max(UPLOAD_WORKERS, 16)
# البوت عام افتراضيًا؛ لا حصة أو حد لطول الفيديو المسجل.
MIN_FREE_MB = 0
ALLOWED_IDS = {int(x) for x in os.getenv("ALLOWED_USERS", "").split(",") if x.strip()}
MAX_SIZE = 2000 * 1024 * 1024
DB_PATH = os.getenv("DB_PATH") or ("/data/bot.db" if os.path.isdir("/data") else "bot.db")
# مواقع محجوبة (محتوى مخالف لقواعد تليجرام). القايمة الأساسية ثابتة في الكود،
# ومتغير BLOCKED_DOMAINS (فاصلة) بيضيف عليها بس — مينفعش يتلغي الحجب بمتغير.
_DEFAULT_BLOCK = ("pornhub.com,xvideos.com,xnxx.com,xhamster.com,redtube.com,youporn.com,"
                  "spankbang.com,tube8.com,eporner.com,motherless.com")
BLOCKED = tuple(dict.fromkeys(
    x.strip().lower() for x in (_DEFAULT_BLOCK + "," + os.getenv("BLOCKED_DOMAINS", "")).split(",") if x.strip()))
VALID_PREFS = {"ask", "best", "1080", "720", "480", "360", "audio"}
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0.0.0 Safari/537.36")

HAS_ARIA2 = shutil.which("aria2c") is not None
SITE_COUNT = sum(1 for _ in list_extractor_classes())

def tune_upload(workers):
    """يرفع عدد طلبات الرفع المتوازية للملف الكبير (المكتبة ثابتة على 4) — أكبر تأثير على سرعة الرفع."""
    try:
        import pyrogram.methods.advanced.save_file as sf
        src, old = inspect.getsource(sf), "workers_count = 4 if is_big else 1"
        if old not in src:
            log.warning("upload tuning skipped: pattern not found (library version changed)")
            return False
        ns = {"__name__": sf.__name__ + "_tuned", "__file__": sf.__file__}
        exec(compile(src.replace(old, f"workers_count = {int(workers)} if is_big else 1"), sf.__file__, "exec"), ns)
        Client.save_file = ns["SaveFile"].save_file
        return True
    except Exception as e:
        log.warning("upload tuning failed: %s", e)
        return False

UPLOAD_TUNED = tune_upload(UPLOAD_WORKERS) if UPLOAD_WORKERS > 4 else False

class UnlimitedSemaphore:
    """واجهة semaphore بلا سقف؛ تمرير acquire لا ينتظر ولا يحجز موردًا."""
    async def acquire(self): return True
    def release(self): pass
    def locked(self): return False
    async def __aenter__(self): return self
    async def __aexit__(self, exc_type, exc, tb): return False

UNLIMITED = UnlimitedSemaphore()

# نستبدل semaphores الداخلية للمكتبة أيضًا حتى لا يبقى سقف تزامن مخفي.
app = Client("vidbot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
             in_memory=True, workers=32, max_concurrent_transmissions=1)
app.save_file_semaphore = UNLIMITED
app.get_file_semaphore = UNLIMITED
DL_SEM = UP_SEM = PROBE_SEM = UNLIMITED

def user_sem(uid):
    return UNLIMITED
URL_RE = re.compile(r"https?://[^\s<>\"']+")
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

# ───────────── قاعدة البيانات ─────────────
try: os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
except OSError as _e: log.warning("cannot create DB dir: %s", _e)
COOKIE_FILE = os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "cookies.txt")
PROXY = os.getenv("PROXY", "").strip()

def has_cookies():
    try: return os.path.getsize(COOKIE_FILE) > 50
    except OSError: return False

def cookies_info(path):
    """(إجمالي الكوكيز, كوكيز يوتيوب/جوجل, فيها جلسة دخول؟)"""
    from yt_dlp.cookies import YoutubeDLCookieJar
    jar = YoutubeDLCookieJar(path)
    jar.load(ignore_discard=True, ignore_expires=True)
    total = yt = 0; login = False
    for c in jar:
        total += 1
        if "youtube.com" in c.domain or "google.com" in c.domain:
            yt += 1
            if c.name in ("SID", "__Secure-1PSID", "__Secure-3PSID", "LOGIN_INFO", "SAPISID"): login = True
    return total, yt, login

def save_cookies_text(raw):
    """بيتحقق من ملف الكوكيز (Netscape) ويحفظه. بيرجّع (نجاح, رسالة)."""
    raw = (raw or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not raw: return False, "الملف فاضي"
    if not raw.lstrip().startswith("# Netscape") and not raw.lstrip().startswith("# HTTP Cookie File"):
        raw = "# Netscape HTTP Cookie File\n" + raw
    tmp = COOKIE_FILE + ".new"
    try:
        with open(tmp, "w", encoding="utf-8") as f: f.write(raw + "\n")
        total, yt, login = cookies_info(tmp)
        if total == 0:
            os.remove(tmp); return False, "مقدرتش أقرا أي كوكيز — لازم الملف يكون بصيغة Netscape (cookies.txt)"
        os.replace(tmp, COOKIE_FILE)
        try: os.chmod(COOKIE_FILE, 0o600)
        except OSError: pass
        return True, f"{total} كوكي ({yt} ليوتيوب/جوجل)" + ("" if login else " ⚠️ مفيش جلسة تسجيل دخول جواهم")
    except Exception as e:
        try: os.remove(tmp)
        except OSError: pass
        return False, f"ملف غير صالح: {str(e)[:80]}"

def seed_cookies_from_env():
    """COOKIES_B64 (أو COOKIES_TXT) من Variables بتتكتب مرة لما قيمتها تتغير؛ رفع الأدمن من البوت بيفضل شغال بعدها."""
    try:
        b64, txt = os.getenv("COOKIES_B64", "").strip(), os.getenv("COOKIES_TXT", "")
        data = base64.b64decode(b64).decode("utf-8", "ignore") if b64 else txt.replace("\\n", "\n")
        if not data.strip(): return
        sig = hashlib.sha1(data.encode()).hexdigest()
        marker = COOKIE_FILE + ".envsig"
        if os.path.exists(marker) and open(marker).read().strip() == sig and has_cookies(): return
        ok, msg = save_cookies_text(data)
        log.info("cookies from env: %s", msg if ok else "rejected - " + msg)
        if ok:
            with open(marker, "w") as f: f.write(sig)
    except Exception as e:
        log.warning("cookies env ignored: %s", e)

seed_cookies_from_env()
db = sqlite3.connect(DB_PATH, check_same_thread=False)
db_lock = threading.Lock()
for _pragma in ("PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL"):
    try: db.execute(_pragma)
    except sqlite3.DatabaseError: pass
db.executescript("""
CREATE TABLE IF NOT EXISTS cache(key TEXT, mode TEXT, file_id TEXT, PRIMARY KEY(key, mode));
CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, first_seen INTEGER,
                                 downloads INTEGER DEFAULT 0, quality TEXT DEFAULT 'ask');
CREATE TABLE IF NOT EXISTS history(user_id INTEGER, title TEXT, mode TEXT, ts INTEGER);
CREATE TABLE IF NOT EXISTS banned(id INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS usage(user_id INTEGER, day TEXT, n INTEGER DEFAULT 0, PRIMARY KEY(user_id, day));
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS allowed(id INTEGER PRIMARY KEY);
CREATE TABLE IF NOT EXISTS urlmap(url TEXT PRIMARY KEY, key TEXT, title TEXT, ts INTEGER);
""")
for _alter in ("ALTER TABLE users ADD COLUMN quality TEXT DEFAULT 'ask'",
               "ALTER TABLE cache ADD COLUMN ts INTEGER DEFAULT 0"):
    try:
        db.execute(_alter); db.commit()
    except sqlite3.OperationalError:
        pass

# نسخة في الذاكرة من قوائم الحظر/المسموح (بدل استعلام قاعدة بيانات مع كل رسالة وكل ضغطة زرار)
BANNED = {r[0] for r in db.execute("SELECT id FROM banned")}
ALLOWED_DB = {r[0] for r in db.execute("SELECT id FROM allowed")}

def db_exec(q, args=()):
    with db_lock:
        cur = db.execute(q, args); db.commit(); return cur.fetchall()

# ───────────── إعدادات الأدمن (بتتغير من لوحة الأدمن وبتتخزن في القاعدة) ─────────────
# قيم المتغيرات (Variables) هنا هي القيم الابتدائية بس؛ أي تغيير من اللوحة بيتخزن ويغلب عليها.
SETTING_DEFAULTS = {
    "public": "1",                               # عام افتراضيًا؛ يمكن تغييره يدويًا من لوحة الأدمن
    "wm_on": "0",                               # العلامة المائية شغّالة؟
    "wm_type": "text",                          # text | image
    "wm_text": "", "wm_img_sig": "",
    "wm_pos": "br",                             # tl | tr | bl | br | c
    "wm_size": "M",                             # S | M | L
    "pe_on": "0", "pe_map": "{}",               # إيموجي بريميوم
}
CFG = dict(SETTING_DEFAULTS)
CFG.update({k: v for k, v in db.execute("SELECT key, value FROM settings") if k in SETTING_DEFAULTS})
# إزالة القفل القديم مرة واحدة: يرجع البوت عامًا بعد التحديث، وبعدها يظل زر الأدمن قابلًا للتغيير.
if not db_exec("SELECT value FROM settings WHERE key=?", ("public_access_reset_v1",)):
    CFG["public"] = "1"
    db_exec("INSERT OR REPLACE INTO settings(key, value) VALUES (?,?)", ("public", "1"))
    db_exec("INSERT OR REPLACE INTO settings(key, value) VALUES (?,?)", ("public_access_reset_v1", "done"))

def cfg(key):
    return CFG[key]

def cfg_set(key, val):
    CFG[key] = str(val)
    db_exec("INSERT OR REPLACE INTO settings(key, value) VALUES (?,?)", (key, str(val)))

def cache_get(key, mode):
    r = db_exec("SELECT file_id FROM cache WHERE key=? AND mode=?", (key, mode))
    return r[0][0] if r else None

def cache_put(key, mode, fid):
    db_exec("INSERT OR REPLACE INTO cache(key, mode, file_id, ts) VALUES (?,?,?,?)",
            (key, mode, fid, int(time.time())))

def urlmap_get(url):
    """يعيد مفتاح الكاش وعنوان الرابط السابق إن وُجدا؛ ما زال metadata يُفحص قبل عرض الاختيارات."""
    r = db_exec("SELECT m.key, m.title FROM urlmap m WHERE m.url=? "
                "AND EXISTS (SELECT 1 FROM cache c WHERE c.key = m.key)", (url,))
    return r[0] if r else None

def urlmap_put(url, key, title):
    db_exec("INSERT OR REPLACE INTO urlmap(url, key, title, ts) VALUES (?,?,?,?)",
            (url[:500], key, (title or "")[:100], int(time.time())))

def touch_user(uid):
    db_exec("INSERT OR IGNORE INTO users(id, first_seen) VALUES (?,?)", (uid, int(time.time())))

def record(uid, title, mode):
    db_exec("UPDATE users SET downloads = downloads + 1 WHERE id=?", (uid,))
    db_exec("INSERT INTO history VALUES (?,?,?,?)", (uid, title[:80], mode, int(time.time())))

def clean_stale_tmp(max_age=3600):
    """بقايا تحميلات اتقطعت (Crash / إعادة تشغيل) — بتتمسح عشان القرص ما يمتلاش."""
    tmp, now = tempfile.gettempdir(), time.time()
    for n in os.listdir(tmp):
        if not n.startswith(("dl_", "speed_")): continue
        path = os.path.join(tmp, n)
        try:
            if now - os.path.getmtime(path) > max_age:
                shutil.rmtree(path, ignore_errors=True) if os.path.isdir(path) else os.remove(path)
        except OSError:
            pass

def get_pref(uid):
    r = db_exec("SELECT quality FROM users WHERE id=?", (uid,))
    return (r[0][0] if r and r[0][0] else "ask")

def set_pref(uid, q):
    if q not in VALID_PREFS: return
    db_exec("UPDATE users SET quality=? WHERE id=?", (q, uid))

# ───────────── الموديلات ─────────────
@dataclass
class Item:
    url: str
    title: str
    key: str
    engine: str = "ytdlp"     # ytdlp | gallery
    referer: str = ""
    status: str = "wait"      # wait | dl | up | done | err
    detail: str = ""
    info: object = field(default=None, repr=False)   # نتيجة الفحص (بتتستخدم تاني عشان نوفر استخراج تاني)

@dataclass
class Batch:
    id: str
    user_id: int
    chat_id: int
    items: list
    mode: str = "720"
    cancelled: bool = False
    done: bool = False
    started: bool = False
    started_at: float = 0.0
    msg: object = None
    sem: UnlimitedSemaphore = field(default_factory=lambda: UNLIMITED)

batches: dict[str, Batch] = {}
awaiting: dict[int, str] = {}
bc_pending: dict[int, tuple] = {}

# رسائل أخطاء مفهومة بالعربي
ERR_MAP = [
    (r"sign in to confirm|not a bot", "يوتيوب طلب التحقق إنك مش روبوت (بيحصل مع IP الاستضافات). صاحب البوت لازم يرفع كوكيز يوتيوب جديدة من لوحة الأدمن 🍪 ← كوكيز يوتيوب."),
    (r"private video|video is private|this account is private", "الفيديو/الحساب خاص"),
    (r"confirm your age|age[- ]restricted|inappropriate for some users", "الفيديو مقيّد بالعمر ولا يمكن تحميله في الوضع المجهول"),
    (r"login required|log in|logged in|rate-limit reached|requires? authentication|cookies", "المحتوى يتطلب تسجيل دخول، والتنزيل المجهول غير متاح له"),
    (r"\bdrm\b|widevine", "محمي بـ DRM ومينفعش يتحمل"),
    (r"geo[- ]?restrict|not available in your country|blocked .* in your country", "محجوب في بلد السيرفر"),
    (r"live event|is live|premieres in|will begin in", "ده بث مباشر/لسه ما بدأش — حاول لما يخلص"),
    (r"unsupported url|no video|there is no video|no media|مفيش فيديو", "مفيش فيديو قابل للتحميل في الرابط ده"),
    (r"unavailable|removed|deleted|does not exist|not found|404", "الفيديو مش متاح أو اتحذف"),
    (r"429|too many requests", "الموقع حظر السيرفر مؤقتًا — حاول بعد شوية"),
    (r"timed out|timeout", "الموقع اتأخر في الرد — حاول تاني"),
]

def clean_err(e):
    s = ANSI_RE.sub("", str(e)).replace("ERROR:", "").strip()
    for rx, msg in ERR_MAP:
        if re.search(rx, s, re.I): return msg
    return s[:150]

def mode_label(m):
    return {"best": "أعلى جودة", "audio": "MP3", "ask": "اسألني كل مرة"}.get(m, f"{m}p")

def is_admin(uid):
    return uid in ADMIN_IDS

def is_banned(uid):
    return uid in BANNED

def is_whitelisted(uid):
    return uid in ALLOWED_IDS or uid in ALLOWED_DB

def is_allowed(uid):
    if uid in ADMIN_IDS: return True
    if is_banned(uid): return False
    return cfg("public") == "1" or is_whitelisted(uid)

def is_blocked(url):
    host = (urlparse(url).hostname or "").lower()
    return any(host == d or host.endswith("." + d) for d in BLOCKED)

def is_safe_url(url):
    """يمنع الروابط الداخلية (localhost / الشبكة الخاصة / metadata) عشان السيرفر ما يتستغلش.
    بيتطبق على: رابط المستخدم، وكل رابط بيتلقط من جوه صفحة، وكل Redirect بنتبعه بنفسنا."""
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https") or not p.hostname: return False
        for *_, sa in socket.getaddrinfo(p.hostname, None):
            ip = ipaddress.ip_address(sa[0].split("%")[0])
            if getattr(ip, "ipv4_mapped", None): ip = ip.ipv4_mapped
            if not ip.is_global or ip.is_multicast:      # is_global بتغطي private/loopback/link-local/CGNAT/reserved
                return False
        return True
    except Exception:
        return False

def url_ok(url):
    return is_safe_url(url) and not is_blocked(url)

# ───────────── إيموجي بريميوم ─────────────
PE = {"on": False, "map": {}, "pat": None, "broken": False}

def _norm_emoji(x): return x.replace("\ufe0f", "").strip()

def pe_reload():
    try: m = json.loads(cfg("pe_map") or "{}")
    except ValueError: m = {}
    PE["map"] = {k: str(v) for k, v in m.items() if k}
    PE["on"] = cfg("pe_on") == "1" and bool(PE["map"])
    keys = sorted(PE["map"], key=len, reverse=True)
    PE["pat"] = re.compile("(" + "|".join(re.escape(k) for k in keys) + ")\ufe0f?") if keys else None

def pe_active(): return PE["on"] and not PE["broken"] and PE["pat"] is not None

def pe_html(text):
    if not text or not pe_active(): return text
    return PE["pat"].sub(lambda m: f'<emoji id="{PE["map"][m.group(1)]}">{m.group(1)}</emoji>', text)

def pe_button(text):
    """لو الزرار بيبدأ بإيموجي مربوط ببريميوم: (id, النص من غير الإيموجي)."""
    if not pe_active(): return None, text
    m = PE["pat"].match(text)
    if not m: return None, text
    return PE["map"][m.group(1)], text[m.end():].lstrip()

def plainify_kb(kb):
    try:
        for row in kb.inline_keyboard:
            for btn in row:
                orig = getattr(btn, "pe_orig", None)
                if orig: btn.icon_custom_emoji_id = None; btn.text = orig
    except Exception: pass

# ───────────── الأزرار الملونة ─────────────
def B(text, data, style=S.PRIMARY):
    kw = {}
    if COLORS and style is not None: kw["style"] = style
    icon, label = pe_button(text)
    if icon and label: kw["icon_custom_emoji_id"] = icon
    else: label = text
    btn = InlineKeyboardButton(label, callback_data=data, **kw)
    if icon and label: btn.pe_orig = text
    return btn

def KB(*rows):
    return InlineKeyboardMarkup([list(r) for r in rows])

def menu_kb(uid):
    rows = [
        [B("➕ تحميل فيديو", "dl", S.SUCCESS)],
        [B("📊 تحميلاتي", "me"), B("⚙️ الإعدادات", "st")],
        [B("🌐 المواقع المدعومة", "sites"), B("❓ المساعدة", "hp")],
    ]
    if is_admin(uid):
        rows.append([B("🛠 لوحة الأدمن", "ad", S.DANGER)])
    return KB(*rows)

def back_kb():
    return KB([B("🏠 القائمة الرئيسية", "m")])

def duration_text(value):
    try:
        seconds = max(0, int(float(value)))
    except (TypeError, ValueError, OverflowError):
        return "غير متاح"
    hours, rem = divmod(seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours}:{minutes:02d}:{seconds:02d}" if hours else f"{minutes}:{seconds:02d}"

def _format_size(fmt, duration):
    for key in ("filesize", "filesize_approx"):
        try:
            value = int(float(fmt.get(key) or 0))
            if value > 0: return value
        except (TypeError, ValueError, OverflowError):
            pass
    try:
        bitrate = float(fmt.get("tbr") or fmt.get("abr") or 0)
        seconds = float(duration or 0)
        if bitrate > 0 and seconds > 0:
            return int(bitrate * 1000 * seconds / 8)
    except (TypeError, ValueError, OverflowError):
        pass
    return None

def _format_height(fmt):
    try: return int(fmt.get("height") or 0)
    except (TypeError, ValueError, OverflowError): return 0

def _format_rank(fmt):
    height = _format_height(fmt)
    try: bitrate = float(fmt.get("tbr") or fmt.get("abr") or 0)
    except (TypeError, ValueError): bitrate = 0
    try: fps = float(fmt.get("fps") or 0)
    except (TypeError, ValueError): fps = 0
    h264 = str(fmt.get("vcodec") or "").startswith("avc1")
    return height, h264, bitrate, fps

def quality_modes(info):
    formats = (info.get("formats") or []) if isinstance(info, dict) else []
    heights = []
    for fmt in formats:
        if not isinstance(fmt, dict) or fmt.get("vcodec") in (None, "none"): continue
        height = _format_height(fmt)
        if height > 0: heights.append(height)
    if not heights: return ["best", "1080", "720", "480", "360"]
    modes = ["best"]
    max_height = max(heights)
    modes.extend(str(height) for height in sorted(set(heights), reverse=True) if height > 1080)
    for target in (1080, 720, 480, 360):
        if max_height >= target and any(height <= target for height in heights): modes.append(str(target))
    return modes

def audio_available(info):
    if not isinstance(info, dict) or not info.get("formats"): return True
    return any(isinstance(f, dict) and f.get("acodec") not in (None, "none")
               for f in info.get("formats") or [])

def thumbnail_url(info):
    if not isinstance(info, dict): return None
    url = info.get("thumbnail")
    if isinstance(url, str) and url.startswith("https://"): return url
    thumbnails = [t for t in (info.get("thumbnails") or [])
                  if isinstance(t, dict) and isinstance(t.get("url"), str) and t["url"].startswith("https://")]
    if not thumbnails: return None
    def rank(t):
        try: return int(t.get("width") or 0) * int(t.get("height") or 0)
        except (TypeError, ValueError, OverflowError): return 0
    return max(thumbnails, key=rank)["url"]

def estimated_size(info, mode):
    if not isinstance(info, dict): return None
    duration = info.get("duration")
    if mode == "audio":
        try:
            seconds = float(duration or 0)
            return int(seconds * 192000 / 8) if seconds > 0 else None
        except (TypeError, ValueError, OverflowError):
            return None
    target = None if mode == "best" else int(mode)
    formats = [f for f in (info.get("formats") or []) if isinstance(f, dict)]
    def has_video(f): return f.get("vcodec") not in (None, "none")
    def has_audio(f): return f.get("acodec") not in (None, "none")
    video = [f for f in formats if has_video(f) and f.get("height")]
    if target is not None:
        video = [f for f in video if _format_height(f) <= target]
    if not video: return None
    video.sort(key=_format_rank, reverse=True)
    separate_video = [f for f in video if not has_audio(f)]
    combined = [f for f in video if has_audio(f)]
    if separate_video:
        vfmt = separate_video[0]
        audio = [f for f in formats if has_audio(f) and not has_video(f)]
        audio.sort(key=_format_rank, reverse=True)
        vsize = _format_size(vfmt, duration)
        asize = _format_size(audio[0], duration) if audio else None
        if vsize is not None and asize is not None: return vsize + asize
    if combined:
        combined.sort(key=_format_rank, reverse=True)
        return _format_size(combined[0], duration)
    return None

def quality_button_label(info, mode):
    label = {"best": "🏆 أعلى جودة", "audio": "🎧 MP3", "1080": "🎬 1080p",
             "720": "🎬 720p", "480": "🎬 480p", "360": "🎬 360p"}.get(mode, f"🎬 {mode}p")
    size = estimated_size(info, mode)
    return f"{label} · ~{human_bytes(size)}" if size else label

def picker_kb(bid, info=None):
    modes = quality_modes(info) if info else ["best", "1080", "720", "480", "360"]
    rows = [[B(quality_button_label(info, "best"), f"q|{bid}|best", S.SUCCESS)]]
    video_modes = [m for m in modes if m != "best"]
    for i in range(0, len(video_modes), 2):
        rows.append([B(quality_button_label(info, m), f"q|{bid}|{m}") for m in video_modes[i:i + 2]])
    if audio_available(info): rows.append([B(quality_button_label(info, "audio"), f"q|{bid}|audio")])
    rows.append([B("❌ إلغاء", f"x|{bid}", S.DANGER)])
    return KB(*rows)

def metadata_text(items, playlist_title=None):
    if len(items) != 1 or items[0].engine != "ytdlp" or not items[0].info:
        text = (f"📋 {_escape_metadata_title(playlist_title, 120)}\n" if playlist_title else "") + f"🎬 لقيت {len(items)} عنصر:\n"
        text += "\n".join(f"• {_escape_metadata_title(item.title, 55)}" for item in items[:6])
        if len(items) > 6: text += f"\n... و{len(items) - 6} كمان"
        return text + "\n\nاختار الجودة التي ستُطبق على العناصر:"
    item, info = items[0], items[0].info
    title = _escape_metadata_title(info.get("title") or item.title or "فيديو", 180)
    lines = [f"🎬 {title}", f"⏱️ المدة: {duration_text(info.get('duration'))}"]
    heights = sorted({_format_height(f) for f in (info.get("formats") or [])
                      if isinstance(f, dict) and _format_height(f) and f.get("vcodec") not in (None, "none")}, reverse=True)
    if heights: lines.append("📺 المتاح: " + "، ".join(f"{h}p" for h in heights[:8]))
    lines.append("🎞️ الصيغة المتوقعة للفيديو: MP4")
    if audio_available(info): lines.append("🎧 يوجد خيار صوت MP3")
    lines.append("📦 الحجم المتوقع لكل اختيار يظهر على الزر (تقديري وقد لا يقدمه المصدر).")
    lines.append("🛡️ يبدأ التنزيل بعد اختيارك إذا كان الرابط متاحًا وغير محمي.")
    lines.append("\nاختر الجودة:")
    return "\n".join(lines)

def _escape_metadata_title(value, limit):
    text = " ".join(str(value or "").split())[:limit]
    return re.sub(r"([_*`\[\]])", r"\\\1", text)

def cancel_kb(b):
    return KB([B("⏹ إيقاف التنزيل", f"c|{b.id}", S.DANGER)])

def after_kb():
    return KB([B("➕ تحميل جديد", "dl", S.SUCCESS)], [B("🏠 القائمة الرئيسية", "m")])

def settings_kb(uid):
    cur = get_pref(uid)
    def mk(v, label):
        sel = v == cur
        return B(("✅ " if sel else "") + label, f"st|{v}", S.SUCCESS if sel else S.PRIMARY)
    return KB(
        [mk("ask", "🤔 اسألني كل مرة")],
        [mk("best", "🏆 أعلى جودة")], [mk("1080", "1080p"), mk("720", "720p")],
        [mk("480", "480p"), mk("360", "360p")], [mk("audio", "🎧 MP3")],
        [B("🏠 القائمة الرئيسية", "m")],
    )

def admin_kb():
    return KB(
        [B("📊 إحصائيات", "ad|st"), B("📢 إذاعة", "ad|bc", S.SUCCESS)],
        [B("⚙️ إعدادات البوت", "ad|cf"), B("💧 العلامة المائية", "ad|wm")],
        [B("⚡ اختبار السرعة", "ad|sp", S.SUCCESS)],
        [B("🍪 كوكيز يوتيوب", "ad|ck", S.SUCCESS), B("✨ إيموجي بريميوم", "ad|pe")],
        [B("🗑 مسح الكاش", "ad|cc", S.DANGER)],
        [B("🏠 القائمة الرئيسية", "m")],
    )

# ───────────── yt-dlp (مع تقليد المتصفح) ─────────────
YT_HOSTS = ("youtube.com", "youtu.be", "youtube-nocookie.com")
RETRY_IMP = re.compile(r"403|forbidden|cloudflare|just a moment|captcha|429|impersonat|not a bot|"
                       r"unable to download webpage|unsupported url|timed out|challenge", re.I)
RETRY_YT_ANON = re.compile(r"sign in to confirm|not a bot|login_required|po.?token|http error 403", re.I)
ARIA2_EXIT = re.compile(r"aria2c exited with code\s+(\d+)", re.I)

def is_youtube_url(url):
    host = (urlparse(url).hostname or "").lower()
    return any(host == h or host.endswith("." + h) for h in YT_HOSTS)

def youtube_android_vr_opts(opts):
    """Best-effort anonymous fallback; YouTube may still reject the hosting IP or limit formats."""
    retry = copy.deepcopy(opts)
    args = retry.setdefault("extractor_args", {})
    yt_args = dict(args.get("youtube") or {})
    yt_args["player_client"] = ["tv", "mweb", "web_safari"] if has_cookies() else ["android_vr"]
    args["youtube"] = yt_args
    return retry

def aria2_exit_code(error):
    match = ARIA2_EXIT.search(str(error))
    return int(match.group(1)) if match else None

def native_downloader_opts(opts):
    """نسخة من خيارات yt-dlp من غير aria2، لاستخدام المحمّل الأصلي عند الحاجة."""
    native = dict(opts)
    native.pop("external_downloader", None)
    native.pop("external_downloader_args", None)
    return native

def note_aria2_failure(error):
    global HAS_ARIA2
    code = aria2_exit_code(error)
    if code == 28 and HAS_ARIA2:
        HAS_ARIA2 = False
        log.error("aria2 rejected a command-line option (exit 28); disabling aria2 for this process")
    return code is not None

def base_opts(referer=None):
    o = {"quiet": True, "no_warnings": True, "retries": 10, "fragment_retries": 10,
         "socket_timeout": 30, "concurrent_fragment_downloads": FRAGMENTS, "noprogress": True}
    if referer: o["http_headers"] = {"Referer": referer, "User-Agent": UA}
    if has_cookies(): o["cookiefile"] = COOKIE_FILE
    if PROXY: o["proxy"] = PROXY
    return o

def ydl_run(url, opts, download):
    """يجرب افتراضيًا بلا جلسة؛ لرفض YouTube يعيد بعميل Android VR مجهول ثم Chrome impersonation."""
    try:
        with yt_dlp.YoutubeDL(opts) as y:
            return y.extract_info(url, download=download)
    except Exception as initial_error:
        error = initial_error
        if opts.get("external_downloader") and note_aria2_failure(error):
            native = native_downloader_opts(opts)
            log.warning("aria2 download failed (code %s); retrying with yt-dlp native downloader",
                        aria2_exit_code(error))
            try:
                with yt_dlp.YoutubeDL(native) as y:
                    return y.extract_info(url, download=download)
            except Exception as native_error:
                error = native_error
                log.warning("native downloader retry failed: %s", native_error)
        if is_youtube_url(url) and RETRY_YT_ANON.search(str(error)):
            anonymous = youtube_android_vr_opts(opts)
            if not HAS_ARIA2:
                anonymous = native_downloader_opts(anonymous)
            log.warning("YouTube rejected the default client; retrying with fallback clients (cookies: %s)", has_cookies())
            try:
                with yt_dlp.YoutubeDL(anonymous) as y:
                    return y.extract_info(url, download=download)
            except Exception as anonymous_error:
                error = anonymous_error
                log.warning("YouTube android_vr retry failed: %s", anonymous_error)
        if IMPERSONATE and (RETRY_IMP.search(str(error)) or RETRY_IMP.search(str(initial_error))):
            o2 = native_downloader_opts(opts)
            o2["impersonate"] = IMPERSONATE
            try:
                with yt_dlp.YoutubeDL(o2) as y:
                    return y.extract_info(url, download=download)
            except Exception as impersonate_error:
                error = impersonate_error
        raise error

def is_single_video(url):
    """فيديو يوتيوب مفرد (مش Playlist). المواقع التانية بيقررها yt-dlp نفسه."""
    p = urlparse(url)
    host = (p.hostname or "").lower()
    if not any(host == h or host.endswith("." + h) for h in YT_HOSTS): return False
    if host.endswith("youtu.be") or p.path.startswith(("/shorts/", "/live/", "/embed/")): return True
    return p.path == "/watch" and "v=" in p.query

def probe_ytdlp(url, referer=None):
    single = is_single_video(url)
    o = base_opts(referer)
    o.update(extract_flat="in_playlist", noplaylist=single)
    if MAX_PLAYLIST > 0: o["playlistend"] = MAX_PLAYLIST
    info = ydl_run(url, o, False)
    items, ptitle = [], None
    if info.get("_type") == "playlist":
        ptitle = info.get("title")
        for e in info.get("entries") or []:
            if not e: continue
            u, eid = e.get("url") or e.get("webpage_url"), e.get("id")
            ie = (e.get("ie_key") or info.get("extractor_key") or "x").lower()
            if not u or not u.startswith("http"):
                if ie == "youtube" and eid: u = f"https://www.youtube.com/watch?v={eid}"
                else: continue
            if not url_ok(u): continue
            items.append(Item(u, e.get("title") or u, f"{ie}:{eid or u}", referer=referer or ""))
            if MAX_PLAYLIST > 0 and len(items) >= MAX_PLAYLIST: break
    else:
        ie = (info.get("extractor_key") or "x").lower()
        items.append(Item(url, info.get("title") or url, f"{ie}:{info.get('id') or url}",
                          referer=referer or "", info=info if info.get("formats") else None))
    return items, ptitle

# ───────────── كاشف الفيديو في أي صفحة ─────────────
OG_RE = re.compile(r"""<meta[^>]+(?:property|name)=["']og:video(?::secure_url|:url)?["'][^>]+content=["']([^"']+)""", re.I)
SRC_RE = re.compile(r"""<(?:video|source)[^>]+src=["']([^"']+)""", re.I)
MEDIA_RE = re.compile(r"""https?://[^\s"'<>\\]+?\.(?:m3u8|mpd|mp4|webm|mov|mkv)(?:\?[^\s"'<>\\]*)?""", re.I)
IFRAME_RE = re.compile(r"""<iframe[^>]+src=["']([^"']+)""", re.I)
TITLE_RE = re.compile(r"""<meta[^>]+property=["']og:title["'][^>]+content=["']([^"']+)|<title[^>]*>([^<]+)""", re.I)

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **kw): return None

_REDIRECT_CODES = (301, 302, 303, 307, 308)

def _get_once(url):
    """طلب واحد من غير ما يتبع Redirect تلقائيًا. بيرجّع (نص, لينك_التحويل_أو_None)."""
    try:
        if IMPERSONATE:
            from curl_cffi import requests as cr
            r = cr.get(url, impersonate="chrome", timeout=20, allow_redirects=False)
            if r.status_code in _REDIRECT_CODES and r.headers.get("location"):
                return "", r.headers["location"]
            return r.text[:3_000_000], None
    except Exception as e:
        log.debug("curl_cffi fetch failed: %s", e)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.build_opener(_NoRedirect).open(req, timeout=20) as r:
            return r.read(3_000_000).decode("utf-8", "ignore"), None
    except urllib.error.HTTPError as e:
        if e.code in _REDIRECT_CODES and e.headers.get("Location"):
            return "", e.headers["Location"]
        raise

def fetch_html(url):
    """بيجيب الصفحة وبيفحص كل Redirect (لحد 5) قبل ما يتبعه — عشان تحويلة لعنوان داخلي ما تعدّيش."""
    cur = url
    for _ in range(6):
        if not url_ok(cur): raise RuntimeError("unsafe or blocked url")
        text, loc = _get_once(cur)
        if loc is None: return text
        cur = urljoin(cur, loc)
    raise RuntimeError("too many redirects")

def sniff(url):
    txt = fetch_html(url).replace("\\/", "/").replace("&amp;", "&")
    cands = OG_RE.findall(txt) + SRC_RE.findall(txt) + MEDIA_RE.findall(txt) + IFRAME_RE.findall(txt)
    seen, urls = set(), []
    for c in cands:
        c = urljoin(url, c.strip())
        if c.startswith("http") and c not in seen and c != url and url_ok(c):   # SSRF: كل مرشح لازم يعدّي الفحص
            seen.add(c); urls.append(c)
    m = TITLE_RE.search(txt)
    title = (m.group(1) or m.group(2)).strip() if m else ""
    for c in urls:
        try:
            items, _ = probe_ytdlp(c, referer=url)
        except Exception:
            continue
        if items:
            it = items[0]
            it.title = title[:100] or it.title
            it.referer = url
            it.key = "sniff:" + hashlib.sha1(c.encode()).hexdigest()[:16]
            return [it]
    raise RuntimeError("no video found in page")

# ───────────── gallery-dl (صور وألبومات) ─────────────
def gallery_supported(url):
    if not (HAS_GALLERY and gdx): return False
    try: return gdx.find(url) is not None
    except Exception: return False

def do_gallery(url, outdir):
    cmd = ["gallery-dl", "-q", "-D", outdir, "-f", "{num:>03}.{extension}"]
    if MAX_GALLERY > 0: cmd += ["--range", f"1-{MAX_GALLERY}"]
    r = subprocess.run(cmd + [url], capture_output=True, text=True)
    files = sorted(os.path.join(outdir, f) for f in os.listdir(outdir)
                   if os.path.isfile(os.path.join(outdir, f)) and not f.endswith((".json", ".txt", ".part")))
    if not files:
        raise RuntimeError((r.stderr or "no media").strip()[-300:])
    return files

# ───────────── محرك الاكتشاف (بالترتيب) ─────────────
def probe(url):
    """yt-dlp ← gallery-dl ← كاشف الصفحة. لو الكل فشل بيرجّع خطأ yt-dlp الأصلي."""
    try:
        return probe_ytdlp(url)
    except Exception as first:
        if gallery_supported(url):
            host = (urlparse(url).hostname or "").replace("www.", "")
            return [Item(url, f"📷 {host}", "gal:" + hashlib.sha1(url.encode()).hexdigest()[:16],
                         engine="gallery")], None
        try:
            return sniff(url), None
        except Exception as e2:
            log.info("all engines failed for %s | ytdlp: %s | sniff: %s", url, first, e2)
            raise first

def check_limits(info):
    """يرفض البث المباشر فقط؛ لا يوجد حد لمدة الفيديو المسجل."""
    if not info: return
    if info.get("is_live") or info.get("live_status") in ("is_live", "is_upcoming"):
        raise RuntimeError("live event")                      # بيتحوّل لرسالة عربي من ERR_MAP

def _match_filter(info, *, incomplete=False):
    if not incomplete: check_limits(info)
    return None

# H.264 أولًا: تليجرام (iOS/Desktop) ساعات مش بيشغّل VP9/AV1 جوه الـ MP4
SZ = "[filesize<?1900M]"

def fmt_for(mode):
    if mode == "audio": return "bestaudio/best"
    if mode == "best":
        return (f"bv*[vcodec^=avc1]{SZ}+ba[ext=m4a]/bv*{SZ}+ba[ext=m4a]/bv*{SZ}+ba/b")
    h = f"[height<={mode}]"
    return (f"bv*{h}[vcodec^=avc1]{SZ}+ba[ext=m4a]/bv*{h}{SZ}+ba/b{h}{SZ}/b{h}/b")

FALLBACK = {"best": ["1080", "720", "480", "360"], "1080": ["720", "480", "360"], "720": ["480", "360"],
            "480": ["360"], "360": [], "audio": []}

def fallback_qualities(mode):
    if mode in FALLBACK: return FALLBACK[mode]
    try: target = int(mode)
    except (TypeError, ValueError): return []
    return [str(height) for height in (2160, 1440, 1080, 720, 480, 360) if height < target]

def do_download_fit(it, mode, outdir, progress_hook=None):
    """لو الملف عدّى 2GB بينزل جودة تلقائيًا بدل ما يفشل. بيرجّع (مسار, info, الجودة_الفعلية)."""
    for m in [mode] + fallback_qualities(mode):
        path, info = do_download(it, m, outdir, progress_hook)
        if os.path.getsize(path) <= MAX_SIZE:
            return path, info, m
        log.info("file too big in %s for %s, trying lower quality", m, it.url)
        try: os.remove(path)
        except OSError: pass
    raise RuntimeError("الملف أكبر من 2GB حتى بأقل جودة")

def do_download(it, mode, outdir, progress_hook=None):
    o = base_opts(it.referer or None)
    o.update(noplaylist=True, outtmpl=f"{outdir}/%(title).60s.%(ext)s")
    if progress_hook: o["progress_hooks"] = [progress_hook]
    if HAS_ARIA2:
        o["external_downloader"] = {"default": "aria2c", "dash": "native",
                                    "m3u8": "native", "m3u8_native": "native"}
        o["external_downloader_args"] = {"aria2c": [
            "-x", str(min(CONN, 16)), "-s", str(CONN), "-k", "1M", "--min-split-size=1M",
            "--file-allocation=none", "--disable-ipv6=true", "--connect-timeout=10",
            "--max-tries=5", "--retry-wait=1", "--summary-interval=0",
            "--console-log-level=error"]}
    o["format"] = fmt_for(mode)
    o["match_filter"] = _match_filter
    if mode == "audio":
        o["postprocessors"] = [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3",
                                "preferredquality": "192"}]
    else:
        o["merge_output_format"] = "mp4"
    info = None
    if it.info:                    # نستخدم نتيجة الفحص: بنوفر 2-5 ثواني استخراج
        check_limits(it.info)      # بره الـ try عشان الرفض ما يتحوّلش لإعادة استخراج
        try:
            with yt_dlp.YoutubeDL(o) as y:
                info = y.process_ie_result(copy.deepcopy(it.info), download=True)
        except Exception as e:
            log.info("reuse probe info failed, re-extracting: %s", e)
            for f in os.listdir(outdir):
                try: os.remove(os.path.join(outdir, f))
                except OSError: pass
            info = None
            if o.get("external_downloader") and note_aria2_failure(e):
                o = native_downloader_opts(o)
                log.warning("aria2 failed on reused probe info; retrying with yt-dlp native downloader")
                try:
                    with yt_dlp.YoutubeDL(o) as y:
                        info = y.process_ie_result(copy.deepcopy(it.info), download=True)
                except Exception as native_error:
                    log.info("native retry of reused probe info failed: %s", native_error)
    if info is None:
        info = ydl_run(it.url, o, True)
    files = [os.path.join(outdir, f) for f in os.listdir(outdir)]
    files = [f for f in files if os.path.isfile(f) and not f.endswith((".aria2", ".part", ".ytdl"))]
    if not files:
        raise RuntimeError("التحميل فشل (مفيش ملف ناتج)")
    return max(files, key=os.path.getsize), info

def dir_size(d):
    t = 0
    for f in os.listdir(d):
        try: t += os.path.getsize(os.path.join(d, f))
        except OSError: pass
    return t

async def make_thumb(path, out):
    for ss in ("1", "0"):          # الفيديو القصير (<1 ثانية) مفيهوش فريم عند الثانية 1
        try:
            p = await asyncio.create_subprocess_exec(
                "ffmpeg", "-y", "-ss", ss, "-i", path, "-frames:v", "1", "-vf", "scale=320:-1", out,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            await p.wait()
        except Exception:
            return None
        if os.path.exists(out): return out
    return None

# ───────────── العلامة المائية (بتتحكم فيها من لوحة الأدمن) ─────────────
FONT = next((f for f in ("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                         "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf") if os.path.exists(f)), None)
WM_IMG_PATH = os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "watermark.png")   # جنب القاعدة (على Volume)
WM_SIZES = {"S": 0.03, "M": 0.045, "L": 0.065}      # حجم النص = نسبة من ارتفاع الفيديو
WM_IMG_SIZES = {"S": 0.12, "M": 0.2, "L": 0.3}      # عرض الصورة = نسبة من عرض الفيديو
WM_POS_LABEL = {"tl": "↖️ فوق شمال", "tr": "↗️ فوق يمين", "bl": "↙️ تحت شمال", "br": "↘️ تحت يمين", "c": "⏺ المنتصف"}
WM_SIZE_LABEL = {"S": "صغير", "M": "وسط", "L": "كبير"}

def wm_snapshot(mode):
    """لقطة من إعدادات العلامة وقت بداية العنصر (عشان تغيير الإعدادات في نص التحميل ما يلخبطش الكاش).
    None = من غير علامة (مقفولة أو الوضع صوت)."""
    if mode == "audio" or cfg("wm_on") != "1": return None
    t = cfg("wm_type")
    d = {"type": t, "pos": cfg("wm_pos"), "size": cfg("wm_size"),
         "v": cfg("wm_text") if t == "text" else cfg("wm_img_sig")}
    d["sig"] = hashlib.sha1(repr(sorted(d.items())).encode()).hexdigest()[:8]
    return d

def cache_mode(mode, wm):
    """الكاش بيفرّق بين النسخة العادية والنسخ اللي عليها علامة (حسب نوعها ومكانها وحجمها)."""
    return f"{mode}|wm{wm['sig']}" if wm else mode

def wm_missing():
    """سبب إن العلامة مش جاهزة (None = جاهزة)."""
    if cfg("wm_type") == "image":
        return None if os.path.exists(WM_IMG_PATH) else "ارفع صورة العلامة الأول (🖼 رفع صورة)"
    if not FONT: return "الخط مش متسطّب على السيرفر"
    return None if cfg("wm_text").strip() else "اكتب نص العلامة الأول (✏️ تغيير النص)"

def build_wm_cmd(src, out, w, h, wm, txt_file=None):
    """أمر ffmpeg بيحط العلامة (دالة نقية عشان تتختبر لوحدها). بيعيد ترميز الفيديو H.264 والصوت بيتنسخ."""
    pos, even = wm["pos"], "scale=trunc(iw/2)*2:trunc(ih/2)*2"
    m = max(8, int(min(w, h) * 0.02))
    enc = ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-pix_fmt", "yuv420p",
           "-c:a", "copy", "-movflags", "+faststart", out]
    if wm["type"] == "image":
        ox = {"tl": m, "bl": m, "tr": f"W-w-{m}", "br": f"W-w-{m}", "c": "(W-w)/2"}[pos]
        oy = {"tl": m, "tr": m, "bl": f"H-h-{m}", "br": f"H-h-{m}", "c": "(H-h)/2"}[pos]
        ww = max(32, int(w * WM_IMG_SIZES[wm["size"]]) // 2 * 2)
        fc = (f"[1:v]scale={ww}:-2,format=rgba,colorchannelmixer=aa=0.85[wm];"
              f"[0:v][wm]overlay={ox}:{oy},{even}[v]")
        return ["ffmpeg", "-y", "-i", src, "-i", WM_IMG_PATH, "-filter_complex", fc,
                "-map", "[v]", "-map", "0:a?"] + enc
    fs = max(14, int(h * WM_SIZES[wm["size"]]))
    tx = {"tl": m, "bl": m, "tr": f"w-text_w-{m}", "br": f"w-text_w-{m}", "c": "(w-text_w)/2"}[pos]
    ty = {"tl": m, "tr": m, "bl": f"h-text_h-{m}", "br": f"h-text_h-{m}", "c": "(h-text_h)/2"}[pos]
    vf = (f"drawtext=fontfile={FONT}:textfile={txt_file}:expansion=none:fontsize={fs}:fontcolor=white@0.85:"
          f"borderw={max(1, fs // 14)}:bordercolor=black@0.6:x={tx}:y={ty},{even}")
    return ["ffmpeg", "-y", "-i", src, "-vf", vf, "-map", "0:v:0", "-map", "0:a?"] + enc

async def probe_wh(path):
    try:
        p = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height",
            "-of", "csv=s=x:p=0", path, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        out, _ = await p.communicate()
        w, h = out.decode().strip().split("x")[:2]
        return int(w), int(h)
    except Exception:
        return 0, 0

async def apply_watermark(path, wm, outdir):
    """بيعمل نسخة بالعلامة المائية ويمسح الأصل. لو فشل بيرمي خطأ واضح (مفيش إرسال من غير علامة بالسكوت)."""
    if wm["type"] == "image":
        if not os.path.exists(WM_IMG_PATH): raise RuntimeError("صورة العلامة المائية مش موجودة — ارفعها من لوحة الأدمن")
    elif not FONT:
        raise RuntimeError("خط العلامة المائية مش متسطّب على السيرفر")
    w, h = await probe_wh(path)
    if not (w and h): raise RuntimeError("مقدرتش أقرا أبعاد الفيديو علشان أحط العلامة")
    out, txt_file = os.path.join(outdir, "wm_out.mp4"), None
    if wm["type"] == "text":
        txt_file = os.path.join(outdir, "wm.txt")
        with open(txt_file, "w", encoding="utf-8") as f: f.write(wm["v"])     # من غير سطر جديد في الآخر
    p = await asyncio.create_subprocess_exec(*build_wm_cmd(path, out, w, h, wm, txt_file),
                                             stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
    _, err = await p.communicate()
    if p.returncode != 0 or not os.path.exists(out):
        log.warning("watermark failed: %s", err.decode("utf-8", "ignore")[-400:])
        raise RuntimeError("فشل إضافة العلامة المائية")
    os.remove(path)
    return out

async def with_retry(fn, *a, tries=3, **kw):
    """إعادة محاولة للرفع: FloodWait بيستنى المدة المطلوبة، وأي خطأ شبكة بيتعاد بفاصل متزايد."""
    for i in range(tries):
        try:
            return await fn(*a, **kw)
        except FloodWait as e:
            if i == tries - 1: raise
            await asyncio.sleep(min(e.value + 1, 60))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if i == tries - 1: raise
            log.info("send failed (%s), retry %d", e, i + 1)
            await asyncio.sleep(2 * (i + 1))

async def wait_disk(poll=3, tries=40):
    """مانع إننا نبدأ تحميل جديد والقرص شبه ممتلئ (تحميلات متزامنة + دمج فيديو/صوت بياخدوا مساحة)."""
    for _ in range(tries):
        if shutil.disk_usage(tempfile.gettempdir()).free > MIN_FREE_MB * 1024 * 1024: return
        await asyncio.sleep(poll)
    raise RuntimeError("السيرفر ممتلئ مؤقتًا — حاول بعد شوية")

# ───────────── الإرسال ─────────────
def human_bytes(value):
    try: value = max(float(value or 0), 0.0)
    except (TypeError, ValueError): value = 0.0
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024

def progress_text(current, total=None, speed=None, eta=None, width=12):
    current = max(float(current or 0), 0.0)
    try: total = float(total) if total else 0.0
    except (TypeError, ValueError): total = 0.0
    if total > 0:
        ratio = min(max(current / total, 0.0), 1.0)
        filled = int(ratio * width)
        bar = "█" * filled + "░" * (width - filled)
        heading = f"{bar} {int(ratio * 100)}%"
        amount = f"{human_bytes(current)} / {human_bytes(total)}"
    else:
        position = int(time.monotonic() * 2) % width
        bar = "░" * position + "█" + "░" * (width - position - 1)
        heading = bar
        amount = human_bytes(current)
    parts = [amount]
    try:
        if speed and float(speed) > 0: parts.append(f"⚡ {human_bytes(speed)}/s")
    except (TypeError, ValueError): pass
    try:
        if eta is not None:
            seconds = max(0, int(eta))
            h, rem = divmod(seconds, 3600)
            m, s = divmod(rem, 60)
            eta_text = f"{h}h {m}m {s}s" if h else f"{m}m {s:02d}s" if m else f"{s}s"
            parts.append(f"⏱ {eta_text}")
    except (TypeError, ValueError, OverflowError): pass
    return heading + "\n" + " • ".join(parts)

def make_upload_progress(it, label="رفع"):
    st = {"at": time.monotonic(), "bytes": 0.0, "speed": 0.0}
    async def callback(current, total, *_):
        now, cur = time.monotonic(), float(current or 0)
        dt, finished = now - st["at"], bool(total and cur >= total)
        if dt < 1.0 and not finished: return
        inst = max(cur - st["bytes"], 0) / max(dt, 0.01)
        st["speed"] = inst if not st["speed"] else 0.6 * st["speed"] + 0.4 * inst   # تنعيم السرعة
        st["at"], st["bytes"] = now, cur
        try: eta = (float(total) - cur) / st["speed"] if st["speed"] > 0 and total else None
        except (TypeError, ValueError): eta = None
        it.detail = progress_text(cur, total, st["speed"], eta)
    return callback

def make_download_progress_hook(it, loop, state):
    throttle = {"at": 0.0}
    def hook(data):
        status = data.get("status")
        now = time.monotonic()
        if status == "downloading":
            if now - throttle["at"] < 0.7: return
            throttle["at"] = now
            current = data.get("downloaded_bytes") or 0
            total = data.get("total_bytes") or data.get("total_bytes_estimate")
            speed = data.get("speed")
            eta = data.get("eta")
            detail = progress_text(current, total, speed, eta)
        elif status == "finished":
            detail = "اكتمل تنزيل الجزء • جارٍ الدمج/المعالجة..."
        else:
            return
        def publish():
            it.detail = detail
            state["updated"] = time.monotonic()
        try: loop.call_soon_threadsafe(publish)
        except RuntimeError: pass
    return hook

async def wait_with_progress(task, it, outdir, hook_state=None):
    previous_size, previous_at = 0, time.monotonic()
    while not task.done():
        await asyncio.sleep(1)
        now = time.monotonic()
        if hook_state and now - hook_state["updated"] < 2:
            continue
        size = dir_size(outdir)
        speed = max(size - previous_size, 0) / max(now - previous_at, 0.01)
        it.detail = progress_text(size, speed=speed)
        previous_size, previous_at = size, now
        if hook_state: hook_state["updated"] = now
    return await task

async def send_media(b, it, src, info, thumb=None, progress=None):
    info = info or {}
    cap = (info.get("title") or it.title)[:200]
    if b.mode == "audio":
        m = await app.send_audio(b.chat_id, src, caption=cap,
                                 duration=int(info.get("duration") or 0),
                                 title=info.get("title"), performer=info.get("uploader"),
                                 progress=progress)
        return m.audio.file_id if m.audio else None
    m = await app.send_video(b.chat_id, src, caption=cap,
                             duration=int(info.get("duration") or 0),
                             width=info.get("width") or 0, height=info.get("height") or 0,
                             thumb=thumb, supports_streaming=True, progress=progress)
    if m.video: return m.video.file_id
    return m.document.file_id if m.document else None

IMG_EXT, VID_EXT = {".jpg", ".jpeg", ".png", ".webp"}, {".mp4", ".mov", ".mkv", ".webm", ".m4v"}

async def send_gallery(b, it, files, progress_factory=None):
    progress_factory = progress_factory or (lambda label: None)
    cap, ext = it.title[:200], lambda f: os.path.splitext(f)[1].lower()
    imgs = [f for f in files if ext(f) in IMG_EXT and os.path.getsize(f) <= 10 * 1024 * 1024]
    vids = [f for f in files if ext(f) in VID_EXT]
    gifs = [f for f in files if ext(f) == ".gif"]
    docs = [f for f in files if f not in imgs and f not in vids and f not in gifs]
    first = True
    for i in range(0, len(imgs), 10):
        chunk = imgs[i:i + 10]
        try:
            if len(chunk) == 1:
                await app.send_photo(b.chat_id, chunk[0], caption=cap if first else None,
                                     progress=progress_factory("رفع صورة"))
            else:
                it.detail = f"رفع ألبوم الصور {i + 1}–{i + len(chunk)} من {len(imgs)}"
                await app.send_media_group(b.chat_id, [InputMediaPhoto(p, caption=cap if (first and j == 0) else None)
                                                       for j, p in enumerate(chunk)])
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
        except Exception:
            docs += chunk
        first = False
    for v in vids:
        await app.send_video(b.chat_id, v, caption=cap if first else None, supports_streaming=True,
                             progress=progress_factory("رفع فيديو")); first = False
    for g in gifs:
        await app.send_animation(b.chat_id, g, caption=cap if first else None,
                                 progress=progress_factory("رفع GIF")); first = False
    for d in docs:
        await app.send_document(b.chat_id, d, caption=cap if first else None,
                                progress=progress_factory("رفع ملف")); first = False

# ───────────── معالجة عنصر واحد ─────────────
async def run_item(b, it):
    async with b.sem:
        if b.cancelled:
            it.status, it.detail = "err", "أُلغي"
            return
        wm = wm_snapshot(b.mode) if it.engine == "ytdlp" else None   # إعدادات العلامة وقت بداية العنصر
        cm = cache_mode(b.mode, wm)
        try:
            if it.engine == "ytdlp":
                fid = cache_get(it.key, cm)
                if fid:
                    try:
                        it.status, it.detail = "up", "فوري ⚡"
                        await send_media(b, it, fid, None)
                        it.status = "done"; record(b.user_id, it.title, b.mode)
                        return
                    except Exception as e:
                        log.warning("cache miss for %s: %s", it.key, e)

            outdir = tempfile.mkdtemp(prefix="dl_")
            try:
                if it.engine == "gallery":
                    async with user_sem(b.user_id), DL_SEM:
                        it.status, it.detail = "dl", "جارٍ تجهيز تنزيل الصور..."
                        task = asyncio.create_task(asyncio.to_thread(do_gallery, it.url, outdir))
                        files = await wait_with_progress(task, it, outdir)
                    if b.cancelled: raise RuntimeError("أُلغي")
                    async with UP_SEM:
                        it.status, it.detail = "up", f"بدء رفع {len(files)} ملف..."
                        await with_retry(send_gallery, b, it, files,
                                         lambda label: make_upload_progress(it, label), tries=2)
                    it.status = "done"; record(b.user_id, it.title, "gallery")
                    return

                async with user_sem(b.user_id), DL_SEM:
                    it.status, it.detail = "dl", "جارٍ تجهيز التنزيل..."
                    await wait_disk()
                    loop = asyncio.get_running_loop()
                    progress_state = {"updated": time.monotonic()}
                    hook = make_download_progress_hook(it, loop, progress_state)
                    task = asyncio.create_task(asyncio.to_thread(
                        do_download_fit, it, b.mode, outdir, hook))
                    path, info, used = await wait_with_progress(task, it, outdir, progress_state)
                    it.detail = f"{'█' * 12} 100%\n{human_bytes(os.path.getsize(path))} • اكتمل التنزيل"
                    if wm and not b.cancelled:
                        it.detail = "🔖 بيحط العلامة المائية..."
                        path = await apply_watermark(path, wm, outdir)
                        if os.path.getsize(path) > MAX_SIZE:
                            raise RuntimeError("الملف بعد العلامة المائية أكبر من 2GB")
                if b.cancelled: raise RuntimeError("أُلغي")
                if used != b.mode: log.info("quality lowered %s -> %s for %s", b.mode, used, it.url)

                async with UP_SEM:
                    it.status, it.detail = "up", "جارٍ تهيئة الرفع..."
                    prog = make_upload_progress(it)
                    thumb = None if b.mode == "audio" else await make_thumb(path, f"{outdir}/thumb.jpg")
                    fid = await with_retry(send_media, b, it, path, info, thumb, prog)
                if fid: cache_put(it.key, cm, fid)
                it.status = "done"; record(b.user_id, it.title, b.mode)
            finally:
                it.info = None
                shutil.rmtree(outdir, ignore_errors=True)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            it.status, it.detail = "err", clean_err(e)
            log.warning("item failed %s: %s", it.url, e)

# ───────────── شاشة الحالة ─────────────
def render(b, final=False):
    total = len(b.items)
    c = Counter(i.status for i in b.items)
    active = [i for i in b.items if i.status in ("dl", "up")]
    blocks = []
    for i in active[:12]:
        index = b.items.index(i) + 1
        blocks.append(f"🚀 {index}/{total} جارٍ التحميل والإرسال إليك...\n{i.detail}")
    if len(active) > 12: blocks.append(f"… و{len(active) - 12} عناصر تانية شغالة دلوقتي")
    if not active and not final and c["wait"]:
        blocks.append(f"⏳ جارٍ التجهيز... ({c['wait']} في الانتظار)")
    txt = "\n\n".join(blocks)
    if not final and (c["done"] or c["err"]):
        txt += ("\n\n" if txt else "") + f"✅ {c['done']}/{total} اتبعتوا" + (f"  •  ❌ {c['err']}" if c["err"] else "")
    errs = [i for i in b.items if i.status == "err"]
    if errs: txt += "\n\n" + "\n".join(f"❌ {i.title[:28]}: {i.detail[:70]}" for i in errs[-4:])
    if final:
        head = f"✅ تم {c['done']}/{total}  •  🏁 خلصنا في {int(time.time() - b.started_at)} ثانية ⚡"
        txt = head + ("\n\n" + txt if txt else "")
    elif b.cancelled: txt += "\n\n🛑 جاري الإلغاء بعد العناصر الجارية..."
    return txt.strip() or "⏳ جارٍ التجهيز..."

async def safe_edit(msg, text, kb=None):
    t = pe_html(text)
    try: await msg.edit_text(t, reply_markup=kb)
    except MessageNotModified: pass
    except FloodWait as e: await asyncio.sleep(min(e.value, 15))
    except Exception as e:
        if pe_active():          # ممكن الإيموجي البريميوم مرفوض (صاحب البوت من غير Premium) → نجرب من غيره
            plainify_kb(kb)
            try:
                await msg.edit_text(text, reply_markup=kb)
                PE["broken"] = True
                log.warning("premium emoji rejected (%s); disabled until admin re-enables", e)
            except MessageNotModified: pass
            except Exception as e2: log.debug("edit failed: %s", e2)
        else: log.debug("edit failed: %s", e)

async def replace_with_reply(old_msg, original_msg, text, kb=None):
    try:
        await old_msg.delete()
    except Exception as e:
        log.debug("could not delete metadata wait message: %s", e)
        await old_msg.edit_text(text, reply_markup=kb)
        return old_msg
    return await original_msg.reply_text(text, reply_markup=kb)

async def run_batch(b):
    b.started = True
    b.started_at = time.time()
    tasks = [asyncio.create_task(run_item(b, it)) for it in b.items]
    async def loop():
        while not b.done:
            await safe_edit(b.msg, render(b), cancel_kb(b))
            await asyncio.sleep(2)
    ui = asyncio.create_task(loop())
    await asyncio.gather(*tasks, return_exceptions=True)
    b.done = True
    ui.cancel()
    await safe_edit(b.msg, render(b, final=True), after_kb())
    batches.pop(b.id, None)

async def begin(b, answer=None):
    """يبدأ الدفعة بلا حصة يومية أو اقتطاع للعناصر."""
    asyncio.create_task(run_batch(b))

# ───────────── الشاشات ─────────────
def welcome_text(name):
    return (f"🎬 أهلاً {name}!\n\n"
            f"أنا بوت تحميل الفيديوهات والصور من أكتر من {SITE_COUNT} موقع.\n"
            "ابعتلي أي لينك (أو أكتر) مباشرة، أو استخدم الأزرار 👇")

HELP = ("❓ ازاي أستخدم البوت؟\n\n"
        "1️⃣ ابعت لينك (أو لينكات كتير في رسالة واحدة)\n"
        "2️⃣ اختار الجودة من الأزرار\n"
        "3️⃣ استنى وهيوصلك الفيديو\n\n"
        "📋 لينك Playlist بيتحمّل كله\n"
        "📷 لينكات الصور والألبومات بتتبعت صور\n"
        "🔎 لو الموقع مش معروف، بدوّر على الفيديو جوه الصفحة لوحدي\n"
        "⚡ النسخة المخزنة بتتبعت من الكاش بعد الاختيار بدل إعادة تنزيل المصدر\n"
        "⚙️ من الإعدادات تثبّت جودة افتراضية وتتخطى سؤال الجودة")

def sites_text():
    return (f"🌐 المواقع المدعومة\n\nبيشتغل مع أكتر من {SITE_COUNT} موقع، منهم:\n\n"
            "🎥 يوتيوب • تيك توك • انستجرام • فيسبوك • تويتر/X • ثريدز • ريديت • لينكدإن\n"
            "🎬 فيميو • ديلي موشن • تويتش • بيليبيلي • رومبل • Streamable • VK • OK.ru\n"
            "🎧 ساوند كلاود • باندكامب • ميكس كلاود (صوت)\n"
            "📷 بنترست • تمبلر • فليكر • صور انستجرام وتويتر وريديت (ألبومات)\n"
            "🔎 أي صفحة فيها فيديو، حتى لو الموقع مش في القايمة\n\n"
            "⚠️ مش بيشتغل مع المحتوى المحمي بـ DRM (نتفلكس، ديزني+، شاهد VIP...) "
            "ولا المحتوى الخاص، ومحجوب المحتوى المخالف لقواعد تليجرام.")

async def show(target, text, kb):
    if hasattr(target, "data"):
        await safe_edit(target.message, text, kb)
    else:
        await target.reply_text(pe_html(text), reply_markup=kb)

def _dl_stream(n):
    req = urllib.request.Request(f"https://speed.cloudflare.com/__down?bytes={n}", headers={"User-Agent": UA})
    got = 0
    with urllib.request.urlopen(req, timeout=30) as r:
        while True:
            chunk = r.read(1 << 20)
            if not chunk: break
            got += len(chunk)
    return got

def speed_download_test(streams=4, size=25 * 1024 * 1024):
    from concurrent.futures import ThreadPoolExecutor
    t = time.monotonic()
    with ThreadPoolExecutor(streams) as ex:
        total = sum(ex.map(_dl_stream, [size] * streams))
    return total / max(time.monotonic() - t, 0.001) / 1e6  # MB/s decimal, aggregate streams

def _read_text(path):
    try:
        with open(path, "r", encoding="ascii") as f: return f.read().strip()
    except (OSError, UnicodeError):
        return None

def _cgroup_memory():
    pairs = (("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.max"),
             ("/sys/fs/cgroup/memory/memory.usage_in_bytes", "/sys/fs/cgroup/memory/memory.limit_in_bytes"),
             ("/sys/fs/cgroup/memory.usage_in_bytes", "/sys/fs/cgroup/memory.limit_in_bytes"))
    for current_path, limit_path in pairs:
        current, raw_limit = _read_text(current_path), _read_text(limit_path)
        try:
            if current is None or raw_limit is None or raw_limit == "max": continue
            current, limit = int(current), int(raw_limit)
            if 0 < limit < (1 << 60): return current, limit
        except (TypeError, ValueError, OverflowError):
            continue
    return None, None

def _cgroup_cpu_limit():
    raw = _read_text("/sys/fs/cgroup/cpu.max")
    if raw:
        try:
            quota, period = raw.split()[:2]
            if quota != "max" and int(period) > 0: return int(quota) / int(period)
        except (ValueError, ZeroDivisionError):
            pass
    quota, period = _read_text("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"), _read_text("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
    try:
        if quota and period and int(quota) > 0 and int(period) > 0: return int(quota) / int(period)
    except (ValueError, ZeroDivisionError):
        pass
    return None

def _process_rss_bytes():
    status = _read_text("/proc/self/status")
    if status:
        for line in status.splitlines():
            if line.startswith("VmRSS:"):
                try: return int(line.split()[1]) * 1024
                except (IndexError, ValueError): break
    return None

def _allowed_cpu_count():
    try: return len(os.sched_getaffinity(0))
    except (AttributeError, OSError): return os.cpu_count()

def _cpu_sample_percent(interval=0.4):
    """نسبة استهلاك عملية البوت من نواة واحدة خلال عينة قصيرة؛ قد تتجاوز 100% عند تعدد الخيوط."""
    wall_start, cpu_start = time.monotonic(), time.process_time()
    time.sleep(interval)
    elapsed = max(time.monotonic() - wall_start, 0.001)
    return max(0.0, (time.process_time() - cpu_start) / elapsed * 100)

def runtime_metrics_text():
    cpu_pct = _cpu_sample_percent()
    cores = _allowed_cpu_count()
    cpu_limit = _cgroup_cpu_limit()
    rss = _process_rss_bytes()
    mem_current, mem_limit = _cgroup_memory()
    uptime = max(0, int(time.monotonic() - PROCESS_STARTED_AT))
    days, rem = divmod(uptime, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    uptime_text = f"{days}ي {hours:02}س {minutes:02}د" if days else f"{hours:02}س {minutes:02}د {seconds:02}ث"
    lines = [f"🖥 CPU عملية البوت: {cpu_pct:.1f}% من نواة واحدة"
             + (f" | حصة cgroup: {cpu_limit:.2f} نواة" if cpu_limit else f" | أنوية CPU المسموحة: {cores or 'غير متاح'}")]
    if mem_current is not None and mem_limit:
        lines.append(f"🧠 RAM الحاوية: {mem_current / 1048576:.0f}/{mem_limit / 1048576:.0f} MiB")
    else:
        lines.append("🧠 RAM الحاوية: حد cgroup غير متاح")
    if rss is not None: lines.append(f"   ذاكرة عملية البوت RSS: {rss / 1048576:.0f} MiB")
    paths = [("تنزيلات /tmp", tempfile.gettempdir())]
    data_dir = os.path.dirname(os.path.abspath(DB_PATH)) or "."
    if os.path.isdir(data_dir) and os.path.realpath(data_dir) != os.path.realpath(tempfile.gettempdir()):
        paths.append(("بيانات DB", data_dir))
    for label, path in paths:
        try:
            disk = shutil.disk_usage(path)
            lines.append(f"💾 {label}: متاح {disk.free / 1073741824:.2f}/{disk.total / 1073741824:.2f} GiB")
        except OSError:
            lines.append(f"💾 {label}: غير متاح")
    lines.append(f"⏱ مدة تشغيل العملية: {uptime_text}")
    return "\n".join(lines)

_speed_running = False

async def speed_test(cq):
    """يقيس سرعة التحميل من النت وسرعة الرفع لتليجرام من السيرفر اللي البوت شغال عليه."""
    global _speed_running
    if _speed_running:
        return await cq.answer("الاختبار شغال دلوقتي", show_alert=True)
    _speed_running = True
    msg, path = cq.message, None
    again = KB([B("🔄 اختبار تاني", "ad|sp", S.SUCCESS)], [B("🔙 رجوع", "ad")])
    try:
        await safe_edit(msg, "⚡ عينة تنزيل من Cloudflare: 4 اتصالات × 25 MiB...")
        try:
            dl = await asyncio.to_thread(speed_download_test)
            dl_txt = f"{dl:.1f} MB/s (≈ {dl*8:.0f} Mbps)"
        except Exception as e:
            dl, dl_txt = None, f"فشل ({clean_err(e)[:40]})"
        await safe_edit(msg, f"⬇️ Cloudflare: {dl_txt}\n\n⚡ عينة رفع إلى Telegram (40 MiB)...")
        fd, path = tempfile.mkstemp(prefix="speed_", suffix=".bin")
        with os.fdopen(fd, "wb") as f:
            for _ in range(40): f.write(os.urandom(1 << 20))
        t = time.monotonic()
        m = await app.send_document(msg.chat.id, path, caption="speed test")
        up = 40 * 1.048576 / max(time.monotonic() - t, 0.001)
        try: await m.delete()
        except Exception: pass
        verdict = ("🟢 ممتازة" if up >= 15 else "🟡 كويسة" if up >= 6 else "🔴 بطيئة — جرّب region تاني أو VPS قريب من تليجرام")
        await safe_edit(msg, f"⚡ قياس عينة نقل من الخادم الحالي\n\n⬇️ Cloudflare (4×25 MiB): {dl_txt}\n"
                             f"⬆️ Telegram (40 MiB): {up:.1f} MB/s (≈ {up*8:.0f} Mbps)\n\n"
                             f"الرفع: {verdict}\n(رفع متوازي: {UPLOAD_WORKERS if UPLOAD_TUNED else 4})\n\n"
                             "⚠️ هذه عينة وقتية وليست أقصى سعة للبوت أو سرعة YouTube؛ تختلف حسب الشبكة والخادم.", again)
    except Exception as e:
        await safe_edit(msg, f"❌ الاختبار فشل: {clean_err(e)}", again)
    finally:
        _speed_running = False
        if path and os.path.exists(path): os.remove(path)

async def broadcast(admin_cq, chat_id, msg_id):
    ids = [r[0] for r in db_exec("SELECT id FROM users")]
    ok = bad = 0
    for u in ids:
        for _ in range(2):
            try:
                await app.copy_message(u, chat_id, msg_id); ok += 1; break
            except FloodWait as e:
                await asyncio.sleep(e.value + 1)
            except Exception:
                bad += 1; break
        await asyncio.sleep(0.05)
    await safe_edit(admin_cq.message, f"📢 خلصت الإذاعة\n✅ وصلت: {ok}\n❌ فشلت: {bad}", admin_kb())

# ───────────── شاشات إعدادات الأدمن ─────────────
def cf_text():
    access = f"👥 الوصول: {'🌍 عام (أي حد)' if cfg('public') == '1' else '🔒 خاص (الأدمن + المسموح لهم بس)'}"
    return ("⚙️ إعدادات البوت\n\n"
            "📥 عدد التنزيلات: ♾ بلا حد يومي\n"
            "⏱ مدة الفيديو المسجل: ♾ بلا حد\n"
            f"{access}\n\n"
            "طلبات وروابط ودفعات متعددة متزامنة بلا سقف تطبيقي.\n"
            "للوصول الخاص: /allow <id> و /unallow <id> لإضافة/شيل مستخدم.")

def cf_kb():
    pub = cfg("public") == "1"
    return KB(
        [B("🌍 عام — اضغط لتخليه خاص" if pub else "🔒 خاص — اضغط لتخليه عام", "ad|cf|pub",
           S.SUCCESS if pub else S.DANGER)],
        [B("🔙 رجوع", "ad")],
    )

def wm_screen_text():
    t = cfg("wm_type")
    return ("💧 العلامة المائية (على الفيديوهات)\n\n"
            f"الحالة: {'✅ شغّالة' if cfg('wm_on') == '1' else '⛔ مقفولة'}\n"
            f"النوع: {'✍️ نص' if t == 'text' else '🖼 صورة'}\n"
            f"النص: {cfg('wm_text') or '— لسه'}\n"
            f"الصورة: {'✅ مرفوعة' if os.path.exists(WM_IMG_PATH) else '— لسه'}\n"
            f"المكان: {WM_POS_LABEL[cfg('wm_pos')]}   الحجم: {WM_SIZE_LABEL[cfg('wm_size')]}\n\n"
            "⚠️ لما تكون شغّالة، كل فيديو بيتعاد ترميزه (وقت ومعالج أكتر). "
            "الصوت MP3 والصور والألبومات من غير علامة.\n"
            "💡 للكتابة بالعربي استخدم علامة صورة (النص بيطلع بحروف مفصولة).")

def wm_kb():
    on, t, pos, sz = cfg("wm_on") == "1", cfg("wm_type"), cfg("wm_pos"), cfg("wm_size")
    def mk(label, sel, data):
        return B(("✅ " if sel else "") + label, data, S.SUCCESS if sel else S.PRIMARY)
    return KB(
        [B("⛔ إيقاف العلامة" if on else "✅ تفعيل العلامة", "ad|wm|tg", S.DANGER if on else S.SUCCESS)],
        [mk("✍️ نص", t == "text", "ad|wm|ty|text"), mk("🖼 صورة", t == "image", "ad|wm|ty|image")],
        [B("✏️ تغيير النص", "ad|wm|tx"), B("🖼 رفع صورة", "ad|wm|im")],
        [mk("↖️", pos == "tl", "ad|wm|ps|tl"), mk("↗️", pos == "tr", "ad|wm|ps|tr")],
        [mk("↙️", pos == "bl", "ad|wm|ps|bl"), mk("↘️", pos == "br", "ad|wm|ps|br")],
        [mk("⏺ المنتصف", pos == "c", "ad|wm|ps|c")],
        [mk("صغير", sz == "S", "ad|wm|sz|S"), mk("وسط", sz == "M", "ad|wm|sz|M"), mk("كبير", sz == "L", "ad|wm|sz|L")],
        [B("🔙 رجوع", "ad")],
    )

def ck_screen_text():
    if has_cookies():
        try:
            total, yt, login = cookies_info(COOKIE_FILE)
            age = int((time.time() - os.path.getmtime(COOKIE_FILE)) / 3600)
            status = (f"✅ موجودة: {total} كوكي ({yt} ليوتيوب/جوجل)\n"
                      + ("🔐 فيها جلسة تسجيل دخول" if login else "⚠️ مفيش جلسة تسجيل دخول — غالبًا مش هتفيد")
                      + f"\n🕒 آخر تحديث من {age} ساعة")
        except Exception as e:
            status = f"⚠️ الملف موجود لكن مش مقروء: {str(e)[:60]}"
    else:
        status = "❌ مفيش كوكيز دلوقتي"
    return ("🍪 كوكيز يوتيوب\n\n" + status + "\n\n"
            "ليه محتاجها؟ يوتيوب بيحجب IP الاستضافات ويطلب «تأكيد إنك مش روبوت». الكوكيز بتحل ده غالبًا.\n\n"
            "إزاي أجيبها:\n"
            "1️⃣ افتح نافذة متصفح خاصة (Incognito) وسجّل دخول بحساب جوجل ثانوي (مش حسابك الأساسي)\n"
            "2️⃣ افتح youtube.com/robots.txt في نفس النافذة\n"
            "3️⃣ صدّر الكوكيز بصيغة Netscape بإضافة «Get cookies.txt LOCALLY»\n"
            "4️⃣ اقفل النافذة الخاصة، وابعت الملف هنا 📤")

def ck_kb():
    rows = [[B("📤 رفع كوكيز", "ad|ck|up", S.SUCCESS)]]
    if has_cookies(): rows.append([B("🗑 حذف الكوكيز", "ad|ck|rm", S.DANGER)])
    rows.append([B("🔙 رجوع", "ad")])
    return KB(*rows)

def pe_screen_text():
    state = "✅ شغّال" if pe_active() else ("⚠️ اتعطّل (تليجرام رفضه — غالبًا صاحب البوت من غير Premium)" if PE["broken"] else "⛔ متوقف")
    keys = " ".join(PE["map"]) or "لسه مفيش"
    return ("✨ إيموجي بريميوم\n\n"
            f"الحالة: {state}\nالمربوط ({len(PE['map'])}): {keys}\n\n"
            "شروط تليجرام: يظهر متحرك لمشتركي Premium، ولازم حساب صاحب البوت (اللي أنشأه من BotFather) "
            "يكون Premium، أو البوت يكون عنده يوزر مشترى من Fragment. غير كده بيظهر الإيموجي العادي.\n\n"
            "الإيموجي اللي البوت بيستخدمه: 🚀 ⚡ ⏱ ⏹ ✅ ❌ ⏳ 🏁 ➕ 📊 ⚙️ 🌐 ❓ 🏠 🛠 🎬 📥 🍪\n"
            "اضغط «إضافة» وابعتلي من حسابك (لازم Premium) رسالة فيها الإيموجي البريميوم اللي عايزه، "
            "وأنا بربط كل واحد بالإيموجي العادي المقابل له.")

def pe_kb():
    return KB([B("➕ إضافة إيموجي", "ad|pe|add", S.SUCCESS)],
              [B("⛔ إيقاف" if cfg("pe_on") == "1" else "✅ تشغيل", "ad|pe|tg")],
              [B("🗑 مسح الكل", "ad|pe|rm", S.DANGER)],
              [B("🔙 رجوع", "ad")])

async def admin_input(m, uid, st):
    """ردود الأدمن على أسئلة لوحة الإعدادات (رقم / نص / صورة)."""
    awaiting.pop(uid, None)
    txt = " ".join((m.text or "").split())
    if st == "ck":
        raw = None
        try:
            if m.document:
                if (m.document.file_size or 0) > 2 * 1024 * 1024:
                    awaiting[uid] = st
                    return await m.reply_text("❌ الملف كبير؛ ملف الكوكيز لازم يكون أقل من 2MB.")
                src = await app.download_media(m, file_name=os.path.join(tempfile.gettempdir(), f"dl_ck_{uuid.uuid4().hex[:6]}.txt"))
                try:
                    with open(src, "r", encoding="utf-8", errors="ignore") as f: raw = f.read()
                finally:
                    try: os.remove(src)
                    except OSError: pass
            else:
                raw = m.text
        except Exception as e:
            log.info("cookies download failed: %s", e)
        ok, msg = await asyncio.to_thread(save_cookies_text, raw)
        if not ok:
            awaiting[uid] = st
            return await m.reply_text("❌ " + msg + "\nابعت ملف cookies.txt صحيح، أو اضغط رجوع من اللوحة.")
        try: await m.delete()      # الكوكيز حساسة: نمسح رسالة الملف من الشات
        except Exception: pass
        return await m.reply_text("✅ اتحفظت الكوكيز: " + msg + "\n\n" + ck_screen_text(), reply_markup=ck_kb())
    if st == "pe":
        text = m.text or m.caption or ""
        u16 = text.encode("utf-16-le")
        found = {}
        for e in (m.entities or m.caption_entities or []):
            if e.type == MessageEntityType.CUSTOM_EMOJI and e.custom_emoji_id:
                fb = _norm_emoji(u16[e.offset * 2:(e.offset + e.length) * 2].decode("utf-16-le", "ignore"))
                if fb: found[fb] = e.custom_emoji_id
        if not found:
            awaiting[uid] = st
            return await m.reply_text("❌ مفيش إيموجي بريميوم في الرسالة. ابعت رسالة فيها إيموجي بريميوم (من حساب Premium).")
        try: cur = json.loads(cfg("pe_map") or "{}")
        except ValueError: cur = {}
        cur.update(found)
        cfg_set("pe_map", json.dumps(cur, ensure_ascii=False)); cfg_set("pe_on", "1")
        PE["broken"] = False; pe_reload()
        return await m.reply_text(f"✅ اتربط {len(found)} إيموجي: {' '.join(found)}\n\n" + pe_screen_text(), reply_markup=pe_kb())
    if st == "wm_text":
        if not txt or len(txt) > 60:
            awaiting[uid] = st
            return await m.reply_text("ابعت نص من 1 لـ 60 حرف.")
        cfg_set("wm_text", txt); cfg_set("wm_type", "text")
        return await m.reply_text("✅ اتحفظ\n\n" + wm_screen_text(), reply_markup=wm_kb())
    if st == "wm_img":
        doc = m.document if m.document and (m.document.mime_type or "").startswith("image/") else None
        if not (m.photo or doc):
            awaiting[uid] = st
            return await m.reply_text("ابعت صورة (يفضل PNG كملف).")
        src = await app.download_media(m, file_name=os.path.join(tempfile.gettempdir(), f"dl_wm_{uuid.uuid4().hex[:6]}_in"))
        tmp = (src or "") + ".png"
        try:
            if not src: raise RuntimeError("download failed")
            p = await asyncio.create_subprocess_exec(           # بيتأكد إنها صورة سليمة + بيصغّرها لو كبيرة
                "ffmpeg", "-y", "-i", src, "-frames:v", "1", "-vf", "scale='min(800,iw)':-2", tmp,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            await p.wait()
            if p.returncode != 0 or not os.path.exists(tmp): raise RuntimeError("bad image")
            shutil.move(tmp, WM_IMG_PATH)
        except Exception as e:
            log.info("watermark image rejected: %s", e)
            awaiting[uid] = st
            return await m.reply_text("❌ مقدرتش أقرا الصورة دي، جرّب PNG تاني.")
        finally:
            for f in (src, tmp):
                if f and os.path.exists(f):
                    try: os.remove(f)
                    except OSError: pass
        with open(WM_IMG_PATH, "rb") as f: sig = hashlib.sha1(f.read()).hexdigest()[:8]
        cfg_set("wm_img_sig", sig); cfg_set("wm_type", "image")
        return await m.reply_text("✅ اتحفظت الصورة\n\n" + wm_screen_text(), reply_markup=wm_kb())

# ───────────── الهاندلرز ─────────────
@app.on_message(filters.command("start") & filters.private)
async def on_start(_, m):
    if not is_allowed(m.from_user.id):
        return await m.reply_text("🔒 البوت ده خاص. كلّم صاحبه علشان يفعّلك.")
    touch_user(m.from_user.id)
    awaiting.pop(m.from_user.id, None)
    await m.reply_text(pe_html(welcome_text(m.from_user.first_name or "")), reply_markup=menu_kb(m.from_user.id))

@app.on_message(filters.command(["ban", "unban", "allow", "unallow"]) & filters.private)
async def on_ban(_, m):
    if not is_admin(m.from_user.id): return
    cmd = m.command[0].lower()
    try: target = int(m.command[1])
    except (IndexError, ValueError):
        return await m.reply_text(f"الاستخدام: /{cmd} <user_id>")
    if cmd == "ban" and target in ADMIN_IDS:
        return await m.reply_text("مينفعش تحظر أدمن.")
    if cmd == "ban":
        db_exec("INSERT OR IGNORE INTO banned(id) VALUES (?)", (target,)); BANNED.add(target)
        txt = f"🚫 اتحظر: {target}"
    elif cmd == "unban":
        db_exec("DELETE FROM banned WHERE id=?", (target,)); BANNED.discard(target)
        txt = f"✅ اتفك الحظر عن: {target}"
    elif cmd == "allow":
        db_exec("INSERT OR IGNORE INTO allowed(id) VALUES (?)", (target,)); ALLOWED_DB.add(target)
        txt = f"✅ اتضاف للمسموح لهم: {target}"
    else:
        db_exec("DELETE FROM allowed WHERE id=?", (target,)); ALLOWED_DB.discard(target)
        txt = f"➖ اتشال من المسموح لهم: {target}"
    await m.reply_text(txt)

@app.on_message(filters.private & ~filters.command(["start", "ban", "unban", "allow", "unallow"]))
async def on_message(_, m):
    uid = m.from_user.id
    if not is_allowed(uid):
        return await m.reply_text("🔒 البوت ده خاص. كلّم صاحبه علشان يفعّلك.")
    touch_user(uid)

    if awaiting.get(uid) == "bc" and is_admin(uid):
        awaiting.pop(uid)
        bc_pending[uid] = (m.chat.id, m.id)
        n = db_exec("SELECT COUNT(*) FROM users")[0][0]
        return await m.reply_text(f"📢 هتتبعت الرسالة دي لـ {n} مستخدم. تأكيد؟",
                                  reply_markup=KB([B("✅ تأكيد الإرسال", "ad|bc|ok", S.SUCCESS)],
                                                  [B("❌ إلغاء", "ad|bc|no", S.DANGER)]))

    if is_admin(uid) and awaiting.get(uid) in ("wm_text", "wm_img", "ck", "pe"):
        return await admin_input(m, uid, awaiting[uid])

    urls = list(dict.fromkeys(u.rstrip(".,;:!?)]}،؛") for u in URL_RE.findall(m.text or "")))
    if not urls:
        return await m.reply_text("ابعت لينك صحيح 🙂 أو ارجع للقائمة 👇", reply_markup=menu_kb(uid))
    blocked = [u for u in urls if is_blocked(u)]
    urls = [u for u in urls if u not in blocked]
    if not urls:
        return await m.reply_text("🚫 الموقع ده مش مسموح بيه (مخالف لقواعد تليجرام).", reply_markup=back_kb())
    safe = await asyncio.gather(*[asyncio.to_thread(is_safe_url, u) for u in urls])
    urls = [u for u, ok in zip(urls, safe) if ok]
    if not urls:
        return await m.reply_text("❌ الرابط غير صالح.", reply_markup=back_kb())
    wait = await m.reply_text(f"⏳ بفحص {len(urls)} رابط...")
    async def _probe(u):
        hit = await asyncio.to_thread(urlmap_get, u)
        try:
            async with PROBE_SEM:
                result = await asyncio.to_thread(probe, u)
            if hit and len(result[0]) == 1 and result[0][0].engine == "ytdlp":
                result[0][0].key = hit[0]   # احتفظ بمفتاح كاش الملف بعد فحص metadata الجديد
                if not result[0][0].title: result[0][0].title = hit[1] or u
            return result
        except Exception:
            if hit: return [Item(u, hit[1] or u, hit[0])], None
            raise
    results = await asyncio.gather(*[_probe(u) for u in urls], return_exceptions=True)
    items, fails, pl = [], [], None
    for u, r in zip(urls, results):
        if isinstance(r, Exception): fails.append(f"• {u[:40]}: {clean_err(r)[:80]}")
        else:
            items += r[0]; pl = pl or r[1]
            if len(r[0]) == 1 and r[0][0].engine == "ytdlp" and r[0][0].url == u and not r[0][0].key.startswith("sniff:"):
                urlmap_put(u, r[0][0].key, r[0][0].title)
    if not items:
        return await wait.edit_text("❌ مقدرتش أحمّل الرابط:\n" + "\n".join(fails), reply_markup=back_kb())

    b = Batch(id=uuid.uuid4().hex[:8], user_id=uid, chat_id=m.chat.id, items=items, msg=wait)
    batches[b.id] = b

    pref = get_pref(uid)
    only_gallery = all(i.engine == "gallery" for i in items)
    preview_info = items[0].info if len(items) == 1 and items[0].engine == "ytdlp" else None
    preview_sent = False
    if preview_info:
        thumbnail = thumbnail_url(preview_info)
        if isinstance(thumbnail, str) and thumbnail.startswith("https://"):
            try:
                if await asyncio.to_thread(is_safe_url, thumbnail):
                    await app.send_photo(m.chat.id, thumbnail, reply_to_message_id=m.id)
                    preview_sent = True
            except Exception as e:
                log.debug("thumbnail preview unavailable: %s", e)
    summary = metadata_text(items, pl)
    if fails: summary += "\n\n⚠️ روابط لم تنجح:\n" + "\n".join(_escape_metadata_title(f, 120) for f in fails[:3])
    if pref != "ask" or only_gallery:         # جودة افتراضية، أو صور بس (مفيش جودة تتختار)
        b.mode = "best" if only_gallery else pref
        if preview_sent:
            wait = await replace_with_reply(wait, m, summary + f"\n\n⚡ هبدأ الآن بالجودة الافتراضية: {mode_label(b.mode)}", cancel_kb(b))
            b.msg = wait
        elif not only_gallery:
            await safe_edit(wait, summary + f"\n\n⚡ هبدأ الآن بالجودة الافتراضية: {mode_label(b.mode)}", cancel_kb(b))
        else:
            await safe_edit(wait, render(b), cancel_kb(b))
        return await begin(b)

    if preview_sent:
        wait = await replace_with_reply(wait, m, summary, picker_kb(b.id, preview_info))
        b.msg = wait
    else:
        await wait.edit_text(summary, reply_markup=picker_kb(b.id, preview_info))

@app.on_callback_query()
async def on_cb(_, cq):
    uid, d = cq.from_user.id, cq.data.split("|")
    act = d[0]
    if not is_allowed(uid):
        return await cq.answer("🔒 البوت خاص", show_alert=True)
    touch_user(uid)

    if act == "m":
        awaiting.pop(uid, None)
        return await show(cq, welcome_text(cq.from_user.first_name or ""), menu_kb(uid))
    if act == "dl":
        return await show(cq, "📥 ابعتلي لينك الفيديو أو الصور (أو أكتر من لينك في رسالة واحدة) 👇", back_kb())
    if act == "hp":
        return await show(cq, HELP, back_kb())
    if act == "sites":
        return await show(cq, sites_text(), back_kb())

    if act == "me":
        if len(d) > 1 and d[1] == "clr":
            db_exec("DELETE FROM history WHERE user_id=?", (uid,))
        total = db_exec("SELECT downloads FROM users WHERE id=?", (uid,))[0][0]
        rows = db_exec("SELECT title, mode FROM history WHERE user_id=? ORDER BY ts DESC LIMIT 8", (uid,))
        txt = f"📊 تحميلاتك\n\nالإجمالي: {total}\n"
        txt += ("\nآخر تحميلاتك:\n" + "\n".join(f"• {t[:40]} ({mode_label(q) if q != 'gallery' else 'صور'})" for t, q in rows)
                if rows else "\nلسه مفيش تحميلات.")
        kb = KB([B("🗑 مسح السجل", "me|clr", S.DANGER)], [B("🏠 القائمة الرئيسية", "m")]) if rows else back_kb()
        return await show(cq, txt, kb)

    if act == "st":
        if len(d) > 1 and d[1] in VALID_PREFS:
            set_pref(uid, d[1])
            await cq.answer("✅ اتحفظ")
        return await show(cq, f"⚙️ الإعدادات\n\nالجودة الافتراضية: {mode_label(get_pref(uid))}\n"
                              "(لو اخترت جودة، البوت هيبدأ التحميل فورًا من غير ما يسألك)",
                          settings_kb(uid))

    if act == "q":
        b = batches.get(d[1])
        if not b or b.user_id != uid or b.started:
            return await cq.answer("انتهت صلاحية الطلب، ابعت الرابط تاني.", show_alert=True)
        mode = d[2]
        dynamic_resolution = (len(b.items) == 1 and mode.isdigit()
                              and mode in quality_modes(b.items[0].info or {}))
        audio_ok = mode == "audio" and (len(b.items) != 1 or audio_available(b.items[0].info or {}))
        if ((mode not in VALID_PREFS and not dynamic_resolution) or mode == "ask"
                or (mode == "audio" and not audio_ok)):
            return await cq.answer("اختيار غير صالح", show_alert=True)
        b.mode = mode
        b.started = True            # يمنع الضغط المزدوج على الزرار من تشغيل الدفعة مرتين
        await cq.answer("بدأنا 🚀")
        return await begin(b)

    if act == "x":
        b = batches.get(d[1])
        if b and b.user_id == uid and not b.started:
            batches.pop(b.id, None)
        await cq.answer("اتلغى")
        return await show(cq, welcome_text(cq.from_user.first_name or ""), menu_kb(uid))

    if act == "c":
        b = batches.get(d[1])
        if b and b.user_id == uid:
            b.cancelled = True
            return await cq.answer("جاري الإلغاء...")
        return await cq.answer()

    if act == "ad":
        if not is_admin(uid):
            return await cq.answer("للأدمن فقط", show_alert=True)
        sub = d[1] if len(d) > 1 else ""
        if sub == "":
            return await show(cq, "🛠 لوحة الأدمن", admin_kb())
        if sub == "st":
            u = db_exec("SELECT COUNT(*), COALESCE(SUM(downloads),0) FROM users")[0]
            c = db_exec("SELECT COUNT(*) FROM cache")[0][0]
            act24 = db_exec("SELECT COUNT(DISTINCT user_id) FROM history WHERE ts > ?", (int(time.time()) - 86400,))[0][0]
            bn = db_exec("SELECT COUNT(*) FROM banned")[0][0]
            current_batches = list(batches.values())
            pending_batches = sum(not b.started and not b.done for b in current_batches)
            running_batches = sum(b.started and not b.done for b in current_batches)
            downloading = sum(it.status == "dl" for b in current_batches for it in b.items)
            uploading = sum(it.status == "up" for b in current_batches for it in b.items)
            runtime = await asyncio.to_thread(runtime_metrics_text)
            return await show(cq, f"📊 إحصائيات\n\n👥 مستخدمين: {u[0]}\n📥 تحميلات: {u[1]}\n"
                                  f"⚡ نسخ في الكاش: {c}\n⏳ طلبات تنتظر اختيار الجودة: {pending_batches} | "
                                  f"⚙️ دفعات بدأت: {running_batches}\n⬇️ عناصر تنزل الآن: {downloading} | "
                                  f"⬆️ عناصر ترفع الآن: {uploading}\n"
                                  f"👤 نشطين 24س: {act24}  |  🚫 محظورين: {bn}" + chr(10) +
                                  f"🚦 حدود التنزيل: بلا حد  |  الوصول: {'عام' if cfg('public') == '1' else 'خاص'}\n"
                                  f"💧 علامة مائية: {'✅' if cfg('wm_on') == '1' else '⛔'}\n\n"
                                  f"{runtime}\n\n"
                                  f"🧩 aria2c: {'✅' if HAS_ARIA2 else '❌'}  "
                                  f"تقليد المتصفح: {'✅' if IMPERSONATE else '❌'}  "
                                  f"gallery-dl: {'✅' if HAS_GALLERY else '❌'}\n"
                                  f"⚡ uvloop: {'✅' if HAS_UVLOOP else '❌'}  رفع متوازي: {UPLOAD_WORKERS if UPLOAD_TUNED else 4}\n"
                                  f"🎨 ألوان الأزرار: {'✅' if COLORS else '❌'}  🍪 كوكيز: {'✅' if has_cookies() else '❌'}  ✨ بريميوم: {'✅' if pe_active() else '⛔'}",
                              KB([B("🔄 تحديث", "ad|st", S.SUCCESS)], [B("🔙 رجوع", "ad")]))
        if sub == "bc":
            if len(d) == 2:
                awaiting[uid] = "bc"
                return await show(cq, "📢 ابعت دلوقتي الرسالة اللي عايز تذيعها (نص أو صورة أو فيديو...)",
                                  KB([B("❌ إلغاء", "ad|bc|no", S.DANGER)]))
            if d[2] == "no":
                awaiting.pop(uid, None); bc_pending.pop(uid, None)
                return await show(cq, "🛠 لوحة الأدمن", admin_kb())
            if d[2] == "ok":
                p = bc_pending.pop(uid, None)
                if not p: return await cq.answer("مفيش رسالة معلّقة", show_alert=True)
                await safe_edit(cq.message, "📢 جاري الإرسال...")
                return asyncio.create_task(broadcast(cq, *p))
        if sub == "ck":
            awaiting.pop(uid, None)
            a = d[2] if len(d) > 2 else ""
            if a == "up":
                awaiting[uid] = "ck"
                return await show(cq, "📤 ابعتلي دلوقتي ملف cookies.txt (كملف Document)", KB([B("🔙 رجوع", "ad|ck")]))
            if a == "rm":
                for f in (COOKIE_FILE, COOKIE_FILE + ".envsig"):
                    try: os.remove(f)
                    except OSError: pass
                await cq.answer("اتحذفت")
            return await show(cq, ck_screen_text(), ck_kb())
        if sub == "pe":
            awaiting.pop(uid, None)
            a = d[2] if len(d) > 2 else ""
            if a == "add":
                awaiting[uid] = "pe"
                return await show(cq, "✨ ابعتلي رسالة فيها الإيموجي البريميوم (ممكن كذا واحد في رسالة)", KB([B("🔙 رجوع", "ad|pe")]))
            if a == "tg":
                if not PE["map"]: return await cq.answer("ضيف إيموجي الأول", show_alert=True)
                cfg_set("pe_on", "0" if cfg("pe_on") == "1" else "1"); PE["broken"] = False; pe_reload()
            if a == "rm":
                cfg_set("pe_map", "{}"); cfg_set("pe_on", "0"); PE["broken"] = False; pe_reload()
            return await show(cq, pe_screen_text(), pe_kb())
        if sub == "cf":
            awaiting.pop(uid, None)
            a = d[2] if len(d) > 2 else ""
            if a == "pub":
                cfg_set("public", "0" if cfg("public") == "1" else "1")
            return await show(cq, cf_text(), cf_kb())
        if sub == "wm":
            awaiting.pop(uid, None)
            a, v = (d[2] if len(d) > 2 else ""), (d[3] if len(d) > 3 else "")
            if a == "tg":
                if cfg("wm_on") != "1" and wm_missing():
                    return await cq.answer(wm_missing(), show_alert=True)
                cfg_set("wm_on", "0" if cfg("wm_on") == "1" else "1")
            elif a == "ty" and v in ("text", "image"):
                cfg_set("wm_type", v)
                if cfg("wm_on") == "1" and wm_missing():
                    cfg_set("wm_on", "0")
                    await cq.answer(wm_missing() + " — العلامة اتقفلت لحد ما تكمّل", show_alert=True)
            elif a == "ps" and v in WM_POS_LABEL:
                cfg_set("wm_pos", v)
            elif a == "sz" and v in WM_SIZES:
                cfg_set("wm_size", v)
            elif a == "tx":
                awaiting[uid] = "wm_text"
                return await show(cq, "✏️ ابعت نص العلامة (لحد 60 حرف)، مثلًا @channel\n"
                                      "⚠️ العربي بيطلع بحروف مفصولة — للعربي استخدم صورة.", KB([B("🔙 رجوع", "ad|wm")]))
            elif a == "im":
                awaiting[uid] = "wm_img"
                return await show(cq, "🖼 ابعت صورة العلامة. الأفضل PNG بخلفية شفافة وتتبعت كـ ملف (File) "
                                      "مش كصورة عادية، عشان الشفافية ما تضيعش.", KB([B("🔙 رجوع", "ad|wm")]))
            return await show(cq, wm_screen_text(), wm_kb())
        if sub == "sp":
            await cq.answer("بدأ الاختبار ⚡")
            return asyncio.create_task(speed_test(cq))
        if sub == "cc":
            if len(d) == 2:
                return await show(cq, "🗑 متأكد إنك عايز تمسح الكاش كله؟ الفيديوهات هتتحمل من الأول.",
                                  KB([B("✅ أيوه امسح", "ad|cc|ok", S.DANGER)], [B("🔙 لأ، رجوع", "ad")]))
            db_exec("DELETE FROM cache")
            await cq.answer("✅ اتمسح")
            return await show(cq, "🛠 لوحة الأدمن", admin_kb())
    await cq.answer()

# ───────────── التشغيل ─────────────
async def main():
    # الافتراضي (cpu+4 خيوط) كان بيحدّد كام فيديو بيتحمّلوا فعلاً مع بعض؛ نكبّره عشان التحميل المتزامن يبقى حقيقي
    asyncio.get_running_loop().set_default_executor(
        ThreadPoolExecutor(max_workers=int(os.getenv("THREADS", "128")), thread_name_prefix="bot"))
    pe_reload()
    clean_stale_tmp(max_age=0)      # أي بقايا من تشغيل سابق (البوت لسه مبدأش يحمّل حاجة)
    await app.start()
    try: await app.delete_bot_commands()
    except Exception as e: log.info("delete_bot_commands: %s", e)
    log.info("sites: %s | aria2c: %s | impersonate: %s | gallery-dl: %s | uvloop: %s | upload-workers: %s | colors: %s | db: %s | cookies: %s | proxy: %s",
             SITE_COUNT, HAS_ARIA2, bool(IMPERSONATE), HAS_GALLERY, HAS_UVLOOP,
             UPLOAD_WORKERS if UPLOAD_TUNED else 4, COLORS, DB_PATH, has_cookies(), bool(PROXY))
    await idle()
    await app.stop()

if __name__ == "__main__":
    # في kurigram الحديثة app.run() مبقاش بياخد coroutine؛ بنشغّل main على نفس لوب العميل (اللي اتسجّلت عليه الهاندلرز)
    app.loop.run_until_complete(main())
