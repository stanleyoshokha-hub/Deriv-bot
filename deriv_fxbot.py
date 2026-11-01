# -*- coding: utf-8 -*-
"""
FOREX CANDLESTICK SIGNAL BOT  (Deriv WebSocket -> Telegram)

* 1-minute candles, 7 major pairs, 3-minute expiry signals
* Candlestick patterns ONLY (no indicators). Trend = pure price structure
  (swing highs / swing lows + candle drift).
* Quality over quantity: trend filter, location filter, score threshold,
  per-pair cooldown, hourly cap, best-signal-per-minute.

Install in Pydroid 3 (menu -> Pip):  websocket-client   requests
"""
import json
import time
import threading
import traceback
import datetime

import requests
import websocket  # pip package name: websocket-client

# ---- SETTINGS ----
BOT_TOKEN = "9018907853:AAFmi8DSO25wMtVVkJNjbtsf_iLAtKFlPf0"   # from @BotFather
CHAT_ID = "8306361825"        # leave empty = auto-detect (send /start to your bot first)

PAIRS = {
    "EUR/USD": "frxEURUSD",
    "GBP/USD": "frxGBPUSD",
    "USD/JPY": "frxUSDJPY",
    "USD/CHF": "frxUSDCHF",
    "AUD/USD": "frxAUDUSD",
    "USD/CAD": "frxUSDCAD",
    "NZD/USD": "frxNZDUSD",
}

EXPIRY_MIN = 3             # expiry shown in the signal (minutes)

# "with_trend": only signals in the direction of the MAJOR trend
#               (pullback-end reversals + continuations)  <- recommended
# "all":        also allows counter-trend reversals
MODE = "with_trend"

MIN_SCORE = 2              # raise to 3 or 4 for even fewer, stronger signals
COOLDOWN_MIN = 6           # minutes between two signals on the same pair
MAX_SIGNALS_PER_HOUR = 6   # global cap
MAX_SIGNALS_PER_MINUTE = 1 # only the best setup of each minute is sent
COLLECT_SEC = 2.0          # wait for all pairs to close before choosing best

MAX_SPIKE = 3.0            # skip if last candle > 3x average range (news spike)
MIN_RANGE_RATIO = 0.00004  # skip dead market  (avg candle range / price)
MAX_RANGE_RATIO = 0.0006   # skip wild market

SEND_RESULTS = True        # send WIN / LOSS after expiry (to measure quality)
HISTORY = 150              # candles loaded at start

ENDPOINTS = [
    "wss://api.derivws.com/trading/v1/options/ws/public",   # new public API
    "wss://ws.derivws.com/websockets/v3?app_id=1089",        # legacy fallback
]

# ---- CANDLE MODEL ----
class C:
    __slots__ = ("t", "o", "h", "l", "c")

    def __init__(self, t, o, h, l, c):
        self.t, self.o, self.h, self.l, self.c = t, o, h, l, c

    body = property(lambda s: abs(s.c - s.o))
    rng = property(lambda s: s.h - s.l)
    top = property(lambda s: max(s.o, s.c))
    bot = property(lambda s: min(s.o, s.c))
    uw = property(lambda s: s.h - max(s.o, s.c))       # upper wick
    lw = property(lambda s: min(s.o, s.c) - s.l)       # lower wick
    bull = property(lambda s: s.c > s.o)
    bear = property(lambda s: s.c < s.o)

    def mirror(self):
        """Flip the chart upside-down: bullish <-> bearish."""
        return C(self.t, -self.o, -self.l, -self.h, -self.c)

# Bullish name -> its bearish mirror
BEAR = {
    "Hammer": "Shooting Star",
    "Inverted Hammer": "Hanging Man",
    "Dragonfly Doji": "Gravestone Doji",
    "Bullish Marubozu": "Bearish Marubozu",
    "Bullish Engulfing": "Bearish Engulfing",
    "Piercing Line": "Dark Cloud Cover",
    "Bullish Harami": "Bearish Harami",
    "Bullish Harami Cross": "Bearish Harami Cross",
    "Tweezer Bottom": "Tweezer Top",
    "Bullish Kicker": "Bearish Kicker",
    "Bullish Counterattack": "Bearish Counterattack",
    "Morning Star": "Evening Star",
    "Morning Doji Star": "Evening Doji Star",
    "Three White Soldiers": "Three Black Crows",
    "Three Inside Up": "Three Inside Down",
    "Three Outside Up": "Three Outside Down",
    "Bullish Abandoned Baby": "Bearish Abandoned Baby",
    "Rising Three Methods": "Falling Three Methods",
    "Upside Tasuki Gap": "Downside Tasuki Gap",
    "Bullish Separating Lines": "Bearish Separating Lines",
    "Upside Gap Three Methods": "Downside Gap Three Methods",
}

# ---- CANDLESTICK PATTERN DETECTION | Only BULLISH patterns are coded. Bearish ones are found by running | the same code on a mirrored (upside-down) chart. | Returns list of (name, kind, weight, candles_used) | kind: "rev" = reversal, "cont" = continuation ----
def bull_patterns(k, ar):
    out = []
    c0, c1, c2, c3, c4 = k[-1], k[-2], k[-3], k[-4], k[-5]
    T = 0.10 * ar          # "equal" tolerance
    G = 0.05 * ar          # minimum gap size (forex 1M gaps are tiny)

    def big(c):
        return c.body >= 0.6 * ar

    def doji(c):
        return c.rng > 0 and c.body <= 0.1 * c.rng

    def mid(c):
        return (c.o + c.c) / 2.0

    def add(name, kind, w, span):
        out.append((name, kind, w, span))

    # ---------------- single candle (reversal) ----------------
    r = c0.rng
    if r > 0 and r >= 0.8 * ar:
        if (c0.body > 0.1 * r and c0.lw >= 2 * c0.body
                and c0.lw >= 0.6 * r and c0.uw <= 0.1 * r):
            add("Hammer", "rev", 2, 1)
        if doji(c0) and c0.lw >= 0.65 * r and c0.uw <= 0.1 * r:
            add("Dragonfly Doji", "rev", 2, 1)
        if (c0.body > 0.1 * r and c0.uw >= 2 * c0.body
                and c0.uw >= 0.6 * r and c0.lw <= 0.1 * r):
            add("Inverted Hammer", "rev", 1, 1)
    if c0.bull and r > 0 and c0.body >= 0.85 * r and c0.body >= ar:
        add("Bullish Marubozu", "rev", 2, 1)

    # ---------------- two candles (reversal) ----------------
    if c1.bear and c0.bull:
        if (c0.o <= c1.c and c0.c >= c1.o and c0.body > c1.body and big(c0)):
            add("Bullish Engulfing", "rev", 3, 2)
        if (big(c1) and c0.o <= c1.c and mid(c1) < c0.c < c1.o
                and c0.body >= 0.5 * ar):
            add("Piercing Line", "rev", 3, 2)
        if (abs(c0.l - c1.l) <= T and c0.body >= 0.3 * ar
                and c1.body >= 0.3 * ar):
            add("Tweezer Bottom", "rev", 2, 2)
        if big(c1) and big(c0) and c0.o >= c1.o - T:
            add("Bullish Kicker", "rev", 3, 2)
        if (big(c1) and big(c0) and c0.o <= c1.c - G
                and abs(c0.c - c1.c) <= T):
            add("Bullish Counterattack", "rev", 2, 2)
    if (c1.bear and big(c1) and c0.top <= c1.top and c0.bot >= c1.bot
            and c0.body <= 0.5 * c1.body):
        if doji(c0):
            add("Bullish Harami Cross", "rev", 2, 2)
        elif c0.bull:
            add("Bullish Harami", "rev", 1, 2)

    # ---------------- three candles (reversal) ----------------
    if (c2.bear and big(c2) and c1.body <= 0.4 * c2.body
            and c1.top <= c2.c + T and c0.bull and big(c0)
            and c0.c > mid(c2)):
        add("Morning Doji Star" if doji(c1) else "Morning Star",
            "rev", 3, 3)
    if (all(c.bull and c.body >= 0.5 * ar and c.uw <= 0.3 * c.body
            for c in (c2, c1, c0))
            and c2.c < c1.c < c0.c
            and c2.bot <= c1.o <= c2.top and c1.bot <= c0.o <= c1.top):
        add("Three White Soldiers", "rev", 3, 3)
    if (c2.bear and big(c2) and c1.bull and c1.top <= c2.top
            and c1.bot >= c2.bot and c0.bull and c0.c > c2.o):
        add("Three Inside Up", "rev", 2, 3)
    if (c2.bear and c1.bull and c1.o <= c2.c and c1.c >= c2.o
            and c1.body > c2.body and c0.bull and c0.c > c1.c):
        add("Three Outside Up", "rev", 3, 3)
    if (c2.bear and big(c2) and doji(c1) and c1.h <= c2.l + G
            and c0.bull and big(c0) and c0.l >= c1.h - G):
        add("Bullish Abandoned Baby", "rev", 3, 3)

    # ---------------- continuation ----------------
    mids = (c3, c2, c1)
    if (c4.bull and c4.body >= 0.8 * ar
            and all(c.body <= 0.6 * c4.body and c.h <= c4.h and c.l >= c4.l
                    for c in mids)
            and sum(1 for c in mids if c.bear) >= 2
            and c0.bull and big(c0) and c0.c > c4.c):
        add("Rising Three Methods", "cont", 3, 5)
    if (c2.bull and c1.bull and big(c1) and c1.o >= c2.c + G
            and c0.bear and c1.bot <= c0.o <= c1.top
            and c2.c < c0.c < c1.o):
        add("Upside Tasuki Gap", "cont", 2, 3)
    if c1.bear and big(c1) and c0.bull and big(c0) and abs(c0.o - c1.o) <= T:
        add("Bullish Separating Lines", "cont", 2, 2)
    if (c2.bull and c1.bull and c1.o >= c2.c + G and c0.bear
            and c1.bot <= c0.o <= c1.top and c2.o <= c0.c <= c2.c):
        add("Upside Gap Three Methods", "cont", 2, 3)
    return out

# ---- TREND (pure price structure, no indicators) ----
def pivots(cs, k):
    highs, lows = [], []
    for i in range(k, len(cs) - k):
        if all(cs[i].h > cs[i - j].h and cs[i].h >= cs[i + j].h
               for j in range(1, k + 1)):
            highs.append(cs[i].h)
        if all(cs[i].l < cs[i - j].l and cs[i].l <= cs[i + j].l
               for j in range(1, k + 1)):
            lows.append(cs[i].l)
    return highs, lows

def structure(cs, k):
    """+1 = higher highs & higher lows, -1 = lower highs & lower lows."""
    hi, lo = pivots(cs, k)
    if len(hi) < 2 or len(lo) < 2:
        return 0
    if hi[-1] > hi[-2] and lo[-1] > lo[-2]:
        return 1
    if hi[-1] < hi[-2] and lo[-1] < lo[-2]:
        return -1
    return 0

def major_trend(cs, ar):
    w = cs[-90:]
    if len(w) < 90:
        return 0
    s = structure(w, 4)
    if s == 0:
        return 0
    net = w[-1].c - w[-40].o
    return s if net * s >= 3 * ar else 0

def drift(cs, ar, n=8):
    """Short-term push: +1 up, -1 down, 0 none (candle based)."""
    w = cs[-n:]
    net = w[-1].c - w[0].o
    if abs(net) < 1.2 * ar:
        return 0
    d = 1 if net > 0 else -1
    agree = sum(1 for c in w if (c.c - c.o) * d > 0)
    return d if agree >= 5 else 0

# ---- SIGNAL ENGINE ----
def analyze(sym, cs):
    last = cs[-1]
    ar = sum(c.rng for c in cs[-21:-1]) / 20.0
    price = last.c
    if ar <= 0 or not (MIN_RANGE_RATIO <= ar / price <= MAX_RANGE_RATIO):
        return None
    if last.rng > MAX_SPIKE * ar:
        return None
    major = major_trend(cs, ar)
    if MODE == "with_trend" and major == 0:
        return None

    sides = {}
    for s in (1, -1):
        fr = list(cs[-100:]) if s == 1 else [c.mirror() for c in cs[-100:]]
        score, names, kinds = 0, [], set()
        for name, kind, w, span in bull_patterns(fr[-5:], ar):
            pre = fr[:-span]
            m = drift(pre, ar)
            if kind == "rev":
                if m != -1:                      # need prior push against it
                    continue
                low_all = min(c.l for c in fr[-20:])
                low_pat = min(c.l for c in fr[-span:])
                if low_pat - low_all > 0.75 * ar:  # must sit at the extreme
                    continue
            else:
                if m != 1:                       # continuation needs trend
                    continue
            if MODE == "with_trend" and major != s:
                continue
            score += w
            names.append(BEAR[name] if s == -1 else name)
            kinds.add(kind)
        sides[s] = (score, names, kinds)

    s = 1 if sides[1][0] > sides[-1][0] else -1
    score, names, kinds = sides[s]
    if score < MIN_SCORE or score - sides[-s][0] < 2:
        return None
    return {"sym": sym, "dir": "CALL" if s == 1 else "PUT", "score": score,
            "names": names, "kinds": kinds, "major": major}

# ---- TELEGRAM ----
def tg(text):
    def _send():
        for _ in range(3):
            try:
                r = requests.post(
                    "https://api.telegram.org/bot%s/sendMessage" % BOT_TOKEN,
                    data={"chat_id": CHAT_ID, "text": text,
                          "parse_mode": "HTML"}, timeout=10)
                if r.ok:
                    return
                print("Telegram error:", r.text)
            except Exception as e:
                print("Telegram exception:", e)
            time.sleep(2)
    threading.Thread(target=_send, daemon=True).start()

def detect_chat_id():
    print(">>> Open Telegram and send /start to your bot now...")
    while True:
        try:
            r = requests.get(
                "https://api.telegram.org/bot%s/getUpdates" % BOT_TOKEN,
                timeout=15).json()
            for u in reversed(r.get("result", [])):
                m = u.get("message") or {}
                if "chat" in m:
                    print(">>> Chat ID found:", m["chat"]["id"])
                    return str(m["chat"]["id"])
        except Exception as e:
            print("getUpdates error:", e)
        time.sleep(3)

def pname(code):
    for n, c in PAIRS.items():
        if c == code:
            return n
    return code

def fmt(code, p):
    return ("%.3f" if "JPY" in code else "%.5f") % p

def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S")

def build_msg(sg):
    up = sg["dir"] == "CALL"
    head = "🟢 <b>CALL (BUY ⬆️)</b>" if up else "🔴 <b>PUT (SELL ⬇️)</b>"
    kinds = sg["kinds"]
    if kinds == {"cont"}:
        kind = "Trend continuation"
    elif kinds == {"rev"}:
        kind = "Reversal setup"
    else:
        kind = "Reversal + continuation"
    trend = {1: "UP ⬆️", -1: "DOWN ⬇️", 0: "Mixed"}[sg["major"]]
    stars = "⭐" * min(3, max(1, (sg["score"] + 1) // 2))
    return (
        "%s — <b>%s</b>\n"
        "⏱ Expiry: <b>%d min</b> | TF: 1M\n"
        "📍 Entry: <b>NOW</b> @ %s\n"
        "🕯 Setup: %s\n"
        "🧭 Type: %s | Major trend: %s\n"
        "%s Strength: %d\n"
        "🕐 %s UTC"
    ) % (head, pname(sg["sym"]), EXPIRY_MIN, fmt(sg["sym"], sg["entry"]),
         " + ".join(sg["names"]), kind, trend, stars, sg["score"], utc_now())

# ---- SIGNAL QUEUE / ANTI-SPAM / RESULTS ----
lock = threading.Lock()
pending = {}      # minute -> list of signals
sent_times = []
last_sig = {}
results = []
stats = {"WIN": 0, "LOSS": 0, "DRAW": 0}
state = {}
conn = {"got_data": False}

def queue_signal(sg, minute):
    with lock:
        first = minute not in pending
        pending.setdefault(minute, []).append(sg)
    if first:
        threading.Timer(COLLECT_SEC, flush, args=(minute,)).start()

def flush(minute):
    with lock:
        sigs = pending.pop(minute, [])
    sigs.sort(key=lambda x: -x["score"])
    sent = 0
    for sg in sigs:
        if sent >= MAX_SIGNALS_PER_MINUTE:
            break
        now = time.time()
        with lock:
            sent_times[:] = [t for t in sent_times if now - t < 3600]
            if len(sent_times) >= MAX_SIGNALS_PER_HOUR:
                return
            if now - last_sig.get(sg["sym"], 0) < COOLDOWN_MIN * 60:
                continue
            cur = state.get(sg["sym"], {}).get("cur")
            sg["entry"] = cur.c if cur else 0.0
            sent_times.append(now)
            last_sig[sg["sym"]] = now
            if SEND_RESULTS:
                results.append({"sym": sg["sym"], "dir": sg["dir"],
                                "entry": sg["entry"],
                                "exp": now + EXPIRY_MIN * 60})
        print("[%s] SIGNAL %s %s %s" % (utc_now(), sg["dir"],
                                        pname(sg["sym"]), sg["names"]))
        tg(build_msg(sg))
        sent += 1

def check_results(sym, price):
    now = time.time()
    done = []
    with lock:
        for r in results:
            if r["sym"] == sym and now >= r["exp"]:
                done.append(r)
        for r in done:
            results.remove(r)
    for r in done:
        if price == r["entry"]:
            res = "DRAW"
        elif (price > r["entry"]) == (r["dir"] == "CALL"):
            res = "WIN"
        else:
            res = "LOSS"
        with lock:
            stats[res] += 1
            w, l, d = stats["WIN"], stats["LOSS"], stats["DRAW"]
        icon = {"WIN": "✅", "LOSS": "❌", "DRAW": "➖"}[res]
        tg("%s <b>%s</b> — %s %s\nEntry %s → Close %s\n"
           "Session: %dW / %dL / %dD" % (
               icon, res, pname(sym), r["dir"], fmt(sym, r["entry"]),
               fmt(sym, price), w, l, d))

# ---- DERIV WEBSOCKET ----
def on_new_candle(sym, newc):
    cs = state[sym]["cs"]
    if len(cs) < 100:
        return
    sg = analyze(sym, cs)
    if sg:
        queue_signal(sg, newc.t)

def on_open(ws):
    print("[%s] Connected. Loading candles..." % utc_now())
    conn["got_data"] = False
    for code in PAIRS.values():
        state[code] = {"cs": [], "cur": None}
        ws.send(json.dumps({
            "ticks_history": code, "adjust_start_time": 1,
            "count": HISTORY, "end": "latest", "start": 1,
            "style": "candles", "granularity": 60, "subscribe": 1}))

def on_message(ws, message):
    try:
        d = json.loads(message)
        if "error" in d:
            print("API error:", d["error"].get("message"))
            return
        t = d.get("msg_type")
        if t == "candles":
            code = d["echo_req"]["ticks_history"]
            arr = [C(int(x["epoch"]), float(x["open"]), float(x["high"]),
                     float(x["low"]), float(x["close"]))
                   for x in d.get("candles", [])]
            if len(arr) < 2:
                return
            state[code]["cs"] = arr[:-1]
            state[code]["cur"] = arr[-1]
            conn["got_data"] = True
            print("Loaded %d candles: %s" % (len(arr), pname(code)))
        elif t == "ohlc":
            o = d["ohlc"]
            code = (o.get("symbol") or o.get("underlying_symbol")
                    or d.get("echo_req", {}).get("ticks_history"))
            if code not in state or state[code]["cur"] is None:
                return
            ot = int(o.get("open_time") or (int(o["epoch"]) // 60 * 60))
            c = C(ot, float(o["open"]), float(o["high"]),
                  float(o["low"]), float(o["close"]))
            st = state[code]
            if ot == st["cur"].t:
                st["cur"] = c
            elif ot > st["cur"].t:
                prev = st["cur"]
                st["cs"].append(prev)
                st["cs"] = st["cs"][-HISTORY:]
                st["cur"] = c
                if ot - prev.t == 60:          # fresh, consecutive candle
                    on_new_candle(code, c)
            if SEND_RESULTS:
                check_results(code, c.c)
    except Exception:
        traceback.print_exc()

def on_error(ws, err):
    print("WS error:", err)

def on_close(ws, code, msg):
    print("WS closed:", code, msg)

def run():
    global CHAT_ID
    if BOT_TOKEN.startswith("PASTE"):
        print("Put your bot token in BOT_TOKEN first.")
        return
    if not CHAT_ID:
        CHAT_ID = detect_chat_id()
    tg("🤖 <b>Forex Candlestick Bot started</b>\n"
       "Pairs: %d | TF: 1M | Expiry: %d min\nMode: %s | Min score: %d\n"
       "Scanning for clean setups..." % (len(PAIRS), EXPIRY_MIN, MODE,
                                          MIN_SCORE))
    idx = 0
    while True:
        url = ENDPOINTS[idx % len(ENDPOINTS)]
        print("Connecting:", url)
        conn["got_data"] = False
        try:
            ws = websocket.WebSocketApp(
                url, on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close)
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception:
            traceback.print_exc()
        if not conn["got_data"]:
            idx += 1                  # try the other endpoint next time
        print("Reconnecting in 5s...")
        time.sleep(5)

if __name__ == "__main__":
    run()
