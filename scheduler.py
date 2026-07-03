"""
PROJECT 1 — Intraday Scanner (Single User / Your Own Alerts)
FILE: scheduler.py (GitHub Actions + Local)

Changes in this version:
  1. get_ltp() fixed — 3-method fallback (fast_info → 1min download → 5min download)
     so EOD prices are real, not entry price fallback
  2. Zerodha tick rounding (Rs.0.05) on all prices
  3. Product type per strategy (MIS/CNC)
  4. SL_Limit column for Zerodha SL orders
  5. Sector rotation (one sector per run, stays under 2 min)
  6. Yahoo Finance User-Agent fix for GitHub Actions IPs
  7. Nifty trend filter (suppress BUY in DOWN market, SELL in UP market)
  8. ORB strategy (Opening Range Breakout)
  9. Daily alerted_today.csv reset built into script
 10. CNC (SWING) positions NOT squared off at EOD

ZERODHA LIVE READINESS: all prices tick-rounded, product types correct.
Kite Connect order placement commented out — uncomment when going live.

Usage:
    python scheduler.py                 # continuous loop (local laptop)
    python scheduler.py --single-run    # one scan + exit (GitHub Actions)
"""

import sys
import math
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone, timedelta
import requests
import schedule
import time
import os

# ── Config ─────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
LOG_FILE         = "intraday_trades.csv"
ALERTED_FILE     = "alerted_today.csv"
OPEN_POS_FILE    = "open_positions.csv"
LEDGER_FILE      = "trade_ledger.csv"
SCAN_INTERVAL    = 5
CAPITAL          = float(os.getenv("TRADING_CAPITAL", "100000"))
RISK_PCT         = float(os.getenv("RISK_PCT", "0.02"))
LIVE_TRADING     = os.getenv("LIVE_TRADING", "false").lower() == "true"
MAX_OPEN_POS     = 5

# ── Zerodha constants (ready for live — do not change) ─────
TICK             = 0.05
STRATEGY_PRODUCT = {
    "SCALP"    : "MIS",
    "MOMENTUM" : "MIS",
    "ORB"      : "MIS",
    "SWING"    : "CNC",   # holds overnight — NOT squared off at EOD
}

# ── Sectors ────────────────────────────────────────────────
SECTORS = {
    "Banking & Finance": [
        "HDFCBANK.NS","ICICIBANK.NS","SBIN.NS","KOTAKBANK.NS","AXISBANK.NS",
        "BAJFINANCE.NS","BAJAJFINSV.NS","INDUSINDBK.NS","AUBANK.NS","FEDERALBNK.NS",
        "IDFCFIRSTB.NS","BANDHANBNK.NS","RBLBANK.NS","PNB.NS","CANBK.NS",
        "BANKBARODA.NS","UNIONBANK.NS","INDIANB.NS","SBILIFE.NS","HDFCLIFE.NS",
        "ICICIPRULI.NS","ICICIGI.NS","MUTHOOTFIN.NS","MANAPPURAM.NS","CHOLAFIN.NS",
        "ABCAPITAL.NS","UJJIVANSFB.NS","EQUITASBNK.NS","ESAFSFB.NS","SURYODAY.NS",
        "UTKARSHBNK.NS","APTUS.NS","HOMEFIRST.NS","AAVAS.NS","CANFINHOME.NS",
        "ANGELONE.NS","IIFL.NS","MOTILALOFS.NS","5PAISA.NS","GEOJITFSL.NS",
    ],
    "Information Technology": [
        "TCS.NS","INFY.NS","WIPRO.NS","HCLTECH.NS","TECHM.NS","MPHASIS.NS",
        "PERSISTENT.NS","COFORGE.NS","KPITTECH.NS","TATAELXSI.NS","OFSS.NS",
        "CYIENT.NS","HFCL.NS","NAUKRI.NS",
    ],
    "Pharma & Healthcare": [
        "SUNPHARMA.NS","DRREDDY.NS","CIPLA.NS","DIVISLAB.NS","APOLLOHOSP.NS",
        "AUROPHARMA.NS","LUPIN.NS","ALKEM.NS","BIOCON.NS","GLAND.NS",
        "LAURUSLABS.NS","GRANULES.NS","NATCOPHARM.NS","IPCALAB.NS","SYNGENE.NS",
        "TORNTPHARM.NS","ZYDUSLIFE.NS","ABBOTINDIA.NS","PFIZER.NS","GLAXO.NS",
        "METROPOLIS.NS","LALPATHLAB.NS","THYROCARE.NS","KRSNAA.NS",
    ],
    "Auto & Auto Ancillary": [
        "MARUTI.NS","HEROMOTOCO.NS","BAJAJ-AUTO.NS","EICHERMOT.NS","MOTHERSON.NS",
        "BHARATFORG.NS","BALKRISIND.NS","APOLLOTYRE.NS","CEATLTD.NS","ASHOKLEY.NS",
        "ESCORTS.NS","TIINDIA.NS","CRAFTSMAN.NS","SUPRAJIT.NS","MRF.NS",
        "BOSCHLTD.NS","SONACOMS.NS",
    ],
    "Energy & Power": [
        "RELIANCE.NS","ONGC.NS","BPCL.NS","IOC.NS","HINDPETRO.NS","PETRONET.NS",
        "MGL.NS","IGL.NS","TATAPOWER.NS","ADANIGREEN.NS","TORNTPOWER.NS",
        "CESC.NS","NTPC.NS","POWERGRID.NS","NHPC.NS","COALINDIA.NS",
    ],
    "Infra & Defence": [
        "LT.NS","ADANIPORTS.NS","ADANIENT.NS","BHARTIARTL.NS","INDUSTOWER.NS",
        "SIEMENS.NS","ABB.NS","HAVELLS.NS","POLYCAB.NS","BHEL.NS","BEL.NS",
        "HAL.NS","COCHINSHIP.NS","GRSE.NS","BEML.NS","RVNL.NS","IRFC.NS",
        "HUDCO.NS","NBCC.NS","CONCOR.NS","BLUEDART.NS","TCI.NS",
        "CUMMINSIND.NS","THERMAX.NS","GRINDWELL.NS","TIMKEN.NS","SCHAEFFLER.NS",
    ],
    "Real Estate": [
        "DLF.NS","GODREJPROP.NS","PRESTIGE.NS","SOBHA.NS","PHOENIXLTD.NS",
        "BRIGADE.NS","OBEROIRLTY.NS","NESCO.NS",
    ],
    "Consumer & FMCG": [
        "HINDUNILVR.NS","ITC.NS","NESTLEIND.NS","BRITANNIA.NS","DABUR.NS",
        "MARICO.NS","COLPAL.NS","GODREJCP.NS","TATACONSUM.NS","PIDILITIND.NS",
        "BERGEPAINT.NS","ASIANPAINT.NS","PAGEIND.NS","WHIRLPOOL.NS","VOLTAS.NS",
        "TITAN.NS","TRENT.NS","DMART.NS","JUBLFOOD.NS","IRCTC.NS",
        "PVRINOX.NS","NAZARA.NS","ZEEL.NS","PAYTM.NS","NYKAA.NS",
        "INDHOTEL.NS","LEMONTREE.NS","CHALET.NS","TAJGVK.NS",
    ],
    "Metals & Materials": [
        "TATASTEEL.NS","JSWSTEEL.NS","HINDALCO.NS","VEDL.NS","SAIL.NS",
        "NMDC.NS","GRASIM.NS","ULTRACEMCO.NS","AMBUJACEM.NS","SHREECEM.NS",
        "APLAPOLLO.NS","JINDALSTEL.NS","RATNAMANI.NS",
    ],
    "Others": [
        "LICI.NS","RECLTD.NS","PFC.NS","TATAINVEST.NS","3MINDIA.NS",
        "HONAUT.NS","INOXWIND.NS","VIJAYA.NS","BOSCHLTD.NS","MPHASIS.NS",
    ],
}
ALL_STOCKS   = [s for sec in SECTORS.values() for s in sec]
SECTOR_NAMES = list(SECTORS.keys())

# ── IST helpers ────────────────────────────────────────────
def ist_now():
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))

def ist_str():
    return ist_now().strftime("%Y-%m-%d %H:%M:%S IST")

def is_market_open():
    n = ist_now()
    return n.weekday() < 5 and (
        (n.hour == 9 and n.minute >= 15) or
        (10 <= n.hour <= 14) or
        (n.hour == 15 and n.minute <= 15)
    )

def is_good_trading_window():
    """Skip first 15 min (volatile open) and last 15 min (no time to resolve)."""
    n = ist_now()
    after_open   = not (n.hour == 9 and n.minute < 30)
    before_close = not (n.hour >= 15)
    return after_open and before_close

def is_eod():
    """True from 3:15 PM IST — trigger MIS square-off."""
    n = ist_now()
    return n.weekday() < 5 and n.hour == 15 and n.minute >= 15

def get_current_sector():
    """Rotate sectors every 15 min — each run scans one sector (~30 stocks, ~90 sec)."""
    n = ist_now()
    minutes_since_open = max(0, (n.hour - 9) * 60 + n.minute - 15)
    return SECTOR_NAMES[(minutes_since_open // 15) % len(SECTOR_NAMES)]

# ── Zerodha tick rounding ───────────────────────────────────
def round_to_tick(price, direction="nearest"):
    """
    Round any price to the nearest valid NSE tick (Rs.0.05).

    direction="nearest" — entry prices and targets
    direction="up"      — BUY stop-loss trigger (round up = safer, triggers earlier)
    direction="down"    — SELL stop-loss trigger (round down = safer, triggers earlier)

    Examples (from your Zerodha observation):
        Buy entry 400.03  nearest -> Rs.400.05
        BUY SL   394.32   up      -> Rs.394.35 trigger, Rs.394.30 limit
        Target   408.61  nearest -> Rs.408.60
    """
    if not price or price == 0:
        return price
    ticks = price / TICK
    if direction == "up":
        return round(math.ceil(ticks) * TICK, 2)
    elif direction == "down":
        return round(math.floor(ticks) * TICK, 2)
    return round(round(ticks) * TICK, 2)

def zerodha_prices(signal_type, raw_price, raw_sl, raw_target):
    """
    Convert raw signal prices to Zerodha-valid tick-rounded prices.
    Returns: (entry, sl_trigger, sl_limit, target)

    Zerodha SL order needs BOTH trigger and limit price:
      BUY  SL: trigger rounds UP,   limit = trigger - 0.05
      SELL SL: trigger rounds DOWN, limit = trigger + 0.05
    """
    entry  = round_to_tick(raw_price,  "nearest")
    target = round_to_tick(raw_target, "nearest")
    if signal_type == "BUY":
        sl_trigger = round_to_tick(raw_sl, "up")
        sl_limit   = round_to_tick(sl_trigger - TICK, "nearest")
    else:
        sl_trigger = round_to_tick(raw_sl, "down")
        sl_limit   = round_to_tick(sl_trigger + TICK, "nearest")
    return entry, sl_trigger, sl_limit, target

# ── Telegram ───────────────────────────────────────────────
def send_telegram(msg, chat_id=None):
    cid = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_TOKEN or not cid:
        print("   [Telegram] Token/ChatID missing.")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": str(cid), "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
        if r.status_code != 200:
            print(f"   [Telegram] HTTP {r.status_code}: {r.text[:80]}")
    except Exception as e:
        print(f"   [Telegram] Error: {e}")

# ── Yahoo Finance session (User-Agent fixes GitHub Actions IP block) ──
def _yf_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36"
    })
    return s

# ── get_ltp — 3-method fallback (FIXED BUG: EOD prices were wrong) ──
def get_ltp(ticker):
    """
    Fetch last traded price with 3 fallback methods.

    Method 1: fast_info (fastest, but unreliable near/after market close)
    Method 2: 1-minute candle download (reliable during and after market hours)
    Method 3: 5-minute candle download (most reliable, slightly stale)

    Before this fix, get_ltp() only used fast_info, which returned None
    near EOD causing exit price = entry price and PnL = Rs.0 always.
    """
    session = _yf_session()

    # Method 1 — fast_info (best speed, least reliable near close)
    try:
        t = yf.Ticker(ticker, session=session)
        price = t.fast_info.get("last_price")
        if price and float(price) > 0:
            return float(price)
    except:
        pass

    # Method 2 — 1-min candle (reliable during + after market hours)
    try:
        data = yf.download(
            ticker, period="1d", interval="1m",
            auto_adjust=True, progress=False,
            timeout=20, session=session
        )
        if data is not None and len(data) > 0:
            price = float(data["Close"].squeeze().iloc[-1])
            if price > 0:
                return price
    except:
        pass

    # Method 3 — 5-min candle (most reliable fallback)
    try:
        data = yf.download(
            ticker, period="1d", interval="5m",
            auto_adjust=True, progress=False,
            timeout=20, session=session
        )
        if data is not None and len(data) > 0:
            price = float(data["Close"].squeeze().iloc[-1])
            if price > 0:
                return price
    except:
        pass

    print(f"   [get_ltp] All 3 methods failed for {ticker}")
    return None

def fetch_intraday(ticker, retries=3):
    """Download 5-min intraday candles with retry + User-Agent header."""
    session = _yf_session()
    for attempt in range(retries):
        try:
            data = yf.download(
                ticker, period="1d", interval="5m",
                auto_adjust=True, progress=False,
                timeout=30, session=session
            )
            if data is not None and len(data) >= 15:
                data.columns = [c[0] if isinstance(c, tuple) else c
                                for c in data.columns]
                return data
            time.sleep(1)
        except Exception as e:
            print(f"   [{ticker}] Attempt {attempt+1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(3)
    return None

def calc_vwap(data):
    tp  = (data["High"].squeeze() + data["Low"].squeeze() + data["Close"].squeeze()) / 3
    vol = data["Volume"].squeeze()
    return (tp * vol).cumsum() / vol.cumsum()

# ── Nifty 50 trend filter ──────────────────────────────────
def get_nifty_trend():
    """
    Suppress BUY signals when Nifty is falling (>0.3% down in last 30 min).
    Suppress SELL signals when Nifty is rising (>0.3% up in last 30 min).
    This avoids fighting the overall market direction.
    """
    try:
        data = yf.download(
            "^NSEI", period="1d", interval="5m",
            auto_adjust=True, progress=False,
            timeout=30, session=_yf_session()
        )
        if data is None or len(data) < 5:
            return "NEUTRAL"
        close      = data["Close"].squeeze()
        lookback   = min(6, len(close) - 1)
        change_pct = (float(close.iloc[-1]) - float(close.iloc[-lookback])) \
                     / float(close.iloc[-lookback]) * 100
        if change_pct > 0.3:  return "UP"
        if change_pct < -0.3: return "DOWN"
        return "NEUTRAL"
    except Exception as e:
        print(f"   [Nifty] Error: {e} — defaulting NEUTRAL")
        return "NEUTRAL"

# ── Strategy 1: SCALP — VWAP + RSI + Volume ───────────────
def scalp_signal(data):
    close  = data["Close"].squeeze()
    volume = data["Volume"].squeeze()
    vwap   = calc_vwap(data)
    delta  = close.diff()
    gain   = delta.clip(lower=0).ewm(span=14).mean()
    loss   = (-delta.clip(upper=0)).ewm(span=14).mean()
    rsi    = 100 - 100 / (1 + gain / loss)
    price    = float(close.iloc[-1])
    vwap_val = float(vwap.iloc[-1])
    rsi_val  = float(rsi.iloc[-1])
    vol_avg  = float(volume.rolling(20).mean().iloc[-1])
    vol_now  = float(volume.iloc[-1])
    vol_spike = vol_now > vol_avg * 1.3
    if price > vwap_val and vol_spike and 40 < rsi_val < 65:
        return "BUY",  price, round(vwap_val*0.997,2), round(price*1.005,2), rsi_val, "SCALP"
    if price < vwap_val and vol_spike and 55 < rsi_val < 75:
        return "SELL", price, round(price*1.003,2), round(price*0.995,2), rsi_val, "SCALP"
    return "HOLD", price, 0, 0, rsi_val, "SCALP"

# ── Strategy 2: MOMENTUM — EMA9/21 crossover + Volume ─────
def momentum_signal(data):
    close  = data["Close"].squeeze()
    volume = data["Volume"].squeeze()
    ema9   = close.ewm(span=9,  adjust=False).mean()
    ema21  = close.ewm(span=21, adjust=False).mean()
    delta  = close.diff()
    gain   = delta.clip(lower=0).ewm(span=14).mean()
    loss   = (-delta.clip(upper=0)).ewm(span=14).mean()
    rsi    = 100 - 100 / (1 + gain / loss)
    price    = float(close.iloc[-1])
    e9n, e9p = float(ema9.iloc[-1]),  float(ema9.iloc[-2])
    e21n,e21p= float(ema21.iloc[-1]), float(ema21.iloc[-2])
    rsi_val  = float(rsi.iloc[-1])
    vol_ok   = float(volume.iloc[-1]) > float(volume.rolling(20).mean().iloc[-1]) * 1.2
    if e9n > e21n and e9p <= e21p and vol_ok and rsi_val < 65:
        return "BUY",  price, round(e21n*0.993,2), round(price*1.015,2), rsi_val, "MOMENTUM"
    if e9n < e21n and e9p >= e21p and rsi_val > 40:
        return "SELL", price, round(e21n*1.007,2), round(price*0.985,2), rsi_val, "MOMENTUM"
    return "HOLD", price, 0, 0, rsi_val, "MOMENTUM"

# ── Strategy 3: SWING — MACD + Bollinger Bands ────────────
def swing_signal(data):
    close = data["Close"].squeeze()
    ema12  = close.ewm(span=12, adjust=False).mean()
    ema26  = close.ewm(span=26, adjust=False).mean()
    macd   = ema12 - ema26
    sig    = macd.ewm(span=9, adjust=False).mean()
    sma20  = close.rolling(20).mean()
    std20  = close.rolling(20).std()
    bb_up  = sma20 + 2*std20
    bb_low = sma20 - 2*std20
    price  = float(close.iloc[-1])
    mn,mp  = float(macd.iloc[-1]), float(macd.iloc[-2])
    sn,sp  = float(sig.iloc[-1]),  float(sig.iloc[-2])
    bbl    = float(bb_low.iloc[-1])
    bbu    = float(bb_up.iloc[-1])
    sma    = float(sma20.iloc[-1])
    if mn > sn and mp <= sp and price < sma and price > bbl:
        return "BUY",  price, round(bbl*0.99,2), round(sma*1.025,2), 0, "SWING"
    if mn < sn and mp >= sp and price > sma and price < bbu:
        return "SELL", price, round(bbu*1.01,2), round(sma*0.975,2), 0, "SWING"
    return "HOLD", price, 0, 0, 0, "SWING"

# ── Strategy 4: ORB — Opening Range Breakout ──────────────
def orb_signal(data):
    """
    Best NSE intraday strategy. First 15 min sets the range.
    Breakout above range high + volume = BUY.
    Breakdown below range low  + volume = SELL.
    Only fires in 9:30–11:00 AM window (ORB window).
    """
    try:
        close  = data["Close"].squeeze()
        high   = data["High"].squeeze()
        low    = data["Low"].squeeze()
        volume = data["Volume"].squeeze()
        if len(data) < 6:
            return "HOLD", float(close.iloc[-1]), 0, 0, 0, "ORB"
        orb_high  = float(high.iloc[:3].max())
        orb_low   = float(low.iloc[:3].min())
        orb_range = orb_high - orb_low
        price     = float(close.iloc[-1])
        vol_ok    = float(volume.iloc[-1]) > \
                    float(volume.rolling(20).mean().iloc[-1]) * 1.5
        n = ist_now()
        in_window = (n.hour == 9 and n.minute >= 30) or \
                    (n.hour == 10) or \
                    (n.hour == 11 and n.minute == 0)
        if not in_window:
            return "HOLD", price, 0, 0, 0, "ORB"
        if price > orb_high and vol_ok:
            return "BUY",  price, \
                   round(orb_high - orb_range*0.5, 2), \
                   round(price    + orb_range*1.5, 2), 0, "ORB"
        if price < orb_low and vol_ok:
            return "SELL", price, \
                   round(orb_low  + orb_range*0.5, 2), \
                   round(price    - orb_range*1.5, 2), 0, "ORB"
        return "HOLD", price, 0, 0, 0, "ORB"
    except:
        return "HOLD", 0, 0, 0, 0, "ORB"

# ── Duplicate alert prevention ─────────────────────────────
def reset_alert_file_if_new_day():
    """Reset alerted_today.csv when a new trading day starts."""
    today = ist_now().strftime("%Y-%m-%d")
    if os.path.exists(ALERTED_FILE):
        df = pd.read_csv(ALERTED_FILE)
        if "Date" in df.columns and len(df) > 0:
            if str(df["Date"].iloc[0]) != today:
                print(f"   [Reset] New day ({today}) — clearing {ALERTED_FILE}")
                pd.DataFrame(columns=["Date","Ticker","Strategy"]).to_csv(
                    ALERTED_FILE, index=False)
            else:
                print(f"   [Reset] Same day ({today}) — keeping {ALERTED_FILE} "
                      f"({len(df)} alerts logged so far)")
            return
    pd.DataFrame(columns=["Date","Ticker","Strategy"]).to_csv(ALERTED_FILE, index=False)
    print(f"   [Reset] Created fresh {ALERTED_FILE}")

def already_alerted_today(ticker, strategy):
    today = ist_now().strftime("%Y-%m-%d")
    if not os.path.exists(ALERTED_FILE):
        return False
    df = pd.read_csv(ALERTED_FILE)
    return len(df[(df["Date"] == today) &
                  (df["Ticker"] == ticker) &
                  (df["Strategy"] == strategy)]) > 0

def mark_alerted(ticker, strategy):
    today = ist_now().strftime("%Y-%m-%d")
    row   = pd.DataFrame([{"Date": today, "Ticker": ticker, "Strategy": strategy}])
    if os.path.exists(ALERTED_FILE):
        updated = pd.concat([pd.read_csv(ALERTED_FILE), row], ignore_index=True)
    else:
        updated = row
    updated.to_csv(ALERTED_FILE, index=False)

# ── Paper position management ──────────────────────────────
POS_COLS = ["Date","Ticker","Strategy","Product","Side",
            "EntryPrice","Qty","SL","SL_Limit","Target",
            "OrderId","EntryTime","Status"]
LED_COLS = ["Date","Ticker","Strategy","Product","Side",
            "EntryPrice","ExitPrice","Qty","PnL",
            "ExitReason","EntryTime","ExitTime","Mode"]

def load_open_positions():
    if not os.path.exists(OPEN_POS_FILE):
        return pd.DataFrame(columns=POS_COLS)
    df = pd.read_csv(OPEN_POS_FILE)
    for c in POS_COLS:
        if c not in df.columns:
            df[c] = ""
    return df

def save_open_positions(df):
    df.to_csv(OPEN_POS_FILE, index=False)

def load_ledger():
    if not os.path.exists(LEDGER_FILE):
        return pd.DataFrame(columns=LED_COLS)
    df = pd.read_csv(LEDGER_FILE)
    for c in LED_COLS:
        if c not in df.columns:
            df[c] = ""
    return df

def save_ledger(df):
    df.to_csv(LEDGER_FILE, index=False)

def open_paper_position(signal):
    """Open a paper position from a confirmed BUY/SELL signal."""
    pos_df   = load_open_positions()
    ticker   = signal["Stock"] + ".NS"
    strategy = signal["Strategy"]

    # Skip if already open in this stock+strategy
    if len(pos_df[(pos_df["Ticker"] == ticker) &
                  (pos_df["Strategy"] == strategy) &
                  (pos_df["Status"] == "OPEN")]) > 0:
        return

    # Cap at MAX_OPEN_POS simultaneous positions
    if len(pos_df[pos_df["Status"] == "OPEN"]) >= MAX_OPEN_POS:
        print(f"   [PAPER] Max positions ({MAX_OPEN_POS}) reached — skipping {ticker}")
        return

    # Parse tick-rounded prices from signal
    raw_price  = float(str(signal["Price"]).replace("Rs.","").replace("₹",""))
    raw_sl     = float(str(signal["Stop Loss"]).replace("Rs.","").replace("₹","")
                       .replace("-","0") or 0)
    raw_target = float(str(signal["Target"]).replace("Rs.","").replace("₹","")
                       .replace("-","0") or 0)

    # Prices already tick-rounded in scan_stock — but re-apply here for safety
    entry, sl_trigger, sl_limit, target = zerodha_prices(
        signal["Signal"], raw_price, raw_sl, raw_target)

    # Position sizing: risk Rs.(CAPITAL * RISK_PCT) per trade
    risk  = abs(entry - sl_trigger) if sl_trigger else entry * 0.01
    qty   = max(1, int((CAPITAL * RISK_PCT) / risk)) if risk > 0 else 1
    product = STRATEGY_PRODUCT.get(strategy, "MIS")

    new_row = pd.DataFrame([{
        "Date"      : ist_now().strftime("%Y-%m-%d"),
        "Ticker"    : ticker,
        "Strategy"  : strategy,
        "Product"   : product,
        "Side"      : signal["Signal"],
        "EntryPrice": entry,
        "Qty"       : qty,
        "SL"        : sl_trigger,
        "SL_Limit"  : sl_limit,
        "Target"    : target,
        "OrderId"   : f"PAPER-{int(time.time()*1000) % 10000000000}",
        "EntryTime" : ist_str(),
        "Status"    : "OPEN",
    }])
    save_open_positions(pd.concat([pos_df, new_row], ignore_index=True))
    print(f"   [PAPER] OPEN {signal['Signal']} {ticker} @ Rs.{entry} "
          f"SL={sl_trigger}(lmt={sl_limit}) T={target} qty={qty} [{product}]")

    # ── LIVE ORDER PLACEHOLDER (uncomment when Kite credentials ready) ──
    # if LIVE_TRADING and kite:
    #     order_id = kite.place_order(
    #         tradingsymbol = ticker.replace(".NS",""),
    #         exchange      = "NSE",
    #         transaction_type = signal["Signal"],          # BUY or SELL
    #         quantity      = qty,
    #         variety       = "regular",
    #         order_type    = "LIMIT",
    #         price         = entry,
    #         product       = product,                      # MIS or CNC
    #         validity      = "DAY",
    #     )
    #     # Place SL order immediately after entry
    #     sl_order_id = kite.place_order(
    #         tradingsymbol    = ticker.replace(".NS",""),
    #         exchange         = "NSE",
    #         transaction_type = "SELL" if signal["Signal"]=="BUY" else "BUY",
    #         quantity         = qty,
    #         variety          = "regular",
    #         order_type       = "SL",
    #         price            = sl_limit,      # limit price
    #         trigger_price    = sl_trigger,    # trigger price
    #         product          = product,
    #         validity         = "DAY",
    #     )

def monitor_and_close_positions():
    """Check all OPEN positions against current LTP. Close on SL or Target hit."""
    pos_df = load_open_positions()
    ledger = load_ledger()
    open_p = pos_df[pos_df["Status"] == "OPEN"].copy()
    if open_p.empty:
        return

    new_ledger_rows = []
    for idx, pos in open_p.iterrows():
        ltp = get_ltp(pos["Ticker"])
        if ltp is None:
            print(f"   [Monitor] LTP unavailable for {pos['Ticker']} — skipping this cycle")
            continue

        entry  = float(pos["EntryPrice"])
        sl     = float(pos["SL"])     if pd.notna(pos["SL"])     and str(pos["SL"])     != "" else 0
        target = float(pos["Target"]) if pd.notna(pos["Target"]) and str(pos["Target"]) != "" else 0
        side   = pos["Side"]
        qty    = int(pos["Qty"])
        exit_reason = None

        if sl > 0:
            if side == "BUY"  and ltp <= sl: exit_reason = "SL_HIT"
            if side == "SELL" and ltp >= sl: exit_reason = "SL_HIT"
        if target > 0 and not exit_reason:
            if side == "BUY"  and ltp >= target: exit_reason = "TARGET_HIT"
            if side == "SELL" and ltp <= target: exit_reason = "TARGET_HIT"

        if exit_reason:
            pnl     = round((ltp-entry)*qty,2) if side=="BUY" else round((entry-ltp)*qty,2)
            product = str(pos.get("Product","MIS"))
            pos_df.at[idx, "Status"] = "CLOSED"
            new_ledger_rows.append({
                "Date"      : pos["Date"],
                "Ticker"    : pos["Ticker"],
                "Strategy"  : pos["Strategy"],
                "Product"   : product,
                "Side"      : side,
                "EntryPrice": entry,
                "ExitPrice" : ltp,
                "Qty"       : qty,
                "PnL"       : pnl,
                "ExitReason": exit_reason,
                "EntryTime" : pos["EntryTime"],
                "ExitTime"  : ist_str(),
                "Mode"      : "LIVE" if LIVE_TRADING else "PAPER",
            })
            emoji = "✅" if exit_reason == "TARGET_HIT" else "🛑"
            send_telegram(
                f"{emoji} <b>{exit_reason}</b> — {pos['Ticker'].replace('.NS','')}\n"
                f"Side    : {side} [{product}]\n"
                f"Entry   : Rs.{entry}\n"
                f"Exit    : Rs.{ltp}\n"
                f"P&amp;L : Rs.{pnl}\n"
                f"Time    : {ist_str()}"
            )
            print(f"   [Monitor] CLOSE {pos['Ticker']} "
                  f"{exit_reason} ltp={ltp} pnl={pnl}")

    save_open_positions(pos_df)
    if new_ledger_rows:
        save_ledger(pd.concat([ledger, pd.DataFrame(new_ledger_rows)],
                               ignore_index=True))

def eod_square_off():
    """
    Force-close all MIS positions at EOD (3:15 PM IST).
    CNC (SWING) positions are NOT touched — they hold overnight.
    Zerodha also auto-squares MIS 5-25 min before close as backup.
    """
    if not is_eod():
        return

    pos_df = load_open_positions()
    ledger = load_ledger()

    # Only MIS positions — leave CNC alone
    if "Product" in pos_df.columns:
        to_close = pos_df[(pos_df["Status"] == "OPEN") &
                          (pos_df["Product"] != "CNC")].copy()
    else:
        to_close = pos_df[pos_df["Status"] == "OPEN"].copy()

    if to_close.empty:
        print(f"   [EOD] No MIS positions to square off.")
        return

    new_ledger_rows = []
    for idx, pos in to_close.iterrows():
        ltp   = get_ltp(pos["Ticker"])
        if ltp is None:
            ltp = float(pos["EntryPrice"])
            print(f"   [EOD] LTP unavailable for {pos['Ticker']} — using entry price")
        entry   = float(pos["EntryPrice"])
        qty     = int(pos["Qty"])
        side    = pos["Side"]
        product = str(pos.get("Product","MIS"))
        pnl     = round((ltp-entry)*qty,2) if side=="BUY" else round((entry-ltp)*qty,2)

        pos_df.at[idx, "Status"] = "CLOSED"
        new_ledger_rows.append({
            "Date"      : pos["Date"],
            "Ticker"    : pos["Ticker"],
            "Strategy"  : pos["Strategy"],
            "Product"   : product,
            "Side"      : side,
            "EntryPrice": entry,
            "ExitPrice" : ltp,
            "Qty"       : qty,
            "PnL"       : pnl,
            "ExitReason": "EOD_SQUARE_OFF",
            "EntryTime" : pos["EntryTime"],
            "ExitTime"  : ist_str(),
            "Mode"      : "LIVE" if LIVE_TRADING else "PAPER",
        })
        print(f"   [EOD] {pos['Ticker']} ltp={ltp} pnl={pnl}")

    save_open_positions(pos_df)
    if new_ledger_rows:
        save_ledger(pd.concat([ledger, pd.DataFrame(new_ledger_rows)],
                               ignore_index=True))

    total_pnl   = sum(r["PnL"] for r in new_ledger_rows)
    wins        = sum(1 for r in new_ledger_rows if r["PnL"] > 0)
    losses      = sum(1 for r in new_ledger_rows if r["PnL"] <= 0)
    send_telegram(
        f"📊 <b>EOD Summary</b>\n"
        f"MIS positions : {len(new_ledger_rows)}\n"
        f"Wins / Losses : {wins} / {losses}\n"
        f"Total P&amp;L : Rs.{round(total_pnl,2)}\n"
        f"Mode          : {'LIVE' if LIVE_TRADING else 'PAPER'}\n"
        f"CNC (SWING)   : held overnight\n"
        f"Time          : {ist_str()}"
    )

# ── Scan one stock across all 4 strategies ─────────────────
def scan_stock(ticker, nifty_trend="NEUTRAL"):
    data = fetch_intraday(ticker)
    if data is None:
        return []

    signals = []
    for strategy_fn in [scalp_signal, momentum_signal, swing_signal, orb_signal]:
        try:
            sig, price, sl, target, rsi, strat = strategy_fn(data)

            # Nifty filter — avoid fighting overall market direction
            if nifty_trend == "DOWN" and sig == "BUY":  continue
            if nifty_trend == "UP"   and sig == "SELL": continue

            if sig in ["BUY","SELL"] and not already_alerted_today(ticker, strat):
                mark_alerted(ticker, strat)

                # Apply Zerodha tick rounding to all prices
                entry_z, sl_z, sl_lmt_z, target_z = zerodha_prices(
                    sig, price, sl, target)
                rr = round(abs(target_z-entry_z)/abs(entry_z-sl_z),2) \
                     if sl_z and target_z and entry_z != sl_z else 0
                product = STRATEGY_PRODUCT.get(strat, "MIS")

                signals.append({
                    "Time"     : ist_str(),
                    "Stock"    : ticker.replace(".NS",""),
                    "Strategy" : strat,
                    "Product"  : product,
                    "Signal"   : sig,
                    "Price"    : f"Rs.{entry_z}",
                    "Stop Loss": f"Rs.{sl_z}" if sl_z else "-",
                    "SL_Limit" : f"Rs.{sl_lmt_z}" if sl_lmt_z else "-",
                    "Target"   : f"Rs.{target_z}" if target_z else "-",
                    "R:R"      : f"1:{rr}" if rr else "-",
                    "RSI"      : round(rsi,1) if rsi else "-",
                })
        except:
            pass
    return signals

def save_to_log(signals):
    if not signals: return
    cols = ["Time","Stock","Strategy","Product","Signal",
            "Price","Stop Loss","SL_Limit","Target","R:R","RSI"]
    log  = pd.read_csv(LOG_FILE) if os.path.exists(LOG_FILE) \
           else pd.DataFrame(columns=cols)
    rows = [{c: s.get(c,"-") for c in cols} for s in signals]
    pd.concat([log, pd.DataFrame(rows)], ignore_index=True).to_csv(
        LOG_FILE, index=False)

# ── Main scan job ──────────────────────────────────────────
def run_full_scan():
    if not is_market_open():
        print(f"[{ist_str()}] Market closed — skipping.")
        return

    # 3:15 PM — EOD square-off for MIS positions
    if is_eod():
        print(f"[{ist_str()}] EOD — squaring off MIS positions.")
        eod_square_off()
        return

    # Always monitor open positions first (SL/Target check)
    monitor_and_close_positions()

    # Skip signal scanning outside the good trading window
    if not is_good_trading_window():
        print(f"[{ist_str()}] Outside signal window (first/last 15 min) — monitoring only.")
        return

    # Sector rotation — one sector per run
    current_sector = get_current_sector()
    sector_stocks  = SECTORS[current_sector]
    nifty_trend    = get_nifty_trend()

    print(f"[{ist_str()}] Sector: {current_sector} "
          f"({len(sector_stocks)} stocks) | Nifty: {nifty_trend}")

    all_signals = []
    for ticker in sector_stocks:
        sigs = scan_stock(ticker, nifty_trend)
        all_signals.extend(sigs)
        time.sleep(0.3)

    buys  = [s for s in all_signals if s["Signal"] == "BUY"]
    sells = [s for s in all_signals if s["Signal"] == "SELL"]

    # Send alert + open paper position for each signal
    for s in buys + sells:
        emoji = "🟢" if s["Signal"] == "BUY" else "🔴"
        send_telegram(
            f"{emoji} <b>{s['Signal']} — {s['Stock']}</b>\n"
            f"Strategy : {s['Strategy']} ({s['Product']})\n"
            f"Price    : {s['Price']}\n"
            f"SL       : {s['Stop Loss']} (lmt: {s['SL_Limit']})\n"
            f"Target   : {s['Target']}\n"
            f"R:R      : {s['R:R']}\n"
            f"RSI      : {s['RSI']}\n"
            f"Nifty    : {nifty_trend}\n"
            f"Time     : {s['Time']}"
        )
        open_paper_position(s)

    save_to_log(all_signals)

    # Scan summary to your Telegram
    send_telegram(
        f"✅ Scan — {current_sector}\n"
        f"🟢 BUY:{len(buys)} 🔴 SELL:{len(sells)}\n"
        f"Nifty:{nifty_trend} | {'LIVE' if LIVE_TRADING else 'PAPER'}\n"
        f"{ist_str()}"
    )
    print(f"[{ist_str()}] Done — BUY:{len(buys)} SELL:{len(sells)}")
    print("-" * 60)

# ── Entry point ─────────────────────────────────────────────
SINGLE_RUN = "--single-run" in sys.argv

print("=" * 60)
print("PROJECT 1 — INTRADAY SCANNER")
print(f"Mode    : {'SINGLE RUN (GitHub Actions)' if SINGLE_RUN else 'CONTINUOUS LOOP (local)'}")
print(f"Capital : {'LIVE' if LIVE_TRADING else 'PAPER'} Rs.{CAPITAL:,.0f}")
print(f"Started : {ist_str()}")
print("=" * 60)

# Always reset alert file first (fixes BUY:0 SELL:0 stale file bug)
reset_alert_file_if_new_day()

if SINGLE_RUN:
    run_full_scan()
    print(f"[{ist_str()}] Single run complete — exiting.")
else:
    send_telegram(
        f"🚀 Scheduler started\n"
        f"Scanning every {SCAN_INTERVAL} min during market hours.\n"
        f"Mode: {'LIVE' if LIVE_TRADING else 'PAPER'} | Rs.{CAPITAL:,.0f}\n"
        f"{ist_str()}"
    )
    run_full_scan()
    schedule.every(SCAN_INTERVAL).minutes.do(run_full_scan)
    while True:
        schedule.run_pending()
        time.sleep(20)
