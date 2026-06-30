"""
Standalone Intraday Scanner Scheduler
Run this in its own terminal window — separate from Streamlit.
This keeps running and scanning every 5 minutes, sending Telegram
alerts, completely independent of whether your browser is open.

Usage:
    python scheduler.py
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timezone, timedelta
import requests
import schedule
import time
import os

from dotenv import load_dotenv
load_dotenv()
from executor import Executor   # ← NEW: risk-managed execution layer

# ── Config ─────────────────────────────────────────────────
# Secrets now come from environment variables — never hardcode tokens.
# Before running:  export TELEGRAM_TOKEN=...  export TELEGRAM_CHAT_ID=...
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
LOG_FILE         = "intraday_trades.csv"
ALERTED_FILE     = "alerted_today.csv"
SCAN_INTERVAL    = 5   # minutes
POSITION_CHECK_INTERVAL = 1   # minutes — how often open positions are checked for SL/target

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
ALL_STOCKS = [s for sec in SECTORS.values() for s in sec]

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

# ── Telegram ───────────────────────────────────────────────
def send_telegram(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID,
                  "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        print(f"Telegram error: {e}")

# ── Fetch 5-min intraday data ──────────────────────────────
def fetch_intraday(ticker):
    try:
        data = yf.download(
            ticker, period="1d", interval="5m",
            auto_adjust=True, progress=False
        )
        if data is None or len(data) < 15:
            return None
        data.columns = [c[0] if isinstance(c, tuple) else c
                        for c in data.columns]
        return data
    except:
        return None

def calc_vwap(data):
    tp  = (data["High"].squeeze() + data["Low"].squeeze() + data["Close"].squeeze()) / 3
    vol = data["Volume"].squeeze()
    return (tp * vol).cumsum() / vol.cumsum()

# ── Strategy 1: SCALP ──────────────────────────────────────
def scalp_signal(data):
    close  = data["Close"].squeeze()
    volume = data["Volume"].squeeze()
    vwap   = calc_vwap(data)
    delta    = close.diff()
    gain     = delta.clip(lower=0).ewm(span=14).mean()
    loss     = (-delta.clip(upper=0)).ewm(span=14).mean()
    rsi      = 100 - 100 / (1 + gain / loss)
    price    = float(close.iloc[-1])
    vwap_val = float(vwap.iloc[-1])
    rsi_val  = float(rsi.iloc[-1])
    vol_avg  = float(volume.rolling(20).mean().iloc[-1])
    vol_now  = float(volume.iloc[-1])
    above_vwap = price > vwap_val
    below_vwap = price < vwap_val
    vol_spike  = vol_now > vol_avg * 1.3
    if above_vwap and vol_spike and 40 < rsi_val < 65:
        sl     = round(vwap_val * 0.997, 2)
        target = round(price * 1.005, 2)
        return "BUY", price, sl, target, rsi_val, "SCALP"
    if below_vwap and vol_spike and 55 < rsi_val < 75:
        sl     = round(price * 1.003, 2)
        target = round(price * 0.995, 2)
        return "SELL", price, sl, target, rsi_val, "SCALP"
    return "HOLD", price, 0, 0, rsi_val, "SCALP"

# ── Strategy 2: MOMENTUM ───────────────────────────────────
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
    e9_now   = float(ema9.iloc[-1]);  e9_prev  = float(ema9.iloc[-2])
    e21_now  = float(ema21.iloc[-1]); e21_prev = float(ema21.iloc[-2])
    rsi_val  = float(rsi.iloc[-1])
    vol_avg  = float(volume.rolling(20).mean().iloc[-1])
    vol_now  = float(volume.iloc[-1])
    cross_up   = e9_now > e21_now and e9_prev <= e21_prev
    cross_down = e9_now < e21_now and e9_prev >= e21_prev
    vol_ok     = vol_now > vol_avg * 1.2
    if cross_up and vol_ok and rsi_val < 65:
        sl     = round(float(ema21.iloc[-1]) * 0.993, 2)
        target = round(price * 1.015, 2)
        return "BUY", price, sl, target, rsi_val, "MOMENTUM"
    if cross_down and rsi_val > 40:
        sl     = round(float(ema21.iloc[-1]) * 1.007, 2)
        target = round(price * 0.985, 2)
        return "SELL", price, sl, target, rsi_val, "MOMENTUM"
    return "HOLD", price, 0, 0, rsi_val, "MOMENTUM"

# ── Strategy 3: SWING ──────────────────────────────────────
def swing_signal(data):
    close = data["Close"].squeeze()
    ema12  = close.ewm(span=12, adjust=False).mean()
    ema26  = close.ewm(span=26, adjust=False).mean()
    macd   = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    sma20  = close.rolling(20).mean()
    std20  = close.rolling(20).std()
    bb_up  = sma20 + 2 * std20
    bb_low = sma20 - 2 * std20
    price    = float(close.iloc[-1])
    macd_now = float(macd.iloc[-1]); macd_prev = float(macd.iloc[-2])
    sig_now  = float(signal.iloc[-1]); sig_prev = float(signal.iloc[-2])
    bb_low_v = float(bb_low.iloc[-1])
    bb_up_v  = float(bb_up.iloc[-1])
    sma_v    = float(sma20.iloc[-1])
    cross_up   = macd_now > sig_now and macd_prev <= sig_prev
    cross_down = macd_now < sig_now and macd_prev >= sig_prev
    near_low   = price < sma_v and price > bb_low_v
    near_high  = price > sma_v and price < bb_up_v
    if cross_up and near_low:
        sl     = round(bb_low_v * 0.99, 2)
        target = round(sma_v * 1.025, 2)
        return "BUY", price, sl, target, 0, "SWING"
    if cross_down and near_high:
        sl     = round(bb_up_v * 1.01, 2)
        target = round(sma_v * 0.975, 2)
        return "SELL", price, sl, target, 0, "SWING"
    return "HOLD", price, 0, 0, 0, "SWING"

# ── Duplicate alert prevention ─────────────────────────────
def already_alerted_today(ticker, strategy):
    today = ist_now().strftime("%Y-%m-%d")
    if not os.path.exists(ALERTED_FILE):
        return False
    df = pd.read_csv(ALERTED_FILE)
    match = df[(df["Date"] == today) & (df["Ticker"] == ticker) & (df["Strategy"] == strategy)]
    return len(match) > 0

def mark_alerted(ticker, strategy):
    today = ist_now().strftime("%Y-%m-%d")
    row   = pd.DataFrame([{"Date": today, "Ticker": ticker, "Strategy": strategy}])
    if os.path.exists(ALERTED_FILE):
        existing = pd.read_csv(ALERTED_FILE)
        updated  = pd.concat([existing, row], ignore_index=True)
    else:
        updated = row
    updated.to_csv(ALERTED_FILE, index=False)

# ── Scan one stock ─────────────────────────────────────────
def scan_stock(ticker):
    data = fetch_intraday(ticker)
    if data is None:
        return []
    signals = []
    for strategy_fn in [scalp_signal, momentum_signal, swing_signal]:
        try:
            sig, price, sl, target, rsi, strat = strategy_fn(data)
            if sig in ["BUY", "SELL"] and not already_alerted_today(ticker, strat):
                mark_alerted(ticker, strat)
                rr = round(abs(target - price) / abs(price - sl), 2) if sl and target and price != sl else 0
                signals.append({
                    "Time"     : ist_str(),
                    "Stock"    : ticker.replace(".NS", ""),
                    "Strategy" : strat,
                    "Signal"   : sig,
                    "Price"    : f"₹{price}",
                    "Stop Loss": f"₹{sl}" if sl else "—",
                    "Target"   : f"₹{target}" if target else "—",
                    "R:R"      : f"1:{rr}" if rr else "—",
                    "RSI"      : round(rsi, 1) if rsi else "—",
                })
        except: pass
    return signals

def save_to_log(signals):
    if not signals: return
    if os.path.exists(LOG_FILE):
        log = pd.read_csv(LOG_FILE)
    else:
        log = pd.DataFrame(columns=["Time","Stock","Strategy","Signal","Price","Stop Loss","Target","R:R","RSI"])
    new_log = pd.concat([log, pd.DataFrame(signals)], ignore_index=True)
    new_log.to_csv(LOG_FILE, index=False)

def alert_signal(s):
    emoji = "🟢" if s["Signal"] == "BUY" else "🔴"
    send_telegram(
        f"{emoji} <b>{s['Signal']} — {s['Stock']}</b>\n"
        f"Strategy   : {s['Strategy']}\n"
        f"Price      : {s['Price']}\n"
        f"Stop Loss  : {s['Stop Loss']}\n"
        f"Target     : {s['Target']}\n"
        f"Risk:Reward: {s['R:R']}\n"
        f"RSI        : {s['RSI']}\n"
        f"Time       : {s['Time']}"
    )

# ── Main scan job ──────────────────────────────────────────
def run_full_scan():
    if not is_market_open():
        print(f"[{ist_str()}] Market closed — skipping scan.")
        return

    print(f"[{ist_str()}] Starting scan of {len(ALL_STOCKS)} stocks...")
    all_signals = []

    for idx, ticker in enumerate(ALL_STOCKS):
        sigs = scan_stock(ticker)
        all_signals.extend(sigs)
        time.sleep(0.3)
        if (idx + 1) % 50 == 0:
            print(f"   ...scanned {idx + 1}/{len(ALL_STOCKS)}")

    buys  = [s for s in all_signals if s["Signal"] == "BUY"]
    sells = [s for s in all_signals if s["Signal"] == "SELL"]

    for s in buys + sells:
        alert_signal(s)

    save_to_log(all_signals)

    # ── NEW: hand signals to the risk-managed executor ──────
    # Paper-trades by default; set LIVE_TRADING=true env var to go live.
    executor.process_signals(all_signals)

    if buys or sells:
        send_telegram(
            f"✅ <b>Scan Complete</b>\n"
            f"🟢 BUY  : {len(buys)}\n"
            f"🔴 SELL : {len(sells)}\n"
            f"{ist_str()}"
        )

    print(f"[{ist_str()}] Scan done — BUY:{len(buys)} SELL:{len(sells)}")
    print("-" * 60)

# ── Entry point ─────────────────────────────────────────────
import sys

SINGLE_RUN = "--single-run" in sys.argv

executor = Executor()
print("=" * 60)
print("INTRADAY SCANNER")
print(f"Mode: {'SINGLE RUN (GitHub Actions)' if SINGLE_RUN else 'CONTINUOUS LOOP (local)'}")
print(f"Execution: {'LIVE (real orders via Kite)' if os.getenv('LIVE_TRADING','false').lower()=='true' else 'PAPER (simulated, no real orders)'}")
print(f"Started at: {ist_str()}")
print("=" * 60)

if SINGLE_RUN:
    # GitHub Actions calls this script every 5 minutes itself —
    # so we do ONE scan + ONE position check, then exit cleanly.
    run_full_scan()
    executor.monitor_positions()
    executor.eod_square_off_if_needed()
    print(f"[{ist_str()}] Single run complete — exiting.")

else:
    # Local laptop mode — keep looping forever, like before.
    send_telegram(
        f"🚀 <b>Scheduler Started</b>\n"
        f"Will scan every {SCAN_INTERVAL} min during market hours.\n"
        f"Time: {ist_str()}"
    )

    run_full_scan()

    schedule.every(SCAN_INTERVAL).minutes.do(run_full_scan)
    schedule.every(POSITION_CHECK_INTERVAL).minutes.do(executor.monitor_positions)
    schedule.every(1).minutes.do(executor.eod_square_off_if_needed)

    while True:
        schedule.run_pending()
        time.sleep(20)
