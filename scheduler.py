"""
PROJECT 1 — Intraday Scanner (Single User / Your Own Alerts)
Runs on GitHub Actions every 5 min during market hours.
Sends signals to YOUR Telegram only.
Executor handles your own paper/live capital separately.

Usage:
    python scheduler_project1.py                # continuous (local)
    python scheduler_project1.py --single-run   # one scan (GitHub Actions)
"""

import sys
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

def is_good_trading_window():
    """Avoid first 15 min (volatile) and last 15 min (no time to resolve)."""
    n = ist_now()
    after_open  = not (n.hour == 9 and n.minute < 30)
    before_close= not (n.hour == 15 and n.minute > 0)
    return after_open and before_close

def is_eod():
    n = ist_now()
    return n.hour == 15 and n.minute >= 15

# ── Telegram ───────────────────────────────────────────────
def send_telegram(msg, chat_id=None):
    cid = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_TOKEN or not cid:
        print(f"   [Telegram] Token/ChatID missing — skipping send.")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": cid, "text": msg, "parse_mode": "HTML"},
            timeout=10
        )
        if r.status_code != 200:
            print(f"   [Telegram] HTTP {r.status_code}: {r.text[:100]}")
    except Exception as e:
        print(f"   [Telegram] Error: {e}")

# ── Nifty 50 market filter ─────────────────────────────────
def get_nifty_trend():
    """Returns 'UP', 'DOWN', or 'NEUTRAL' based on Nifty 50 direction."""
    try:
        data = yf.download("^NSEI", period="1d", interval="5m",
                           auto_adjust=True, progress=False)
        if data is None or len(data) < 5:
            return "NEUTRAL"
        close = data["Close"].squeeze()
        # Compare current price to 30-min ago (6 candles of 5 min)
        lookback = min(6, len(close) - 1)
        current = float(close.iloc[-1])
        prev    = float(close.iloc[-lookback])
        change_pct = (current - prev) / prev * 100
        if change_pct > 0.3:
            return "UP"
        elif change_pct < -0.3:
            return "DOWN"
        return "NEUTRAL"
    except:
        return "NEUTRAL"

# ── Fetch 5-min intraday data ──────────────────────────────
def fetch_intraday(ticker):
    try:
        data = yf.download(ticker, period="1d", interval="5m",
                           auto_adjust=True, progress=False)
        if data is None or len(data) < 15:
            return None
        data.columns = [c[0] if isinstance(c, tuple) else c for c in data.columns]
        return data
    except:
        return None

def get_ltp(ticker):
    try:
        t = yf.Ticker(ticker)
        price = t.fast_info.get("last_price")
        return float(price) if price else None
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

# ── NEW: Strategy 4 — Opening Range Breakout (ORB) ─────────
def orb_signal(data):
    """
    Opening Range Breakout — most powerful NSE intraday strategy.
    First 15 min (9:15-9:30) defines the range. Breakout = signal.
    Only fires once in the first 90 min of the session.
    """
    try:
        close  = data["Close"].squeeze()
        high   = data["High"].squeeze()
        low    = data["Low"].squeeze()
        volume = data["Volume"].squeeze()

        # Need at least 6 candles (30 min) to establish ORB
        if len(data) < 6:
            return "HOLD", float(close.iloc[-1]), 0, 0, 0, "ORB"

        # First 3 candles = 9:15-9:30 AM (opening range)
        orb_high = float(high.iloc[:3].max())
        orb_low  = float(low.iloc[:3].min())
        orb_range= orb_high - orb_low

        # Current values
        price   = float(close.iloc[-1])
        vol_avg = float(volume.rolling(20).mean().iloc[-1])
        vol_now = float(volume.iloc[-1])
        vol_ok  = vol_now > vol_avg * 1.5  # stronger volume required for ORB

        # Only trade ORB in first 90 min (9:30-11:00 AM)
        n = ist_now()
        in_orb_window = (n.hour == 9 and n.minute >= 30) or \
                        (n.hour == 10) or \
                        (n.hour == 11 and n.minute == 0)

        if not in_orb_window:
            return "HOLD", price, 0, 0, 0, "ORB"

        # Breakout above range high
        if price > orb_high and vol_ok:
            sl     = round(orb_high - orb_range * 0.5, 2)
            target = round(price + orb_range * 1.5, 2)
            return "BUY", price, sl, target, 0, "ORB"

        # Breakdown below range low
        if price < orb_low and vol_ok:
            sl     = round(orb_low + orb_range * 0.5, 2)
            target = round(price - orb_range * 1.5, 2)
            return "SELL", price, sl, target, 0, "ORB"

        return "HOLD", price, 0, 0, 0, "ORB"
    except:
        return "HOLD", 0, 0, 0, 0, "ORB"

# ── Duplicate alert prevention ─────────────────────────────
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
def load_open_positions():
    if not os.path.exists(OPEN_POS_FILE):
        return pd.DataFrame(columns=[
            "Date","Ticker","Strategy","Side","EntryPrice","Qty",
            "SL","Target","OrderId","EntryTime","Status"])
    return pd.read_csv(OPEN_POS_FILE)

def save_open_positions(df):
    df.to_csv(OPEN_POS_FILE, index=False)

def load_ledger():
    if not os.path.exists(LEDGER_FILE):
        return pd.DataFrame(columns=[
            "Date","Ticker","Strategy","Side","EntryPrice","ExitPrice",
            "Qty","PnL","ExitReason","EntryTime","ExitTime","Mode"])
    return pd.read_csv(LEDGER_FILE)

def save_ledger(df):
    df.to_csv(LEDGER_FILE, index=False)

def open_paper_position(signal):
    """Open a new paper position from a signal dict."""
    pos_df  = load_open_positions()
    ticker  = signal["Stock"] + ".NS"

    # Skip if already have open position in this stock+strategy
    existing = pos_df[(pos_df["Ticker"] == ticker) &
                      (pos_df["Strategy"] == signal["Strategy"]) &
                      (pos_df["Status"] == "OPEN")]
    if len(existing) > 0:
        return

    # Count open positions — cap at 5 simultaneous
    open_count = len(pos_df[pos_df["Status"] == "OPEN"])
    if open_count >= 5:
        return

    price   = float(str(signal["Price"]).replace("Rs.", "").replace("₹", ""))
    sl      = float(str(signal["Stop Loss"]).replace("Rs.", "").replace("₹", "").replace("-", "0") or 0)
    target  = float(str(signal["Target"]).replace("Rs.", "").replace("₹", "").replace("-", "0") or 0)
    risk    = abs(price - sl) if sl else price * 0.01
    qty     = max(1, int((CAPITAL * RISK_PCT) / risk)) if risk > 0 else 1
    side    = signal["Signal"]
    order_id= f"PAPER-{int(time.time() * 1000) % 10000000000}"
    today   = ist_now().strftime("%Y-%m-%d")

    new_row = pd.DataFrame([{
        "Date"      : today,
        "Ticker"    : ticker,
        "Strategy"  : signal["Strategy"],
        "Side"      : side,
        "EntryPrice": price,
        "Qty"       : qty,
        "SL"        : sl,
        "Target"    : target,
        "OrderId"   : order_id,
        "EntryTime" : ist_str(),
        "Status"    : "OPEN",
    }])
    updated = pd.concat([pos_df, new_row], ignore_index=True)
    save_open_positions(updated)
    print(f"   [PAPER] OPEN {side} {ticker} @ {price} qty={qty} SL={sl} T={target}")

def monitor_and_close_positions():
    """Check all open positions against current price. Close if SL/Target hit."""
    pos_df  = load_open_positions()
    ledger  = load_ledger()
    open_p  = pos_df[pos_df["Status"] == "OPEN"].copy()

    if open_p.empty:
        return

    updated_rows = []
    new_ledger_rows = []

    for idx, pos in open_p.iterrows():
        ltp = get_ltp(pos["Ticker"])
        if ltp is None:
            updated_rows.append(pos)
            continue

        entry  = float(pos["EntryPrice"])
        sl     = float(pos["SL"]) if pos["SL"] else 0
        target = float(pos["Target"]) if pos["Target"] else 0
        side   = pos["Side"]
        qty    = int(pos["Qty"])
        exit_reason = None

        # Check SL hit
        if sl > 0:
            if side == "BUY"  and ltp <= sl: exit_reason = "SL_HIT"
            if side == "SELL" and ltp >= sl: exit_reason = "SL_HIT"

        # Check Target hit
        if target > 0 and not exit_reason:
            if side == "BUY"  and ltp >= target: exit_reason = "TARGET_HIT"
            if side == "SELL" and ltp <= target: exit_reason = "TARGET_HIT"

        if exit_reason:
            if side == "BUY":
                pnl = round((ltp - entry) * qty, 2)
            else:
                pnl = round((entry - ltp) * qty, 2)

            pos["Status"] = "CLOSED"
            updated_rows.append(pos)
            new_ledger_rows.append({
                "Date"       : pos["Date"],
                "Ticker"     : pos["Ticker"],
                "Strategy"   : pos["Strategy"],
                "Side"       : side,
                "EntryPrice" : entry,
                "ExitPrice"  : ltp,
                "Qty"        : qty,
                "PnL"        : pnl,
                "ExitReason" : exit_reason,
                "EntryTime"  : pos["EntryTime"],
                "ExitTime"   : ist_str(),
                "Mode"       : "LIVE" if LIVE_TRADING else "PAPER",
            })
            emoji = "TARGET" if exit_reason == "TARGET_HIT" else "SL"
            print(f"   [PAPER] CLOSE {pos['Ticker']} {exit_reason} LTP={ltp} PnL={pnl}")
            send_telegram(
                f"{'✅' if exit_reason == 'TARGET_HIT' else '🛑'} "
                f"<b>{exit_reason} — {pos['Ticker'].replace('.NS','')}</b>\n"
                f"Side    : {side}\n"
                f"Entry   : Rs.{entry}\n"
                f"Exit    : Rs.{ltp}\n"
                f"P&L     : Rs.{pnl}\n"
                f"Time    : {ist_str()}"
            )
        else:
            updated_rows.append(pos)

    # Update positions file
    closed_mask = pos_df["Status"] != "OPEN"
    closed_rows = pos_df[closed_mask]
    updated_df  = pd.concat(
        [closed_rows, pd.DataFrame(updated_rows)], ignore_index=True
    ).drop_duplicates(subset=["OrderId"], keep="last")
    save_open_positions(updated_df)

    # Update ledger
    if new_ledger_rows:
        updated_ledger = pd.concat(
            [ledger, pd.DataFrame(new_ledger_rows)], ignore_index=True)
        save_ledger(updated_ledger)

def eod_square_off():
    """Force-close all open positions at EOD."""
    if not is_eod():
        return
    pos_df = load_open_positions()
    ledger = load_ledger()
    open_p = pos_df[pos_df["Status"] == "OPEN"].copy()
    if open_p.empty:
        return

    new_ledger_rows = []
    for idx, pos in open_p.iterrows():
        ltp = get_ltp(pos["Ticker"]) or float(pos["EntryPrice"])
        entry = float(pos["EntryPrice"])
        qty   = int(pos["Qty"])
        side  = pos["Side"]
        pnl   = round((ltp - entry) * qty, 2) if side == "BUY" else round((entry - ltp) * qty, 2)
        pos_df.at[idx, "Status"] = "CLOSED"
        new_ledger_rows.append({
            "Date"      : pos["Date"],
            "Ticker"    : pos["Ticker"],
            "Strategy"  : pos["Strategy"],
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
        print(f"   [EOD] Square-off {pos['Ticker']} PnL={pnl}")

    save_open_positions(pos_df)
    if new_ledger_rows:
        updated_ledger = pd.concat([ledger, pd.DataFrame(new_ledger_rows)], ignore_index=True)
        save_ledger(updated_ledger)

    # Send EOD summary
    total_pnl = sum(r["PnL"] for r in new_ledger_rows)
    send_telegram(
        f"📊 <b>EOD Summary</b>\n"
        f"Positions closed : {len(new_ledger_rows)}\n"
        f"Total P&amp;L     : Rs.{round(total_pnl, 2)}\n"
        f"Mode             : {'LIVE' if LIVE_TRADING else 'PAPER'}\n"
        f"Time             : {ist_str()}"
    )

# ── Scan one stock ─────────────────────────────────────────
def scan_stock(ticker, nifty_trend="NEUTRAL"):
    data = fetch_intraday(ticker)
    if data is None:
        return []

    signals = []
    for strategy_fn in [scalp_signal, momentum_signal, swing_signal, orb_signal]:
        try:
            sig, price, sl, target, rsi, strat = strategy_fn(data)

            # Nifty 50 filter — suppress signals that fight the market
            if nifty_trend == "DOWN" and sig == "BUY":
                continue
            if nifty_trend == "UP" and sig == "SELL":
                continue

            if sig in ["BUY", "SELL"] and not already_alerted_today(ticker, strat):
                mark_alerted(ticker, strat)
                rr = round(abs(target - price) / abs(price - sl), 2) \
                     if sl and target and price != sl else 0
                signals.append({
                    "Time"     : ist_str(),
                    "Stock"    : ticker.replace(".NS", ""),
                    "Strategy" : strat,
                    "Signal"   : sig,
                    "Price"    : f"Rs.{price}",
                    "Stop Loss": f"Rs.{sl}" if sl else "-",
                    "Target"   : f"Rs.{target}" if target else "-",
                    "R:R"      : f"1:{rr}" if rr else "-",
                    "RSI"      : round(rsi, 1) if rsi else "-",
                })
        except:
            pass
    return signals

def save_to_log(signals):
    if not signals: return
    cols = ["Time","Stock","Strategy","Signal","Price","Stop Loss","Target","R:R","RSI"]
    log  = pd.read_csv(LOG_FILE) if os.path.exists(LOG_FILE) \
           else pd.DataFrame(columns=cols)
    pd.concat([log, pd.DataFrame(signals)], ignore_index=True).to_csv(LOG_FILE, index=False)

# ── Main scan ──────────────────────────────────────────────
def run_full_scan():
    if not is_market_open():
        print(f"[{ist_str()}] Market closed - skipping scan.")
        return

    # EOD check first
    if is_eod():
        print(f"[{ist_str()}] EOD — squaring off open positions.")
        eod_square_off()
        return

    # Skip bad trading windows
    if not is_good_trading_window():
        print(f"[{ist_str()}] Outside good trading window (first/last 15 min) - monitoring only.")
        monitor_and_close_positions()
        return

    # Check Nifty trend for market-wide filter
    nifty_trend = get_nifty_trend()
    print(f"[{ist_str()}] Nifty trend: {nifty_trend} | Scanning {len(ALL_STOCKS)} stocks...")

    all_signals = []
    for idx, ticker in enumerate(ALL_STOCKS):
        sigs = scan_stock(ticker, nifty_trend)
        all_signals.extend(sigs)
        time.sleep(0.3)
        if (idx + 1) % 50 == 0:
            print(f"   ...scanned {idx + 1}/{len(ALL_STOCKS)}")

    buys  = [s for s in all_signals if s["Signal"] == "BUY"]
    sells = [s for s in all_signals if s["Signal"] == "SELL"]

    # Alert YOUR Telegram
    for s in buys + sells:
        emoji = "BUY" if s["Signal"] == "BUY" else "SELL"
        send_telegram(
            f"{'BUY' if s['Signal']=='BUY' else 'SELL'} - {s['Stock']}\n"
            f"Strategy   : {s['Strategy']}\n"
            f"Price      : {s['Price']}\n"
            f"Stop Loss  : {s['Stop Loss']}\n"
            f"Target     : {s['Target']}\n"
            f"R:R        : {s['R:R']}\n"
            f"RSI        : {s['RSI']}\n"
            f"Nifty      : {nifty_trend}\n"
            f"Time       : {s['Time']}"
        )
        # Open paper position
        open_paper_position(s)

    save_to_log(all_signals)

    # Monitor existing open positions for SL/Target hits
    monitor_and_close_positions()

    # Founder summary
    send_telegram(
        f"Scan Done\n"
        f"BUY  : {len(buys)} | SELL: {len(sells)}\n"
        f"Nifty: {nifty_trend}\n"
        f"Mode : {'LIVE' if LIVE_TRADING else 'PAPER'}\n"
        f"{ist_str()}"
    )
    print(f"[{ist_str()}] Done - BUY:{len(buys)} SELL:{len(sells)} Nifty:{nifty_trend}")
    print("-" * 60)

# ── Entry point ─────────────────────────────────────────────
SINGLE_RUN = "--single-run" in sys.argv
print("=" * 60)
print("PROJECT 1 - INTRADAY SCANNER (Single User)")
print(f"Mode    : {'SINGLE RUN' if SINGLE_RUN else 'CONTINUOUS'}")
print(f"Capital : {'LIVE' if LIVE_TRADING else 'PAPER'} Rs.{CAPITAL:,.0f}")
print(f"Started : {ist_str()}")
print("=" * 60)

if SINGLE_RUN:
    run_full_scan()
    print(f"[{ist_str()}] Single run complete - exiting.")
else:
    send_telegram(f"PROJECT 1 Scheduler started\nEvery {SCAN_INTERVAL} min during market hours.\n{ist_str()}")
    run_full_scan()
    schedule.every(SCAN_INTERVAL).minutes.do(run_full_scan)
    while True:
        schedule.run_pending()
        time.sleep(20)
