import asyncio, websockets, json, telegram, httpx, sys
import pandas as pd
import pandas_ta as ta
from datetime import datetime

# ==================== CONFIG ====================
TELEGRAM_TOKEN = '8349229275:AAGNWV2A0_Pf9LhlwZCczeBoMcUaJL2shFg'
CHAT_ID        = '1950462171'
SYMBOL         = 'SOLUSDT'

RSI_P, WMA_P = 40, 15

# ==================== STAGE CONFIG ====================
STAGE1_R        = 1.5
STAGE2_R        = 2.2
STAGE3_R        = 3.0
STAGE1_SL_TRAIL = 0.8
STAGE2_SL_TRAIL = 1.5

# ==================== STATS ====================
# ── Fill these with your real values before running ──
stats = {
    "balance"      : 100,
    "risk_percent" : 0.02,
    "total_trades" : 00,
    "win_s3"       : 00,
    "win_partial"  : 00,
    "loss_sl"      : 00,
    "reached_s1"   : 0,
    "reached_s2"   : 0,
    "reached_s3"   : 0,
    "sl_points"    : 0.0,
    "tp_points"    : 0.0,
}

active_trade = None
http_client  = httpx.AsyncClient()
entry_lock   = asyncio.Lock()
closing_lock = asyncio.Lock()

# ==================== LOGGER ====================

def now():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

def log(tag, msg):
    """
    Structured console log. Every event prints with:
      [HH:MM:SS.mmm] [TAG] message
    Tags:
      WS     → raw WebSocket events
      CANDLE → candle close events
      SIGNAL → entry signal checks (crossover logic)
      INDIC  → raw indicator values every candle close
      TRADE  → trade open/close/stage events
      LOCK   → lock acquire/release events
      ERROR  → any exception
    """
    print(f"[{now()}] [{tag:<7}] {msg}")

# ==================== DATA & INDICATORS ====================

async def fetch_indicators():
    """
    FIX 1 — limit=1000 (RSI convergence):
      RSI(40) needs ~400+ candles to stabilize and match TradingView.
      Previous limit=200 was in the initialization zone causing RSI
      values to diverge by 1-3 points. 1000 is the Binance max and
      gives full convergence identical to TradingView.

    FIX 2 — iloc[-2] / iloc[-3] (no forming candle):
      After the 3M close event fires and our HTTP round-trip completes,
      Binance REST has already opened a new forming candle at iloc[-1].
      iloc[-2] = just-closed confirmed candle  ← current
      iloc[-3] = candle before that            ← previous
      iloc[-1] = new forming candle            ← never use

    NOTE: @kline_3m stream already matches interval='3m' so there
    is no live repainting issue — signal fires exactly once per
    3M close, identical to TradingView behaviour.
    """
    try:
        url    = "https://api.binance.com/api/v3/klines"
        # FIX 1: 1000 candles for full RSI(40) convergence
        params = {'symbol': SYMBOL, 'interval': '3m', 'limit': 1000}

        log("INDIC", f"Fetching {SYMBOL} 3m klines (limit=1000)...")
        resp = await http_client.get(url, params=params)
        data = resp.json()
        log("INDIC", f"Received {len(data)} candles from Binance REST")

        df          = pd.DataFrame(data, columns=['ts','o','h','l','c','v','ts_e','q','n','tb','tq','i'])
        df['close'] = df['c'].astype(float)

        rsi = ta.rsi(df['close'], length=RSI_P)
        wma = ta.wma(rsi,         length=WMA_P)

        # Log last 3 confirmed candles — compare these directly with TradingView
        log("INDIC", "─── Last 3 confirmed candles (iloc -4, -3, -2) ───")
        for i, idx in enumerate([-4, -3, -2], start=1):
            ts_ms  = int(df['ts'].iloc[idx])
            ts_str = datetime.utcfromtimestamp(ts_ms / 1000).strftime("%H:%M")
            log("INDIC", f"  Candle {i}: {ts_str} UTC | close={df['close'].iloc[idx]:.4f} | RSI={rsi.iloc[idx]:.4f} | WMA={wma.iloc[idx]:.4f}")

        # FIX 2: use iloc[-2] and iloc[-3], skip forming candle at iloc[-1]
        curr_rsi = rsi.iloc[-2]
        curr_wma = wma.iloc[-2]
        prev_rsi = rsi.iloc[-3]
        prev_wma = wma.iloc[-3]

        log("INDIC", "─── Crossover check ───")
        log("INDIC", f"  PREV: RSI={prev_rsi:.4f}  WMA={prev_wma:.4f}  | RSI<=WMA? {prev_rsi <= prev_wma}")
        log("INDIC", f"  CURR: RSI={curr_rsi:.4f}  WMA={curr_wma:.4f}  | RSI>WMA?  {curr_rsi > curr_wma}")
        log("INDIC", f"  CROSSOVER = {(prev_rsi <= prev_wma) and (curr_rsi > curr_wma)}")

        return curr_rsi, curr_wma, prev_rsi, prev_wma

    except Exception as e:
        log("ERROR", f"fetch_indicators failed: {e}")
        return None, None, None, None

# ==================== TELEGRAM ====================

async def tg(bot, msg):
    await bot.send_message(CHAT_ID, msg, parse_mode='Markdown')

def _stats_footer():
    t  = stats['total_trades']
    w3 = stats['win_s3']
    wp = stats['win_partial']
    ls = stats['loss_sl']
    wr = ((w3 + wp) / t * 100) if t > 0 else 0
    return (
        f"\n━━━━━━━━━━━━━━━━━━\n"
        f"📊 *SESSION STATS*\n"
        f"├ 🎯 Full Win (S3): `{w3}`\n"
        f"├ 🔶 Partial Win:   `{wp}`\n"
        f"├ 🛑 SL Loss:       `{ls}`\n"
        f"├ 📈 Win Rate:      `{wr:.1f}%`\n"
        f"├ 🏦 Balance:       `${stats['balance']:.2f}`\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🔢 *STAGE COUNTS*\n"
        f"├ S1 Reached: `{stats['reached_s1']}`\n"
        f"├ S2 Reached: `{stats['reached_s2']}`\n"
        f"└ S3 Reached: `{stats['reached_s3']}`\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📐 *POINTS*\n"
        f"├ SL pts lost: `{stats['sl_points']:.4f}`\n"
        f"└ TP pts won:  `{stats['tp_points']:.4f}`"
    )

# ==================== TRADE ENGINE ====================

async def monitor_trade(price, bot):
    """
    Runs on every WebSocket tick while trade is open.
    s1/s2 boolean flags guarantee each stage fires exactly once.
    closing_lock guarantees close_trade() fires at most once per trade.
    """
    global active_trade, stats

    if not active_trade:
        return
    if active_trade.get('closing'):
        return

    entry      = active_trade['entry']
    initial_sl = active_trade['initial_sl']
    risk_dist  = entry - initial_sl
    if risk_dist <= 0:
        return

    rr = (price - entry) / risk_dist

    # ── STAGE 1: 1.5R → trail SL to +0.8R, no position exit ──
    if not active_trade['s1'] and rr >= STAGE1_R:
        active_trade['s1'] = True
        active_trade['sl'] = entry + (risk_dist * STAGE1_SL_TRAIL)
        stats['reached_s1'] += 1
        log("TRADE", f"STAGE 1 HIT | price={price:.4f} | rr={rr:.2f}R | new SL={active_trade['sl']:.4f}")

        await tg(bot,
            f"🟢 *STAGE 1 HIT — {SYMBOL}*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📍 Price:       `${price:.4f}`\n"
            f"🎯 Target hit:  `1.5R`\n"
            f"🛡️ SL trailed:  `+0.8R → ${active_trade['sl']:.4f}`\n"
            f"📦 Position:    `100% still open`\n"
            f"ℹ️ _Waiting for Stage 2 at 2.2R..._"
        )

    # ── STAGE 2: 2.2R → exit 50%, trail SL to +1.5R ──────────
    elif not active_trade['s2'] and rr >= STAGE2_R:
        active_trade['s2'] = True
        active_trade['sl'] = entry + (risk_dist * STAGE2_SL_TRAIL)

        realized                      = (active_trade['risk_usd'] * 0.5) * rr
        active_trade['realized_pnl']  = realized
        active_trade['s2_exit_price'] = price

        stats['balance']    += realized
        stats['tp_points']  += (price - entry) * 0.5
        stats['reached_s2'] += 1
        log("TRADE", f"STAGE 2 HIT | price={price:.4f} | rr={rr:.2f}R | realized={realized:.2f} USDT | new SL={active_trade['sl']:.4f}")

        await tg(bot,
            f"💰 *STAGE 2 HIT — {SYMBOL}*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📍 Price:        `${price:.4f}`\n"
            f"🎯 Target hit:   `2.2R`\n"
            f"📤 Exited:       `50% of position`\n"
            f"💵 Realized PnL: `+{realized:.2f} USDT`\n"
            f"📐 Points:       `+{price - entry:.4f}`\n"
            f"🛡️ SL trailed:   `+1.5R → ${active_trade['sl']:.4f}`\n"
            f"📦 Remaining:    `50% still open`\n"
            f"ℹ️ _Waiting for Stage 3 at 3.0R..._"
        )

    # ── EXIT CONDITIONS ────────────────────────────────────────
    if rr >= STAGE3_R:
        async with closing_lock:
            if active_trade and not active_trade.get('closing'):
                active_trade['closing'] = True
                log("TRADE", f"STAGE 3 HIT — closing | price={price:.4f} | rr={rr:.2f}R")
                await close_trade(price, "S3_TARGET", bot)

    elif price <= active_trade['sl']:
        async with closing_lock:
            if active_trade and not active_trade.get('closing'):
                active_trade['closing'] = True
                if active_trade['s2']:
                    reason = "SL_AFTER_S2"
                elif active_trade['s1']:
                    reason = "SL_AFTER_S1"
                else:
                    reason = "SL_PURE"
                log("TRADE", f"SL HIT — {reason} | price={price:.4f} | sl={active_trade['sl']:.4f} | rr={rr:.2f}R")
                await close_trade(price, reason, bot)


async def close_trade(exit_price, reason, bot):
    """
    All stat mutations happen synchronously before any await.
    active_trade = None is set BEFORE await tg() so any new candle
    close arriving during the Telegram send correctly sees no open trade.
    """
    global active_trade, stats

    entry          = active_trade['entry']
    initial_sl     = active_trade['initial_sl']
    risk_dist      = entry - initial_sl
    rr             = (exit_price - entry) / risk_dist
    remaining_mult = 0.5 if active_trade['s2'] else 1.0
    remaining_pnl  = (active_trade['risk_usd'] * remaining_mult) * rr
    total_pnl      = remaining_pnl + active_trade.get('realized_pnl', 0)

    # ── All mutations before any await ────────────────────────
    stats['balance']      += remaining_pnl
    stats['total_trades'] += 1

    if reason == "S3_TARGET":
        stats['win_s3']      += 1
        stats['reached_s3']  += 1
        stats['tp_points']   += (exit_price - entry) * 0.5
        outcome_emoji = "🎯"
        outcome_label = "FULL WIN — Stage 3 Target"

    elif reason == "SL_AFTER_S2":
        stats['win_partial'] += 1
        stats['sl_points']   += (entry - exit_price) * 0.5
        outcome_emoji = "🔶"
        outcome_label = "PARTIAL WIN — SL after Stage 2"

    elif reason == "SL_AFTER_S1":
        if total_pnl > 0:
            stats['win_partial'] += 1
            outcome_emoji = "🔶"
            outcome_label = "PARTIAL WIN — SL after Stage 1 (+0.8R)"
        else:
            stats['loss_sl']   += 1
            outcome_emoji = "🛑"
            outcome_label = "LOSS — SL after Stage 1"
        stats['sl_points'] += abs(entry - exit_price)

    else:  # SL_PURE
        stats['loss_sl']   += 1
        stats['sl_points'] += risk_dist
        outcome_emoji = "🛑"
        outcome_label = "LOSS — Initial SL Hit"

    log("TRADE", f"CLOSED — {outcome_label} | exit={exit_price:.4f} | rr={rr:.2f}R | pnl={total_pnl:+.2f} USDT | balance={stats['balance']:.2f}")

    # ── Nullify BEFORE await so next signal isn't blocked ─────
    active_trade = None

    pnl_sign = "+" if total_pnl >= 0 else ""
    await tg(bot,
        f"{outcome_emoji} *TRADE CLOSED — {outcome_label}*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry:        `${entry:.4f}`\n"
        f"🚪 Exit:         `${exit_price:.4f}`\n"
        f"📐 Points:       `{exit_price - entry:+.4f}`\n"
        f"📊 R achieved:   `{rr:.2f}R`\n"
        f"💵 Total PnL:    `{pnl_sign}{total_pnl:.2f} USDT`\n"
        f"🏦 New Balance:  `${stats['balance']:.2f}`"
        + _stats_footer()
    )

# ==================== ENTRY HANDLER ====================

async def handle_candle_close(price, closed_low, bot):
    """
    Called only on confirmed 3M candle close (data['k']['x'] = True).
    entry_lock prevents two simultaneous close events both opening a trade.

    closed_low = data['k']['l'] from WebSocket event — the low of the
    candle that JUST closed, final and accurate. No extra REST call needed.
    """
    global active_trade

    log("CANDLE", f"3M candle closed | close={price:.4f} | low={closed_low:.4f}")

    async with entry_lock:
        log("LOCK", "entry_lock acquired")

        if active_trade:
            log("SIGNAL", f"SKIP — trade already open (entry={active_trade['entry']:.4f})")
            log("LOCK", "entry_lock released")
            return

        rsi, wma, prsi, pwma = await fetch_indicators()

        # `rsi is not None` not `if rsi` — RSI=0.0 is valid but falsy in Python.
        # `if rsi` would silently skip a real signal in that edge case.
        if rsi is None:
            log("SIGNAL", "SKIP — indicator fetch returned None (API error)")
            log("LOCK", "entry_lock released")
            return

        # Crossover: RSI crossed UP through WMA on the just-closed candle
        # prev candle: RSI was at or below WMA  (prsi <= pwma)
        # curr candle: RSI is now above WMA     (rsi  >  wma)
        crossover = (prsi <= pwma) and (rsi > wma)

        log("SIGNAL", f"RSI={rsi:.4f} | WMA={wma:.4f} | prevRSI={prsi:.4f} | prevWMA={pwma:.4f}")
        log("SIGNAL", f"prsi<=pwma: {prsi <= pwma} | rsi>wma: {rsi > wma} | CROSSOVER: {crossover}")

        if not crossover:
            log("SIGNAL", "SKIP — no crossover this candle")
            log("LOCK", "entry_lock released")
            return

        low_val   = closed_low * 0.9995   # small buffer below candle low
        risk_dist = price - low_val

        if risk_dist <= 0:
            log("SIGNAL", f"SKIP — invalid risk distance ({risk_dist:.6f})")
            log("LOCK", "entry_lock released")
            return

        active_trade = {
            'entry'        : price,
            'initial_sl'   : low_val,
            'sl'           : low_val,
            'risk_usd'     : stats['balance'] * stats['risk_percent'],
            's1'           : False,
            's2'           : False,
            'closing'      : False,
            'realized_pnl' : 0.0,
            's2_exit_price': None,
        }

        log("TRADE", f"TRADE OPENED | entry={price:.4f} | sl={low_val:.4f} | risk={active_trade['risk_usd']:.2f} USDT")
        log("LOCK", "entry_lock released")

    # Send alert OUTSIDE lock — never hold lock during network I/O
    await tg(bot,
        f"🚀 *LONG SIGNAL — {SYMBOL}*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry:       `${price:.4f}`\n"
        f"🛑 Stop Loss:   `${low_val:.4f}`\n"
        f"📐 Risk (pts):  `{risk_dist:.4f}`\n"
        f"💵 Risk (USDT): `${active_trade['risk_usd']:.2f}`\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"🎯 Stage 1:     `${price + risk_dist * STAGE1_R:.4f}` (1.5R — SL trail)\n"
        f"🎯 Stage 2:     `${price + risk_dist * STAGE2_R:.4f}` (2.2R — exit 50%)\n"
        f"🎯 Stage 3:     `${price + risk_dist * STAGE3_R:.4f}` (3.0R — exit 50%)\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📊 RSI: `{rsi:.2f}` | WMA: `{wma:.2f}`"
    )

# ==================== MAIN LOOP ====================

async def main():
    async with telegram.Bot(TELEGRAM_TOKEN) as bot:
        ws_url = f"wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@kline_3m"

        # Outer loop reconnects automatically on any disconnect
        while True:
            try:
                log("WS", f"Connecting to {ws_url}...")
                async with websockets.connect(ws_url) as ws:
                    log("WS", "Connected.")
                    await tg(bot,
                        f"🤖 *Bot Started*\n"
                        f"├ Symbol:    `{SYMBOL}`\n"
                        f"├ Timeframe: `3M`\n"
                        f"├ RSI:       `{RSI_P}` | WMA: `{WMA_P}`\n"
                        f"└ Balance:   `${stats['balance']:.2f}`"
                    )

                    while True:
                        raw  = await ws.recv()
                        data = json.loads(raw)

                        if 'k' not in data:
                            continue

                        price    = float(data['k']['c'])
                        is_close = data['k']['x']

                        # Monitor open trade on every tick
                        if active_trade:
                            await monitor_trade(price, bot)

                        # Only act on confirmed 3M candle close
                        if is_close:
                            closed_low = float(data['k']['l'])
                            await handle_candle_close(price, closed_low, bot)

            except websockets.ConnectionClosed as e:
                log("WS", f"Connection closed: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                log("ERROR", f"Unexpected error: {e}. Reconnecting in 10s...")
                await asyncio.sleep(10)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("WS", "Bot stopped by user.")
        sys.exit()
