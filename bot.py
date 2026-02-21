import asyncio, websockets, json, telegram, httpx, sys
import pandas as pd
import pandas_ta as ta

# ==================== CONFIG ====================
TELEGRAM_TOKEN = '8349229275:AAGNWV2A0_Pf9LhlwZCczeBoMcUaJL2shFg'
CHAT_ID        = '1950462171'
SYMBOL         = 'SOLUSDT'

RSI_P, WMA_P = 40, 15

# ==================== STAGE CONFIG ====================
STAGE1_R        = 1.5   # trail SL only, no exit
STAGE2_R        = 2.2   # exit 50%, trail SL
STAGE3_R        = 3.0   # exit remaining 50%, close trade

STAGE1_SL_TRAIL = 0.8   # SL → +0.8R after stage 1
STAGE2_SL_TRAIL = 1.5   # SL → +1.5R after stage 2

# ==================== STATS ====================
stats = {
    "balance"      : 100,
    "risk_percent" : 0.02,

    "total_trades" : 00,
    "win_s3"       : 00,   # full target hit (Stage 3)
    "win_partial"  : 00,   # net positive but SL hit before Stage 3
    "loss_sl"      : 00,   # SL hit before any exit

    "reached_s1"   : 0,
    "reached_s2"   : 0,
    "reached_s3"   : 0,

    "sl_points"    : 0.0,  # cumulative price distance lost on SL trades
    "tp_points"    : 0.0,  # cumulative price distance captured on exits
}

active_trade = None
http_client  = httpx.AsyncClient()

# ==================== LOCKS ====================
# entry_lock  : prevents two simultaneous candle-close events
#               from both seeing active_trade=None and opening two trades
# closing_lock: prevents two simultaneous ticks from both triggering
#               close_trade() at the same moment
entry_lock   = asyncio.Lock()
closing_lock = asyncio.Lock()

# ==================== DATA & INDICATORS ====================

async def fetch_indicators():
    """
    FIX — repainting:
      iloc[-2] = just-closed candle (confirmed, final values)
      iloc[-3] = candle before that (for crossover comparison)
      iloc[-1] is intentionally skipped — it is the NEW forming candle
      that exists by the time our HTTP response arrives after the close event.

    FIX — RSI warmup:
      limit=200 gives RSI(40) enough history to stabilize.
      limit=100 puts RSI in its initialization zone, causing divergence
      from TradingView/Binance chart values.
    """
    try:
        url    = "https://api.binance.com/api/v3/klines"
        params = {'symbol': SYMBOL, 'interval': '3m', 'limit': 200}
        resp   = await http_client.get(url, params=params)
        data   = resp.json()

        df          = pd.DataFrame(data, columns=['ts','o','h','l','c','v','ts_e','q','n','tb','tq','i'])
        df['close'] = df['c'].astype(float)

        rsi = ta.rsi(df['close'], length=RSI_P)
        wma = ta.wma(rsi,         length=WMA_P)

        return rsi.iloc[-2], wma.iloc[-2], rsi.iloc[-3], wma.iloc[-3]

    except Exception as e:
        print(f"[fetch_indicators ERROR] {e}")
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
    Called on every WebSocket tick while a trade is open.
    Uses closing_lock to guarantee close_trade() is called
    at most once per trade, even if multiple ticks arrive
    simultaneously during an async await.
    """
    global active_trade, stats

    # Primary guard — no trade open
    if not active_trade:
        return

    # Secondary guard — trade is already being closed
    # (closing_lock is held by another tick's close_trade call)
    if active_trade.get('closing'):
        return

    entry      = active_trade['entry']
    initial_sl = active_trade['initial_sl']
    risk_dist  = entry - initial_sl
    if risk_dist <= 0:
        return

    rr = (price - entry) / risk_dist

    # ── STAGE 1: 1.5R ──────────────────────────────────────
    # Guard: active_trade['s1'] flag ensures this block runs
    # exactly once regardless of how many ticks see rr >= 1.5
    if not active_trade['s1'] and rr >= STAGE1_R:
        active_trade['s1'] = True
        active_trade['sl'] = entry + (risk_dist * STAGE1_SL_TRAIL)
        stats['reached_s1'] += 1

        await tg(bot,
            f"🟢 *STAGE 1 HIT — {SYMBOL}*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📍 Price:       `${price:.4f}`\n"
            f"🎯 Target hit:  `1.5R`\n"
            f"🛡️ SL trailed:  `+0.8R → ${active_trade['sl']:.4f}`\n"
            f"📦 Position:    `100% still open`\n"
            f"ℹ️ _Waiting for Stage 2 at 2.2R..._"
        )

    # ── STAGE 2: 2.2R ──────────────────────────────────────
    # Guard: active_trade['s2'] flag ensures this block runs
    # exactly once. Balance, tp_points, realized_pnl all
    # update here and only here for the 50% exit.
    elif not active_trade['s2'] and rr >= STAGE2_R:
        active_trade['s2'] = True
        active_trade['sl'] = entry + (risk_dist * STAGE2_SL_TRAIL)

        realized                      = (active_trade['risk_usd'] * 0.5) * rr
        active_trade['realized_pnl']  = realized
        active_trade['s2_exit_price'] = price

        stats['balance']    += realized
        stats['tp_points']  += (price - entry) * 0.5   # 50% of position
        stats['reached_s2'] += 1

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

    # ── EXIT CONDITIONS ─────────────────────────────────────
    # closing_lock ensures only ONE tick can ever enter
    # close_trade(). Any other tick that arrives during the
    # await inside close_trade() will see the lock is held
    # and skip silently. This is the core one-trade guarantee.

    if rr >= STAGE3_R:
        async with closing_lock:
            # Re-check inside lock — another tick may have
            # already closed the trade while we waited
            if active_trade and not active_trade.get('closing'):
                active_trade['closing'] = True
                await close_trade(price, "S3_TARGET", bot)

    elif price <= active_trade['sl']:
        async with closing_lock:
            if active_trade and not active_trade.get('closing'):
                active_trade['closing'] = True
                if active_trade['s2']:
                    await close_trade(price, "SL_AFTER_S2", bot)
                elif active_trade['s1']:
                    await close_trade(price, "SL_AFTER_S1", bot)
                else:
                    await close_trade(price, "SL_PURE", bot)


async def close_trade(exit_price, reason, bot):
    """
    Called at most once per trade (enforced by closing_lock +
    active_trade['closing'] flag in monitor_trade).
    All stats updates happen here atomically before any await.
    """
    global active_trade, stats

    entry      = active_trade['entry']
    initial_sl = active_trade['initial_sl']
    risk_dist  = entry - initial_sl
    rr         = (exit_price - entry) / risk_dist

    remaining_mult = 0.5 if active_trade['s2'] else 1.0
    remaining_pnl  = (active_trade['risk_usd'] * remaining_mult) * rr
    total_pnl      = remaining_pnl + active_trade.get('realized_pnl', 0)

    # ── Update all stats BEFORE any await ──────────────────
    # Doing all mutations here synchronously means no other
    # coroutine can interleave and see a half-updated state.
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

    # ── Nullify trade BEFORE await ──────────────────────────
    # Setting active_trade = None here (before the Telegram
    # await) means any new candle close that arrives while
    # the message is sending will correctly see no open trade
    # and can safely open a new one via entry_lock.
    active_trade = None

    pnl_sign = "+" if total_pnl >= 0 else ""
    msg = (
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
    await tg(bot, msg)


# ==================== ENTRY HANDLER ====================

async def handle_candle_close(price, closed_low, bot):
    """
    Wrapped in entry_lock so that even if two candle-close
    events fire nearly simultaneously (network burst, reconnect
    overlap), only one can check active_trade and open a trade
    at a time. The second one will wait at the lock, then see
    active_trade is already set and skip.
    """
    global active_trade

    async with entry_lock:
        # Already in a trade — skip
        if active_trade:
            return

        rsi, wma, prsi, pwma = await fetch_indicators()

        # FIX: `rsi is not None` not `if rsi` — RSI=0.0 is valid
        # and falsy, so `if rsi` would silently skip a real signal
        if rsi is None:
            print("[BOT] Indicator fetch failed, skipping candle.")
            return

        crossover = (prsi <= pwma) and (rsi > wma)
        if not crossover:
            return

        # FIX: closed candle low comes from WebSocket event data['k']['l']
        # NOT from a separate REST call with limit=1, which returns the
        # currently forming candle (wrong values, wrong candle entirely)
        low_val   = closed_low * 0.9995
        risk_dist = price - low_val

        if risk_dist <= 0:
            print("[BOT] Invalid risk distance, skipping.")
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

    # Send alert OUTSIDE the lock so we don't hold it during network I/O
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

        while True:
            try:
                print(f"[BOT] Connecting...")
                async with websockets.connect(ws_url) as ws:
                    print(f"[BOT] Connected. Monitoring {SYMBOL} 3M.")
                    await tg(bot, f"🤖 *Bot Started*\n└ Monitoring `{SYMBOL}` on `3M` timeframe")

                    while True:
                        raw  = await ws.recv()
                        data = json.loads(raw)

                        if 'k' not in data:
                            continue

                        price = float(data['k']['c'])

                        # Monitor open trade on every tick
                        if active_trade:
                            await monitor_trade(price, bot)

                        # Handle candle close
                        if data['k']['x']:
                            closed_low = float(data['k']['l'])
                            await handle_candle_close(price, closed_low, bot)

            except websockets.ConnectionClosed as e:
                print(f"[BOT] WebSocket closed: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"[BOT] Error: {e}. Reconnecting in 10s...")
                await asyncio.sleep(10)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[BOT] Stopped.")
        sys.exit()
