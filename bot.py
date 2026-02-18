import asyncio, websockets, json, telegram, httpx, sys, logging
import pandas as pd
import pandas_ta as ta

# ==================== CONFIG & LOGGING ====================
TELEGRAM_TOKEN = '8349229275:AAGNWV2A0_Pf9LhlwZCczeBoMcUaJL2shFg'
CHAT_ID = '1950462171'
SYMBOL = 'SOLUSDT'

RSI_P, WMA_P = 40, 15

stats = {
    "balance": 121.0, 
    "risk_percent": 0.02, 
    "total_trades": 124,
    "wins_final": 15, 
    "wins_trailed": 40, 
    "losses": 69
}

active_trade = None
http_client = httpx.AsyncClient()

# ==================== DATA & SIGNAL ====================

async def fetch_indicators():
    try:
        url = "https://api.binance.com/api/v3/klines"
        # CHANGE 1: interval set to 3m
        params = {'symbol': SYMBOL, 'interval': '3m', 'limit': 100}
        resp = await http_client.get(url, params=params)
        data = resp.json()
        
        df = pd.DataFrame(data, columns=['ts','o','h','l','c','v','ts_e','q','n','tb','tq','i'])
        df['close'] = df['c'].astype(float)
        
        rsi = ta.rsi(df['close'], length=RSI_P)
        wma = ta.wma(rsi, length=WMA_P)
        
        return rsi.iloc[-1], wma.iloc[-1], rsi.iloc[-2], wma.iloc[-2]
    except Exception as e:
        print(f"Error fetching indicators: {e}")
        return None, None, None, None

# ==================== ENGINE (STAGES) ====================

async def monitor_trade(price, bot):
    global active_trade, stats
    if not active_trade: return

    risk_dist = active_trade['entry'] - active_trade['initial_sl']
    if risk_dist <= 0: return 
    rr = (price - active_trade['entry']) / risk_dist

    # --- STAGE 1: LOCK PROFIT (1.5R) ---
    if not active_trade['s1'] and rr >= 1.5:
        active_trade['sl'] = active_trade['entry'] + (risk_dist * 0.8)
        active_trade['s1'] = True
        await bot.send_message(CHAT_ID, "🟢 *STAGE 1: PROFIT LOCKED*\n└ SL moved to +0.8R", parse_mode='Markdown')

    # --- STAGE 2: PARTIAL EXIT (2.2R) ---
    elif not active_trade['s2'] and rr >= 2.2:
        active_trade['s2'] = True
        active_trade['sl'] = active_trade['entry'] + (risk_dist * 1.5)
        
        realized = (active_trade['risk_usd'] * 0.5) * rr
        active_trade['realized_pnl'] = realized
        stats['balance'] += realized
        
        await bot.send_message(CHAT_ID, f"💰 *STAGE 2: 50% EXIT*\n└ Realized: `+{realized:.2f} USDT`\n└ SL Trailed to +1.5R", parse_mode='Markdown')

    # --- EXIT CONDITIONS ---
    if rr >= 3.0:
        await close_trade(price, "🎯 TARGET HIT (3.0R)", bot)
    elif price <= active_trade['sl']:
        reason = "🛡️ TRAILED SL HIT" if active_trade['s1'] else "🛑 INITIAL SL HIT"
        await close_trade(price, reason, bot)

async def close_trade(exit_price, reason, bot):
    global active_trade, stats
    mult = 0.5 if active_trade['s2'] else 1.0
    risk_dist = active_trade['entry'] - active_trade['initial_sl']
    pnl = (active_trade['risk_usd'] * mult) * ((exit_price - active_trade['entry']) / risk_dist)
    total_pnl = pnl + active_trade.get('realized_pnl', 0)
    
    stats['balance'] += pnl
    stats['total_trades'] += 1
    if "TARGET" in reason: stats['wins_final'] += 1
    elif total_pnl > 0: stats['wins_trailed'] += 1
    else: stats['losses'] += 1

    msg = (f"🏁 *TRADE CLOSED: {reason}*\n━━━━━━━━━━━━━━━━━━\n"
           f"💵 *Total PnL:* `+{total_pnl:.2f} USDT`\n"
           f"🏦 *New Balance:* `${stats['balance']:.2f}`\n\n"
           f"📊 *Stats:* 🎯 {stats['wins_final']} | 🛡️ {stats['wins_trailed']} | 🛑 {stats['losses']}\n"
           f"📈 *Win Rate:* `{( (stats['wins_final']+stats['wins_trailed'])/stats['total_trades'] )*100:.1f}%`")
    
    await bot.send_message(CHAT_ID, msg, parse_mode='Markdown')
    active_trade = None

# ==================== MAIN LOOP ====================

async def main():
    global active_trade
    async with telegram.Bot(TELEGRAM_TOKEN) as bot:
        # CHANGE 2: Connect to the 3m kline stream
        url = f"wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@kline_3m"
        async with websockets.connect(url) as ws:
            print(f"Bot Started. Monitoring {SYMBOL} on 3M Timeframe...")
            while True:
                raw_data = await ws.recv()
                data = json.loads(raw_data)
                
                if 'k' in data:
                    price = float(data['k']['c'])
                    if active_trade: await monitor_trade(price, bot)
                    
                    # 'x' is True when the 3m candle closes
                    if data['k']['x']: 
                        rsi, wma, prsi, pwma = await fetch_indicators()
                        if rsi and not active_trade and prsi <= pwma and rsi > wma:
                            # CHANGE 3: Fetch the low of the closed 3m candle
                            api_res = await http_client.get(f"https://api.binance.com/api/v3/klines?symbol={SYMBOL}&interval=3m&limit=1")
                            low_val = float(api_res.json()[0][3]) * 0.9995
                            
                            active_trade = {
                                'entry': price, 'initial_sl': low_val, 'sl': low_val, 
                                'risk_usd': stats['balance'] * stats['risk_percent'],
                                's1': False, 's2': False, 'realized_pnl': 0
                            }
                            await bot.send_message(CHAT_ID, f"🚀 *3M LONG SIGNAL: {SYMBOL}*\n💰 Entry: `${price:.2f}`\n🛑 Stop: `${low_val:.2f}`", parse_mode='Markdown')

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit()
