import os
import asyncio
from aiohttp import web
import ccxt.async_support as ccxt

async def arbitrage_bot_loop(app):
    while True:
        delta = ccxt.delta({
            'enableRateLimit': True,
        })
        try:
            print("Scanning Bitcoin options for High/Low IV spread test...")
            await delta.load_markets()
            
            options_symbols = [s for s in delta.symbols if 'BTC-' in s and ('C' in s or 'P' in s)]
            
            iv_data = []
            for symbol in options_symbols[:15]:
                try:
                    ticker = await delta.fetch_ticker(symbol)
                    mark_iv = ticker.get('info', {}).get('mark_iv')
                    if mark_iv is not None:
                        iv_data.append({'symbol': symbol, 'iv': float(mark_iv), 'bid': ticker.get('bid'), 'ask': ticker.get('ask')})
                except Exception:
                    continue
            
            if iv_data:
                iv_data.sort(key=lambda x: x['iv'], reverse=True)
                
                highest_iv_option = iv_data[0]
                lowest_iv_option = iv_data[-1]
                
                iv_spread = highest_iv_option['iv'] - lowest_iv_option['iv']
                
                print(f"-> SELL High IV: {highest_iv_option['symbol']} (IV: {highest_iv_option['iv']}%)")
                print(f"-> BUY Low IV: {lowest_iv_option['symbol']} (IV: {lowest_iv_option['iv']}%)")
                print(f"-> IV Spread: {iv_spread:.2f}%")
                
                if iv_spread > 5.0:
                    print(f"TEST SIGNAL: Spread threshold met! Executing simulated spread trade.")
            
        except Exception as e:
            print(f"Error in IV test loop: {e}")
        finally:
            # Session ko cleanly close karne ke liye taaki warning na aaye
            await delta.close()
            
        await asyncio.sleep(30)

async def start_background_task(app):
    app['bot_task'] = asyncio.create_task(arbitrage_bot_loop(app))

async def handle(request):
    return web.Response(text="IV Arbitrage Test Bot is active and running 24/7!")

app = web.Application()
app.router.add_get("/", handle)
app.on_startup.append(start_background_task)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    web.run_app(app, host="0.0.0.0", port=port)
