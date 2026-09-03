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
            print("Connecting to Delta and loading markets...")
            await delta.load_markets()
            
            options_symbols = [s for s in delta.symbols if 'BTC' in s and ('C' in s or 'P' in s) and '-' in s]
            
            if options_symbols:
                # Ek sample option ka poora ticker print karke keys check karte hain
                sample_symbol = options_symbols[0]
                print(f"Inspecting full ticker for: {sample_symbol}")
                ticker = await delta.fetch_ticker(sample_symbol)
                print(f"Full Ticker Output: {ticker}")
                
        except Exception as e:
            print(f"Error in debug loop: {e}")
        finally:
            await delta.close()
            
        await asyncio.sleep(60)

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
