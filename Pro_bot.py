import os
import asyncio
from aiohttp import web

async def arbitrage_bot_loop(app):
    while True:
        try:
            print("Arbitrage bot checking order book & skew...")
            # Aapka main arbitrage logic yahan aayega
        except Exception as e:
            print(f"Error in bot loop: {e}")
        await asyncio.sleep(60)  # Har 60 second me check karega

async def start_background_task(app):
    app['bot_task'] = asyncio.create_task(arbitrage_bot_loop(app))

async def handle(request):
    return web.Response(text="IV Arbitrage Bot is active and running 24/7!")

app = web.Application()
app.router.add_get("/", handle)
app.on_startup.append(start_background_task)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    web.run_app(app, host="0.0.0.0", port=port)
