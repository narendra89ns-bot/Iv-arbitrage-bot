
import os
import asyncio
import aiohttp
from aiohttp import web
import ccxt.async_support as ccxt
import hmac
import hashlib

# Configuration from Environment Variables
DELTA_KEY = os.getenv("DELTA_API_KEY", "")
DELTA_SECRET = os.getenv("DELTA_API_SECRET", "")
CS_KEY = os.getenv("COINSWITCH_API_KEY", "")
CS_SECRET = os.getenv("COINSWITCH_API_SECRET", "")

SPREAD_THRESHOLD = float(os.getenv("SPREAD_THRESHOLD", "0.02"))  # 2% spread threshold


class CoinSwitchClient:
    def __init__(self, key, secret):
        self.base_url = "https://api-trading.coinswitch.co"
        self.key = key
        self.secret = secret

    def _get_signature(self, method, endpoint, payload=""):
        msg = f"{method}{endpoint}{payload}".encode("utf-8")
        return hmac.new(self.secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()

    async def fetch_order_book(self, session, symbol):
        endpoint = f"/v1/derivatives/orderbook?symbol={symbol}"
        headers = {
            "Content-Type": "application/json",
            "X-AUTH-APIKEY": self.key,
            "X-AUTH-SIGNATURE": self._get_signature("GET", endpoint)
        }
        try:
            async with session.get(self.base_url + endpoint, headers=headers, timeout=5) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {
                        "bids": data.get("bids", []),
                        "asks": data.get("asks", [])
                    }
        except Exception:
            pass
        return {"bids": [], "asks": []}


async def arbitrage_bot_loop(app):
    print("[SYSTEM] Starting Delta & CoinSwitch Arbitrage Engine...")

    # Exchange instance single initialization
    delta = ccxt.delta({
        "apiKey": DELTA_KEY,
        "secret": DELTA_SECRET,
        "enableRateLimit": True,
    })
    cs_client = CoinSwitchClient(CS_KEY, CS_SECRET)

    # Active target symbol pair
    target_delta_symbol = "BTC/USDT:USDT-260904-80000-C"
    target_cs_symbol = "BTC-260904-80000-C"

    try:
        async with aiohttp.ClientSession() as cs_session:
            while True:
                try:
                    delta_task = delta.fetch_order_book(target_delta_symbol)
                    cs_task = cs_client.fetch_order_book(cs_session, target_cs_symbol)

                    delta_book, cs_book = await asyncio.gather(delta_task, cs_task, return_exceptions=True)

                    if isinstance(delta_book, Exception) or isinstance(cs_book, Exception):
                        print(f"[WARN] Polling issue | Delta: {delta_book} | CS: {cs_book}")
                        await asyncio.sleep(2)
                        continue

                    # Best Ask (Buying) and Best Bid (Selling)
                    d_bid = delta_book['bids'][0][0] if delta_book.get('bids') else 0
                    d_ask = delta_book['asks'][0][0] if delta_book.get('asks') else float('inf')

                    cs_bid = cs_book['bids'][0][0] if cs_book.get('bids') else 0
                    cs_ask = cs_book['asks'][0][0] if cs_book.get('asks') else float('inf')

                    # Route 1: Delta par Buy, CoinSwitch par Sell
                    if d_ask > 0 and d_ask != float('inf') and cs_bid > 0:
                        spread_1 = (cs_bid - d_ask) / d_ask
                        if spread_1 >= SPREAD_THRESHOLD:
                            print(f"\n[OPPORTUNITY] Route 1: Buy Delta @ {d_ask} | Sell CS @ {cs_bid} | Spread: {spread_1*100:.2f}%")

                    # Route 2: CoinSwitch par Buy, Delta par Sell
                    if cs_ask > 0 and cs_ask != float('inf') and d_bid > 0:
                        spread_2 = (d_bid - cs_ask) / cs_ask
                        if spread_2 >= SPREAD_THRESHOLD:
                            print(f"\n[OPPORTUNITY] Route 2: Buy CS @ {cs_ask} | Sell Delta @ {d_bid} | Spread: {spread_2*100:.2f}%")

                except Exception as e:
                    print(f"[LOOP ERROR] {e}")

                await asyncio.sleep(1)

    except asyncio.CancelledError:
        print("[SYSTEM] Task received shutdown signal.")
    finally:
        # Solves 'Unclosed connector' and CCXT crash
        await delta.close()
        print("[SYSTEM] Delta session closed successfully.")


async def start_background_task(app):
    app['bot_task'] = asyncio.create_task(arbitrage_bot_loop(app))

async def handle(request):
    return web.Response(text="Delta-CoinSwitch Arbitrage Worker is Active")

app = web.Application()
app.router.add_get('/', handle)
app.on_startup.append(start_background_task)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    web.run_app(app, host='0.0.0.0', port=port)
