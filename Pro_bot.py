import os
import asyncio
import aiohttp
from aiohttp import web
import ccxt.async_support as ccxt
import hmac
import hashlib
from datetime import datetime, timedelta

DELTA_KEY = os.getenv("DELTA_API_KEY", "")
DELTA_SECRET = os.getenv("DELTA_API_SECRET", "")
CS_KEY = os.getenv("COINSWITCH_API_KEY", "")
CS_SECRET = os.getenv("COINSWITCH_API_SECRET", "")
SPREAD_THRESHOLD = float(os.getenv("SPREAD_THRESHOLD", "0.0"))


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


def get_next_friday_expiry_code():
    today = datetime.utcnow()
    days_ahead = (4 - today.weekday() + 7) % 7
    if days_ahead == 0 and today.hour >= 8:
        days_ahead = 7
    target_date = today + timedelta(days=days_ahead)
    return target_date.strftime("%y%m%d")


async def arbitrage_bot_loop(app):
    print("[SYSTEM] Starting Dynamic Strike Engine...", flush=True)

    delta = ccxt.delta({
        "apiKey": DELTA_KEY,
        "secret": DELTA_SECRET,
        "enableRateLimit": True,
    })
    cs_client = CoinSwitchClient(CS_KEY, CS_SECRET)

    try:
        await delta.load_markets()
        print("[SYSTEM] Delta markets loaded successfully.", flush=True)

        async with aiohttp.ClientSession() as cs_session:
            while True:
                try:
                    spot_ticker = await delta.fetch_ticker("BTC/USDT:USDT")
                    spot_price = spot_ticker.get("last", 80000)
                    atm_strike = int(round(spot_price / 1000.0) * 1000)
                    expiry_code = get_next_friday_expiry_code()

                    print(f"[SCAN] Spot: {spot_price:.1f} | ATM: {atm_strike} | Exp: {expiry_code}", flush=True)

                    strikes_to_scan = [atm_strike - 1000, atm_strike, atm_strike + 1000]

                    for strike in strikes_to_scan:
                        for opt_type in ["C", "P"]:
                            delta_symbol = f"BTC/USDT:USDT-{expiry_code}-{strike}-{opt_type}"
                            cs_symbol = f"BTC-{expiry_code}-{strike}-{opt_type}"

                            delta_task = delta.fetch_order_book(delta_symbol)
                            cs_task = cs_client.fetch_order_book(cs_session, cs_symbol)

                            delta_book, cs_book = await asyncio.gather(delta_task, cs_task, return_exceptions=True)

                            if isinstance(delta_book, Exception) or isinstance(cs_book, Exception):
                                continue

                            d_bid = delta_book['bids'][0][0] if delta_book.get('bids') else 0
                            d_ask = delta_book['asks'][0][0] if delta_book.get('asks') else float('inf')
                            cs_bid = cs_book['bids'][0][0] if cs_book.get('bids') else 0
                            cs_ask = cs_book['asks'][0][0] if cs_book.get('asks') else float('inf')

                            if d_ask > 0 and d_ask != float('inf') and cs_bid > 0:
                                spread_1 = (cs_bid - d_ask) / d_ask
                                if spread_1 >= SPREAD_THRESHOLD:
                                    print(f"[SIGNAL] {delta_symbol} | Buy Delta @ {d_ask} | Sell CS @ {cs_bid} | Spread: {spread_1*100:.2f}%", flush=True)

                            if cs_ask > 0 and cs_ask != float('inf') and d_bid > 0:
                                spread_2 = (d_bid - cs_ask) / cs_ask
                                if spread_2 >= SPREAD_THRESHOLD:
                                    print(f"[SIGNAL] {delta_symbol} | Buy CS @ {cs_ask} | Sell Delta @ {d_bid} | Spread: {spread_2*100:.2f}%", flush=True)

                except Exception as e:
                    print(f"[LOOP ERROR] {e}", flush=True)

                await asyncio.sleep(3)

    except asyncio.CancelledError:
        print("[SYSTEM] Engine cancelled.", flush=True)
    finally:
        await delta.close()
        print("[SYSTEM] Delta session closed.", flush=True)


async def start_background_task(app):
    app['bot_task'] = asyncio.create_task(arbitrage_bot_loop(app))

async def handle(request):
    return web.Response(text="Delta-CoinSwitch Dynamic Arbitrage Worker is Active")

app = web.Application()
app.router.add_get('/', handle)
app.on_startup.append(start_background_task)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    web.run_app(app, host='0.0.0.0', port=port)
