import os
import asyncio
import aiohttp
import json
from aiohttp import web
import ccxt.async_support as ccxt
import hmac
import hashlib
from datetime import datetime, timedelta, timezone

DELTA_KEY = os.getenv("DELTA_API_KEY", "")
DELTA_SECRET = os.getenv("DELTA_API_SECRET", "")
CS_KEY = os.getenv("COINSWITCH_API_KEY", "")
CS_SECRET = os.getenv("COINSWITCH_API_SECRET", "")

SPREAD_THRESHOLD = float(os.getenv("SPREAD_THRESHOLD", "0.005"))
TOTAL_COST_BUFFER = float(os.getenv("COST_BUFFER", "0.0025"))
TARGET_LOT_SIZE = float(os.getenv("TARGET_LOT_SIZE", "0.001"))
LIVE_EXECUTION = os.getenv("LIVE_EXECUTION", "False").lower() in ("true", "1")

# Diagnostic store to expose via web browser
DIAGNOSTIC_STATE = {
    "status": "Initializing",
    "coinswitch_last_response": "None",
    "delta_spot": 0.0,
    "scanned_symbols": []
}


class CoinSwitchClient:
    def __init__(self, key, secret):
        self.base_url = "https://coinswitch.co/trade/api/v2"
        self.key = key
        self.secret = secret

    def _get_signature(self, method, endpoint, payload=""):
        msg = f"{method}{endpoint}{payload}".encode("utf-8")
        return hmac.new(self.secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()

    async def test_instruments(self, session):
        """Fetches active contracts/symbols from CoinSwitch"""
        endpoint = "/derivatives/instruments"
        try:
            async with session.get(self.base_url + endpoint, timeout=5) as resp:
                text = await resp.text()
                DIAGNOSTIC_STATE["coinswitch_instruments_probe"] = f"Status: {resp.status} | Body: {text[:300]}"
                return text
        except Exception as e:
            DIAGNOSTIC_STATE["coinswitch_instruments_probe"] = f"Error: {str(e)}"
            return str(e)

    async def fetch_order_book(self, session, symbol):
        endpoint = f"/derivatives/orderbook?symbol={symbol}"
        headers = {
            "Content-Type": "application/json",
            "X-AUTH-APIKEY": self.key,
            "X-AUTH-SIGNATURE": self._get_signature("GET", endpoint)
        }
        try:
            async with session.get(self.base_url + endpoint, headers=headers, timeout=5) as resp:
                text = await resp.text()
                DIAGNOSTIC_STATE["coinswitch_last_response"] = f"Status: {resp.status} | URL: {endpoint} | Res: {text[:150]}"
                if resp.status == 200:
                    data = json.loads(text)
                    return {
                        "bids": data.get("bids", []),
                        "asks": data.get("asks", [])
                    }
        except Exception as e:
            DIAGNOSTIC_STATE["coinswitch_last_response"] = f"Fetch Error: {str(e)}"
        return {"bids": [], "asks": []}


def calculate_vwap_and_depth(orders, required_qty):
    if not orders:
        return None
    total_qty = 0.0
    total_cost = 0.0
    for order in orders:
        price = float(order[0])
        qty = float(order[1])
        fill = min(qty, required_qty - total_qty)
        total_cost += fill * price
        total_qty += fill
        if total_qty >= required_qty:
            break

    if total_qty < required_qty:
        return None
    return total_cost / total_qty


def get_daily_expiry_code():
    now_utc = datetime.now(timezone.utc)
    if now_utc.hour >= 8:
        target_date = now_utc + timedelta(days=1)
    else:
        target_date = now_utc
    return target_date.strftime("%y%m%d")


async def arbitrage_bot_loop(app):
    print("[SYSTEM] Starting Real Engine with Diagnostic Inspector...", flush=True)

    delta = ccxt.delta({
        "apiKey": DELTA_KEY,
        "secret": DELTA_SECRET,
        "enableRateLimit": True,
    })
    cs_client = CoinSwitchClient(CS_KEY, CS_SECRET)

    try:
        await delta.load_markets()
        async with aiohttp.ClientSession() as cs_session:
            # First check instruments
            await cs_client.test_instruments(cs_session)

            while True:
                try:
                    spot_ticker = await delta.fetch_ticker("BTC/USDT:USDT")
                    spot_price = spot_ticker.get("last", 80000)
                    atm_strike = int(round(spot_price / 1000.0) * 1000)
                    expiry_code = get_daily_expiry_code()

                    DIAGNOSTIC_STATE["status"] = "Active Scanning"
                    DIAGNOSTIC_STATE["delta_spot"] = spot_price

                    strikes_to_scan = [atm_strike]
                    for strike in strikes_to_scan:
                        for opt_type in ["C", "P"]:
                            delta_symbol = f"BTC/USDT:USDT-{expiry_code}-{strike}-{opt_type}"
                            cs_symbol = f"BTC-{expiry_code}-{strike}-{opt_type}"

                            delta_task = delta.fetch_order_book(delta_symbol)
                            cs_task = cs_client.fetch_order_book(cs_session, cs_symbol)

                            delta_book, cs_book = await asyncio.gather(delta_task, cs_task, return_exceptions=True)

                            d_vwap_ask = calculate_vwap_and_depth(delta_book.get('asks', []), TARGET_LOT_SIZE) if not isinstance(delta_book, Exception) else None
                            cs_vwap_bid = calculate_vwap_and_depth(cs_book.get('bids', []), TARGET_LOT_SIZE) if not isinstance(cs_book, Exception) else None

                            print(f"[BOOK DATA] {strike}{opt_type} -> Delta Ask: {d_vwap_ask} | CS Bid: {cs_vwap_bid}", flush=True)

                except Exception as e:
                    print(f"[LOOP ERROR] {e}", flush=True)

                await asyncio.sleep(4)

    except asyncio.CancelledError:
        pass
    finally:
        await delta.close()


async def start_background_task(app):
    app['bot_task'] = asyncio.create_task(arbitrage_bot_loop(app))

async def handle_debug(request):
    """Web Browser Diagnostic View"""
    return web.json_response(DIAGNOSTIC_STATE)

app = web.Application()
app.router.add_get('/', handle_debug)
app.on_startup.append(start_background_task)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    web.run_app(app, host='0.0.0.0', port=port)

    
