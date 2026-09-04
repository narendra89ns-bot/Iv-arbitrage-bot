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

# Configurable Risk & Cost Parameters
SPREAD_THRESHOLD = float(os.getenv("SPREAD_THRESHOLD", "0.005"))  # 0.5% Net Profit Target
TOTAL_COST_BUFFER = float(os.getenv("COST_BUFFER", "0.0025"))     # 0.25% (Exchange Fees + Slippage)
TARGET_LOT_SIZE = float(os.getenv("TARGET_LOT_SIZE", "0.05"))     # Target Contract Qty (BTC)


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


def calculate_vwap_and_depth(orders, required_qty):
    """Calculates Volume-Weighted Average Price across depth. Returns None if liquidity is thin."""
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
        return None  # Insufficient liquidity to fill target lot size
    return total_cost / total_qty


def get_next_friday_expiry_code():
    today = datetime.utcnow()
    days_ahead = (4 - today.weekday() + 7) % 7
    if days_ahead == 0 and today.hour >= 8:
        days_ahead = 7
    target_date = today + timedelta(days=days_ahead)
    return target_date.strftime("%y%m%d")


async def arbitrage_bot_loop(app):
    print("[SYSTEM] Starting Dynamic Strike Engine with Slippage Protection...", flush=True)

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

                            # Calculate Slippage-Adjusted Effective Execution Prices
                            d_vwap_ask = calculate_vwap_and_depth(delta_book.get('asks', []), TARGET_LOT_SIZE)
                            d_vwap_bid = calculate_vwap_and_depth(delta_book.get('bids', []), TARGET_LOT_SIZE)
                            cs_vwap_ask = calculate_vwap_and_depth(cs_book.get('asks', []), TARGET_LOT_SIZE)
                            cs_vwap_bid = calculate_vwap_and_depth(cs_book.get('bids', []), TARGET_LOT_SIZE)

                            # Route 1: Buy Delta -> Sell CoinSwitch
                            if d_vwap_ask and cs_vwap_bid:
                                gross_spread_1 = (cs_vwap_bid - d_vwap_ask) / d_vwap_ask
                                net_spread_1 = gross_spread_1 - TOTAL_COST_BUFFER

                                if net_spread_1 >= SPREAD_THRESHOLD:
                                    print(
                                        f"[NET PROFIT SIGNAL] {delta_symbol} | "
                                        f"Buy Delta @ {d_vwap_ask:.2f} | Sell CS @ {cs_vwap_bid:.2f} | "
                                        f"Net: {net_spread_1 * 100:.2f}% (Gross: {gross_spread_1 * 100:.2f}%)",
                                        flush=True
                                    )

                            # Route 2: Buy CoinSwitch -> Sell Delta
                            if cs_vwap_ask and d_vwap_bid:
                                gross_spread_2 = (d_vwap_bid - cs_vwap_ask) / cs_vwap_ask
                                net_spread_2 = gross_spread_2 - TOTAL_COST_BUFFER

                                if net_spread_2 >= SPREAD_THRESHOLD:
                                    print(
                                        f"[NET PROFIT SIGNAL] {delta_symbol} | "
                                        f"Buy CS @ {cs_vwap_ask:.2f} | Sell Delta @ {d_vwap_bid:.2f} | "
                                        f"Net: {net_spread_2 * 100:.2f}% (Gross: {gross_spread_2 * 100:.2f}%)",
                                        flush=True
                                    )

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
    return web.Response(text="Delta-CoinSwitch Dynamic Arbitrage Worker with Slippage Guard is Active")

app = web.Application()
app.router.add_get('/', handle)
app.on_startup.append(start_background_task)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    web.run_app(app, host='0.0.0.0', port=port)
