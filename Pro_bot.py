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

SPREAD_THRESHOLD = float(os.getenv("SPREAD_THRESHOLD", "-1.0"))  
TOTAL_COST_BUFFER = float(os.getenv("COST_BUFFER", "0.0025"))     
TARGET_LOT_SIZE = float(os.getenv("TARGET_LOT_SIZE", "0.001"))     
LIVE_EXECUTION = os.getenv("LIVE_EXECUTION", "False").lower() in ("true", "1")


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

    async def place_order(self, session, symbol, side, price, qty):
        endpoint = "/v1/derivatives/order"
        payload = json.dumps({
            "symbol": symbol,
            "side": side.lower(),
            "order_type": "limit",
            "price": price,
            "quantity": qty,
            "time_in_force": "IOC"
        })
        headers = {
            "Content-Type": "application/json",
            "X-AUTH-APIKEY": self.key,
            "X-AUTH-SIGNATURE": self._get_signature("POST", endpoint, payload)
        }
        try:
            async with session.post(self.base_url + endpoint, headers=headers, data=payload, timeout=5) as resp:
                return await resp.json()
        except Exception as e:
            return {"error": str(e)}


def calculate_vwap_and_depth(orders, required_qty):
    if not orders:
        return None
    try:
        # Best available level extract karein testing mock ke liye
        return float(orders[0][0])
    except Exception:
        return None


def get_daily_expiry_code():
    now_utc = datetime.now(timezone.utc)
    if now_utc.hour >= 8:
        target_date = now_utc + timedelta(days=1)
    else:
        target_date = now_utc
    return target_date.strftime("%y%m%d")


async def execute_arbitrage(delta, cs_client, cs_session, delta_sym, cs_sym, d_side, cs_side, d_price, cs_price, qty):
    print(f"\n[EXECUTION TRIGGERED] Delta: {d_side.upper()} @ {d_price} | CS: {cs_side.upper()} @ {cs_price} | Qty: {qty}", flush=True)

    if not LIVE_EXECUTION:
        print("[DRY-RUN] Orders simulated. Set LIVE_EXECUTION=True in Render to send actual orders.\n", flush=True)
        return

    delta_task = delta.create_order(
        symbol=delta_sym,
        type='limit',
        side=d_side,
        amount=qty,
        price=d_price,
        params={'timeInForce': 'ioc'}
    )
    cs_task = cs_client.place_order(
        session=cs_session,
        symbol=cs_sym,
        side=cs_side,
        price=cs_price,
        qty=qty
    )

    results = await asyncio.gather(delta_task, cs_task, return_exceptions=True)
    print(f"[EXECUTION RESULT] Delta: {results[0]} | CoinSwitch: {results[1]}", flush=True)


async def arbitrage_bot_loop(app):
    print(f"[SYSTEM] Starting Arbitrage Engine (Live Orders: {LIVE_EXECUTION})...", flush=True)

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
                    expiry_code = get_daily_expiry_code()

                    print(f"[SCAN] Spot: {spot_price:.1f} | ATM: {atm_strike} | Daily Exp: {expiry_code}", flush=True)

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

                            d_vwap_ask = calculate_vwap_and_depth(delta_book.get('asks', []), TARGET_LOT_SIZE)
                            d_vwap_bid = calculate_vwap_and_depth(delta_book.get('bids', []), TARGET_LOT_SIZE)
                            cs_vwap_ask = calculate_vwap_and_depth(cs_book.get('asks', []), TARGET_LOT_SIZE)
                            cs_vwap_bid = calculate_vwap_and_depth(cs_book.get('bids', []), TARGET_LOT_SIZE)

                            # Route 1: Buy Delta -> Sell CoinSwitch
                            if d_vwap_ask and cs_vwap_bid:
                                gross_spread = (cs_vwap_bid - d_vwap_ask) / d_vwap_ask
                                net_spread = gross_spread - TOTAL_COST_BUFFER

                                if net_spread >= SPREAD_THRESHOLD:
                                    await execute_arbitrage(
                                        delta, cs_client, cs_session,
                                        delta_symbol, cs_symbol,
                                        d_side="buy", cs_side="sell",
                                        d_price=d_vwap_ask, cs_price=cs_vwap_bid,
                                        qty=TARGET_LOT_SIZE
                                    )
                                    await asyncio.sleep(4)

                            # Route 2: Buy CoinSwitch -> Sell Delta
                            if cs_vwap_ask and d_vwap_bid:
                                gross_spread = (d_vwap_bid - cs_vwap_ask) / cs_vwap_ask
                                net_spread = gross_spread - TOTAL_COST_BUFFER

                                if net_spread >= SPREAD_THRESHOLD:
                                    await execute_arbitrage(
                                        delta, cs_client, cs_session,
                                        delta_symbol, cs_symbol,
                                        d_side="sell", cs_side="buy",
                                        d_price=d_vwap_bid, cs_price=cs_vwap_ask,
                                        qty=TARGET_LOT_SIZE
                                    )
                                    await asyncio.sleep(4)

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
    mode = "LIVE TRADING" if LIVE_EXECUTION else "SIMULATION / DRY-RUN"
    return web.Response(text=f"Delta-CoinSwitch Dynamic Arbitrage Worker ({mode}) is Active")

app = web.Application()
app.router.add_get('/', handle)
app.on_startup.append(start_background_task)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    web.run_app(app, host='0.0.0.0', port=port)
