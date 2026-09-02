import asyncio
import aiohttp
import logging

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# API Endpoints & Webhook URLs (Apne hisab se credentials update kar sakte hain)
DELTA_API_URL = "https://api.delta.exchange/v2/tickers"
COINSWITCH_API_URL = "https://api.coinswitch.co/v2/ticker"
WEBHOOK_URL = "https://your-mirrorpip-webhook-url.com/webhook"  # Replace with actual Mirrorpip webhook

PROFIT_THRESHOLD_PCT = 0.5  # 0.5% minimum profit threshold

async def fetch_data(session, url):
    try:
        async with session.get(url, timeout=5) as response:
            if response.status == 200:
                return await response.json()
    except Exception as e:
        logging.error(f"Error fetching data from {url}: {e}")
    return None

async def trigger_webhook(session, payload):
    try:
        async with session.post(WEBHOOK_URL, json=payload, timeout=5) as response:
            if response.status == 200:
                logging.info("Arbitrage webhook triggered successfully!")
            else:
                logging.warning(f"Webhook failed with status: {response.status}")
    except Exception as e:
        logging.error(f"Error triggering webhook: {e}")

async def monitor_arbitrage():
    logging.info("Starting 24/7 Async Arbitrage Bot...")
    async with aiohttp.ClientSession() as session:
        while True:
            # Fetch prices asynchronously from both exchanges
            delta_task = fetch_data(session, DELTA_API_URL)
            coinswitch_task = fetch_data(session, COINSWITCH_API_URL)
            
            delta_data, coinswitch_data = await asyncio.gather(delta_task, coinswitch_task)
            
            if delta_data and coinswitch_data:
                try:
                    # Parse prices (Note: Adjust keys based on exact API response structures)
                    delta_price = float(delta_data.get('result', {}).get('btc_price', 0))
                    coinswitch_price = float(coinswitch_data.get('data', {}).get('btc_price', 0))
                    
                    if delta_price > 0 and coinswitch_price > 0:
                        diff_pct = abs(delta_price - coinswitch_price) / min(delta_price, coinswitch_price) * 100
                        logging.info(f"Delta: {delta_price} | CoinSwitch: {coinswitch_price} | Diff: {diff_pct:.2f}%")
                        
                        if diff_pct >= PROFIT_THRESHOLD_PCT:
                            payload = {
                                "exchange_a": "Delta",
                                "price_a": delta_price,
                                "exchange_b": "CoinSwitch",
                                "price_b": coinswitch_price,
                                "difference_pct": diff_pct
                            }
                            await trigger_webhook(session, payload)
                except Exception as parse_err:
                    logging.error(f"Parsing error: {parse_err}")
            
            # Check every 3 seconds asynchronously without blocking
            await asyncio.sleep(3)

if __name__ == "__main__":
    try:
        asyncio.run(monitor_arbitrage())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user.")
