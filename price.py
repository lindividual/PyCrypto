import requests
import json, yaml
from urllib.parse import parse_qs
from threading import Thread
from time import sleep
from datetime import datetime

requests.urllib3.disable_warnings()

DELAY_TIME = 5

def update_binance_ticker(prices: dict):
    while True:
        try:
            binance_price = fetch_data("https://api.binance.com/api/v3/ticker/price?symbol=ETHBTC")
            prices["Binance"] = float(binance_price["price"])
        except:
            pass

        sleep(DELAY_TIME)

def update_kucoin_ticker(prices: dict):
    while True:
        try:
            kucoin_price = fetch_data("https://api.kucoin.com/api/v1/market/stats?symbol=ETH-BTC")
            prices["Kucoin"] = float(kucoin_price["data"]["last"])
        except:
            pass

        sleep(DELAY_TIME)

def update_wazirx_ticker(prices: dict):
    while True:
        try:
            wazirx_price = fetch_data("https://api.wazirx.com/sapi/v1/ticker/24hr?symbol=ethbtc")
            prices["Wazirx"] = float(wazirx_price["lastPrice"])
        except:
            pass

        sleep(DELAY_TIME)

def update_coinhar_ticker(prices: dict):
    while True:
        try:
            coinhar_price = fetch_data("https://api.coinhar.io/api/v3/ticker?symbol=ETHBTC")
            prices["Coinhar"] = float(coinhar_price["price"])
        except:
            pass

        sleep(DELAY_TIME)

def update_indodax_ticker(prices: dict):
    while True:
        try:
            indodax_eth_price = fetch_data("https://indodax.com/api/ticker/ethidr")
            indodax_btc_price = fetch_data("https://indodax.com/api/ticker/btcidr")
            prices["Indodax"] = float(indodax_eth_price["ticker"]["sell"]) / float(indodax_btc_price["ticker"]["buy"])
        except:
            pass

        sleep(DELAY_TIME)

def fetch_data(url: str):
    resp = requests.get(url, timeout=10)
    content_type = resp.headers["Content-Type"]

    if resp.status_code != 200:
        raise requests.exceptions.RequestException(resp.status_code)

    if content_type.startswith("application/json"):
        return json.loads(resp.text)
    
    elif content_type.startswith("application/x-www-form-urlencoded"):
        return parse_qs(resp.text)
    
    elif content_type.startswith("application/yaml"):
        return yaml.load(resp.text, Loader=yaml.Loader)

def calculate_average_price(prices: dict):
    print("ETH/BTC Rate History")
    print("%-20s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s" % ("Timestamp", "Binance", "Kucoin", "Wazirx", "Coinhar", "Indodax", "Highest", "Lowest", "Average"))
    failed_count = 0
    while True:
        sleep(DELAY_TIME)

        if failed_count >= 10:
            print("Check your network connection please.")
            exit(0)

        if len(prices) < 3:
            failed_count += 1
            continue

        values = list(prices.values())
        values.sort()

        now = datetime.now()
        data = [now.strftime("%d/%m/%Y %H:%M:%S")]

        for company in ["Binance", "Kucoin", "Wazirx", "Coinhar", "Indodax"]:
            if company in prices:
                data.append(f"{prices[company]:.6f}")
            else:
                data.append("-")

        data.append(f"{values[-1]:.6f}")
        data.append(f"{values[0]:.6f}")
        data.append(f"{(sum(values[1:-1]) / (len(values)-2)):.6f}")

        print("%-20s %-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s" % tuple(data))

def main():
    prices = {}

    binance_worker = Thread(target=update_binance_ticker, args=(prices,))
    binance_worker.start()
    kucoin_worker = Thread(target=update_kucoin_ticker, args=(prices,))
    kucoin_worker.start()
    wazirx_worker = Thread(target=update_wazirx_ticker, args=(prices,))
    wazirx_worker.start()
    coinhar_worker = Thread(target=update_coinhar_ticker, args=(prices,))
    coinhar_worker.start()
    indodax_worker = Thread(target=update_indodax_ticker, args=(prices,))
    indodax_worker.start()

    calculate_average_price(prices)

if __name__ == '__main__':
    main()
