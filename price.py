import requests
import time 
import json, yaml
import csv
from urllib.parse import parse_qs
from threading import Thread
from time import sleep
from datetime import datetime, timedelta

requests.urllib3.disable_warnings()

DELAY_TIME = 5
# END_TIME = datetime.now() + timedelta(minutes=10)
last_file_creation_time = datetime.now()
FILENAME_TEMPLATE = 'prices_{timestamp}.csv'
TIME_INTERVAL = 10

def get_new_filename():
    """Generates a new filename based on the current timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    return FILENAME_TEMPLATE.format(timestamp=timestamp)

def update_binance_ticker(prices: dict):
    global last_file_creation_time
    while True:
        current_time = datetime.now()
        if current_time - last_file_creation_time >= timedelta(minutes=TIME_INTERVAL):
            filename = get_new_filename()
            last_file_creation_time = current_time
        else:
            filename = FILENAME_TEMPLATE.format(timestamp=last_file_creation_time.strftime("%Y%m%d%H%M%S"))
        
        try:
            binance_price = fetch_data("https://api.binance.com/api/v3/ticker/price?symbol=ETHBTC")
            prices["Binance"] = float(binance_price["price"])
            with open(filename, 'a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["Binance", prices["Binance"], current_time])
        except Exception as e:
            print(f"Error in update_binance_ticker: {e}")
        finally:
            sleep(DELAY_TIME)

def update_kucoin_ticker(prices: dict):
    global last_file_creation_time
    while True:
        current_time = datetime.now()
        if current_time - last_file_creation_time >= timedelta(minutes=TIME_INTERVAL):
            filename = get_new_filename()
            last_file_creation_time = current_time
        else:
            filename = FILENAME_TEMPLATE.format(timestamp=last_file_creation_time.strftime("%Y%m%d%H%M%S"))
        
        try:
            kucoin_price = fetch_data_with_retry("https://api.kucoin.com/api/v1/market/stats?symbol=ETH-BTC")
            prices["Kucoin"] = float(kucoin_price["data"]["last"])
            with open(filename, 'a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["Kucoin", prices["Kucoin"], current_time])
        except Exception as e:
            print(f"Error in update_kucoin_ticker: {e}")
        finally:
            sleep(DELAY_TIME)
    
def update_wazirx_ticker(prices: dict):
    while True:
        try:
            wazirx_price = fetch_data("https://api.wazirx.com/sapi/v1/ticker/24hr?symbol=ethbtc")
            prices["Wazirx"] = float(wazirx_price["lastPrice"])
        except:
            pass
        finally:
            sleep(DELAY_TIME)

def update_coinhar_ticker(prices: dict):
    while True:
        try:
            coinhar_price = fetch_data("https://api.coinhar.io/api/v3/ticker?symbol=ETHBTC")
            prices["Coinhar"] = float(coinhar_price["price"])
        except:
            pass
        finally:
            sleep(DELAY_TIME)

def update_indodax_ticker(prices: dict):
    while True:
        try:
            indodax_eth_price = fetch_data("https://indodax.com/api/ticker/ethidr")
            indodax_btc_price = fetch_data("https://indodax.com/api/ticker/btcidr")
            prices["Indodax"] = float(indodax_eth_price["ticker"]["sell"]) / float(indodax_btc_price["ticker"]["buy"])
        except:
            pass
        finally:
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
    else:
        print(resp.text)

def fetch_data_with_retry(url, retries=5, backoff_factor=1):
    for i in range(retries):
        response = requests.get(url)
        if response.status_code == 429:
            wait = backoff_factor * (2 ** i)
            print(f"Rate limit exceeded. Retrying in {wait} seconds.")
            time.sleep(wait)
        else:
            return response.json()
    return None  # or raise an exception

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
