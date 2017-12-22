import requests
import argparse
from time import sleep as sleep
parser = argparse.ArgumentParser()
parser.add_argument("arg_currency", help="Please enter a valid cryptocurrency name")
args = parser.parse_args()
# Tick a single currency pair (xxx/usd) to the console.
if __name__ == '__main__':
    while True:
        api_response = dict(requests.get('https://api.bitfinex.com/v1/pubticker/' + args.arg_currency + 'usd').json())
        print(api_response.get('mid'))
        sleep(1)