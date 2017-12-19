import configparser
from pathlib import Path
import psycopg2
import requests
import retrying
import sys

import time

config = configparser.ConfigParser()

CONFIGURATION_FILE = Path(__file__).parent / "pricegetter1.ini"
DEFAULT_CONFIGURATION = """[GENERAL]
tickerurl = https://api.bitfinex.com/v1/pubticker/{fromcurrency}{tocurrency}
riskquotient = 0.1

[CURRENCYPAIRS]
btc = usd, eth, eur
eth = btc, usd
ltc = usd
iot = usd

[DATABASE]
dbname = temp
user = pricegetter1
host = localhost
password = MYPASSWORD"""
TABLE_CHECK_SQL = """SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'ticker';"""
TABLE_CREATE_SQL = """CREATE TABLE ticker (id BIGSERIAL PRIMARY KEY,
fromcurrency VARCHAR(5),
tocurrency VARCHAR(5),
mid NUMERIC,
bid NUMERIC,
ask NUMERIC,
last_price NUMERIC,
low NUMERIC,
high NUMERIC,
volume NUMERIC,
timestamp TIMESTAMP);"""
TABLE_INSERT_SQL = """INSERT INTO ticker (fromcurrency, tocurrency, mid, bid, ask, last_price, low, high, volume, timestamp)
VALUES (%(fromcurrency)s, %(tocurrency)s, %(mid)s, %(bid)s, %(ask)s, %(last_price)s, %(low)s, %(high)s, %(volume)s, to_timestamp(%(timestamp)s));"""

@retrying.retry(wait_exponential_multiplier=1000, wait_exponential_max=120000)
def get_currency_pair_info(fromcurrency, tocurrency):
    """
    Fetches the information about a currency pair ("symbol" in BitFinex parlance). Retries and backs off exponentially.
    :param fromcurrency: Currency to be converted from
    :param tocurrency: Currency to be converted to
    :return: The dictionary about the currency pair, None if the call fails
    """
    URL = config["GENERAL"]["TICKERURL"].format(fromcurrency=fromcurrency, tocurrency=tocurrency)
    # print(URL)
    api_response = dict(requests.get(URL).json())
    # print(api_response)
    if "message" in api_response:
        print("Possible error, API call returned:", api_response)
        print("\tfor URL:", URL)
        return None
    else:
        return api_response


# Read the configuration file if it exists, create it if it doesn't
try:
    with open(CONFIGURATION_FILE, "r") as configfile:
        config.read_file(configfile)
except FileNotFoundError:  # EAFP
    with open(CONFIGURATION_FILE, "w") as configfile:
        configfile.write(DEFAULT_CONFIGURATION)
    config.read_string(DEFAULT_CONFIGURATION)

# Connect to database
try:
    conn = psycopg2.connect(**config["DATABASE"])
except psycopg2.OperationalError:
    print("""Unable to connect to the database with connection parameters: {}""".format(dict(config["DATABASE"])))
    sys.exit(1)

# Check if our table exists, create it if it doesn't
with conn, conn.cursor() as cursor:
    cursor.execute(TABLE_CHECK_SQL)
    table_present = cursor.fetchone()[0]
    if not table_present:
        cursor.execute(TABLE_CREATE_SQL)

# Read pairs from the configuration and fetch the information from the API, insert it into our table
for fromcurrency, tocurrencies in config["CURRENCYPAIRS"].items():
    for tocurrency in tocurrencies.split(", "):
        # This is retried automatically a couple times with exponential backoff
        api_response = get_currency_pair_info(fromcurrency, tocurrency)
        # Avoid getting banned by slowing down
        sleep = time.sleep(1)
        # Maybe the service is down or our currency pair doesn't exist, so this cannot be guaranteed
        if api_response:
            api_response.update(fromcurrency=fromcurrency, tocurrency=tocurrency)
            with conn, conn.cursor() as cursor:
                cursor.execute(TABLE_INSERT_SQL, dict(api_response))
