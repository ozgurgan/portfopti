import configparser
import psycopg2
import requests
import retrying
import sys

config = configparser.ConfigParser()
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
TABLE_CHECK_SQL = """SELECT EXISTS(SELECT 1 FROM information_schema.tables
WHERE table_catalog='temp'
AND table_schema='public'
AND table_name='ticker');"""
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
timestamp NUMERIC);"""
TABLE_INSERT_SQL = """INSERT INTO ticker (fromcurrency, tocurrency, mid, bid, ask, last_price, low, high, volume, timestamp)
VALUES (%(fromcurrency)s, %(tocurrency)s, %(mid)s, %(bid)s, %(ask)s, %(last_price)s, %(low)s, %(high)s, %(volume)s, %(timestamp)s);"""


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
    with open("pricegetter1.ini", "r") as configfile:
        config.read_file(configfile)
except FileNotFoundError:  # EAFP
    with open("pricegetter1.ini", "w") as configfile:
        configfile.write(DEFAULT_CONFIGURATION)
    config.read_string(DEFAULT_CONFIGURATION)

# Connect to database
try:
    conn = psycopg2.connect(**config["DATABASE"])
except psycopg2.OperationalError:
    print(f"""Unable to connect to the database with connection parameters: {dict(config["DATABASE"])}""")
    sys.exit(1)

# Check if our table exists, create it if it doesn't
with conn, conn.cursor() as cursor:
    cursor.execute(TABLE_CHECK_SQL)
    table_present = cursor.fetchone()[0]
    if not table_present:
        cursor.execute(TABLE_CREATE_SQL)

# Read pairs from the configuration and fetch the information from the API, insert it into our table
with conn, conn.cursor() as cursor:
    for fromcurrency, tocurrencies in config["CURRENCYPAIRS"].items():
        for tocurrency in tocurrencies.split(", "):
            # This is retried automatically a couple times with exponential backoff
            api_response = get_currency_pair_info(fromcurrency, tocurrency)
            if api_response:
                # Maybe the service is down or our currency pair doesn't exist, so this cannot be guaranteed
                api_response.update(fromcurrency=fromcurrency, tocurrency=tocurrency)
                cursor.execute(TABLE_INSERT_SQL, dict(api_response))
