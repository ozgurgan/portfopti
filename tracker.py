import requests
import configparser
import psycopg2
import os

config = configparser.ConfigParser()

def get_cfg(string):
    config.read('config.ini')
    return config['DATABASE'][string]

def sql_init():
    try:
        connect_str = "dbname='" + get_cfg('dbname') + "' user='" + get_cfg('user') + "' host='" + get_cfg('host') + "' " + \
                      "password='" + get_cfg('password') + "'"
        conn = psycopg2.connect(connect_str)
        cursor = conn.cursor()

        cursor.execute("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_name = 'ticker'
                """.format('ticker'.replace('\'', '\'\'')))
        if cursor.fetchone()[0] == 0:
            cursor.execute("""CREATE TABLE ticker (pair char(40), value real);""")
            conn.commit()
        # create a new table with columns "pair" and "value"
        else:
            print("Table already exists.")
    except Exception as e:
        print("Uh oh, can't connect. Have you checked dbname, user or password?")
        print(e)
        return 0

def initial_cfg():
    #config file
    if (os.path.isfile('./config.ini') is not True):
        config['DEFAULT'] = {'TickerUrl': 'https://api.bitfinex.com/v1/pubticker/',
                          'RiskQuotient': '0.1'}
        config['CURRENCIES'] = {'c1': 'btc',
                             'c2': 'eth',
                             'c3': 'ltc',
                             'c4': 'iot'}
        config['DATABASE'] = {'dbname': 'testpython',
                           'user': 'portfopti',
                           'host': 'localhost',
                           'password': 'MYPASSWORD'}
        with open('config.ini', 'w') as cfg_file:
            config.write(cfg_file)
    #sql db
    sql_init()

# ticker and data populator
def ticker():
    for key in config['CURRENCIES']:
        url = config['DEFAULT']['TickerUrl'] + key + "usd"
        response = requests.request("GET", url)
        resp_json = response.text
        parsed_json = json.loads(resp_json)
        current_price = parsed_json['mid']
    return 0

#MAIN
initial_cfg()
