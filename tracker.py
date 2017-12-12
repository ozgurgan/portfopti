import requests
import configparser
import psycopg2
import os
import json
import time

config = configparser.ConfigParser()


def get_cfg(string):
    config.read('config.ini')
    return config['DATABASE'][string]
def with_connection(f):
    def with_connection_(*args, **kwargs):
        connect_str = "dbname='" + get_cfg('dbname') + "' user='" + get_cfg('user') + "' host='" + get_cfg(
            'host') + "' " + \
                      "password='" + get_cfg('password') + "'"
        cnn = psycopg2.connect(connect_str)
        try:
            rv = f(cnn, *args, **kwargs)
        except Exception as e:
            cnn.rollback()
            raise
        else:
            cnn.commit() # or maybe not
        finally:
            cnn.close()
        return rv
    return with_connection_
@with_connection
def send_to_db(cnn, SQL, arg1, arg2=None):
    cur = cnn.cursor()
    data = (arg1, arg2)
    cur.execute(SQL, data)  # Note: no % operator
    return cur
# def db_connection():
#     try:
#         connect_str = "dbname='" + get_cfg('dbname') + "' user='" + get_cfg('user') + "' host='" + get_cfg('host') + "' " + \
#                       "password='" + get_cfg('password') + "'"
#         psycopg2.connect(connect_str)
#     except Exception as e:
#         print("Uh oh, can't connect. Have you checked dbname, user or password?")
#         print(e)
#         return 0

def sql_init():
    send_to_db
    sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = %s"
    cur = send_to_db(sql, 'ticker')
    if cur.fetchone()[0] == 0:
        print("No table found in DB, creating one.")
        SQL ='CREATE TABLE %s (pair char(40), value real);'
        send_to_db(SQL, 'ticker')

    # create a new table with columns "pair" and "value"
    else:
        print("Table already exists in DB, moving on.")

def initial_cfg():
    #config file
    if (os.path.isfile('./config.ini') is not True):
        print("No config.ini found, creating new one")
        config['SETTINGS'] = {'TickerUrl': 'https://api.bitfinex.com/v1/pubticker/',
                          'RiskQuotient': '0.1'}
        config['CURRENCIES'] = {'btc': 'btc',
                             'eth': 'eth',
                             'ltc': 'ltc',
                             'iot': 'iot'}
        config['DATABASE'] = {'dbname': 'testpython',
                           'user': 'portfopti',
                           'host': 'localhost',
                           'password': 'MYPASSWORD'}
        with open('config.ini', 'w') as cfg_file:
            config.write(cfg_file)
    else:
        print("config.ini found, reading configurations.")

    sql_init()

# ticker and data populator
def ticker():
    for key in config['CURRENCIES']:
        url = config['SETTINGS']['TickerUrl'] + key + "usd"
        response = requests.request("GET", url)
        resp_json = response.text
        parsed_json = json.loads(resp_json)
        current_price = parsed_json['mid']
        SQL = "INSERT INTO ticker(pair,value) VALUES (%s,%s);"
        send_to_db(SQL, key,current_price)
        time.sleep(2)
    return 0

#MAIN

initial_cfg()
ticker()