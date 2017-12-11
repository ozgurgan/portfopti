import requests
import json
import time

while(True):
    list = ['btcusd','ethusd','ethbtc','ltcusd','ltcbtc','iotusd','iotbtc']
    ticker_list = []
    my_amount = [0, 0, 0, 0, 0, 0, 0]
    total = 0.0
    n = 0

    for pair in list:
        time.sleep(5)
        url = "https://api.bitfinex.com/v1/pubticker/"+pair
        response = requests.request("GET", url)
        resp_json = response.text
        parsed_json = json.loads(resp_json)
        ticker_list.append(parsed_json['mid'])

    for value in ticker_list:
        total += float(value) * my_amount[n]
        n += 1

    with open('my_money.csv', 'a') as myfile:
        myfile.write(str(total)+'\n')
        myfile.close()

