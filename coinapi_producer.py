import requests
import time
import json
from kafka import KafkaProducer

API_KEY = 'abb93252-dc7c-49cf-b685-6a6ae2782c9e'
URL = 'https://rest.coinapi.io/v1/exchangerate/BTC/USD'

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

headers = {'X-CoinAPI-Key': API_KEY}

while True:
    try:
        response = requests.get(URL, headers=headers)
        if response.status_code == 200:
            data = response.json()
            producer.send('raw_coin_data', data)
            print(f"Sent: {data}")
        else:
            print("Error:", response.status_code, response.text)
    except Exception as e:
        print("Exception:", e)
    time.sleep(10)