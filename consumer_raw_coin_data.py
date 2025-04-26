import json
import pymysql
from kafka import KafkaConsumer
from dateutil import parser  # Make sure to install it via: pip install python-dateutil

consumer = KafkaConsumer(
    'raw_coin_data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

conn = pymysql.connect(host='localhost', user='coinuser', password='Coinpass@123', database='cryptodb')
cursor = conn.cursor()

for message in consumer:
    data = message.value
    try:
        # Convert ISO 8601 time to MySQL-compatible format
        raw_time = data.get("time")
        parsed_time = parser.isoparse(raw_time).strftime('%Y-%m-%d %H:%M:%S')

        cursor.execute(
            "INSERT INTO raw_coin_data (asset_id_base, asset_id_quote, rate, time) VALUES (%s, %s, %s, %s)",
            (data.get("asset_id_base"), data.get("asset_id_quote"), data.get("rate"), parsed_time)
        )
        conn.commit()
        print("Inserted raw_coin_data:", data)
    except Exception as e:
        print("Error:", e)
