import json
import pymysql
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'aggregated_data',  # Changed to match the topic we're writing to in spark_stream_processor
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='coin_update_consumer_group'
)

# Using the same credentials as spark_stream_processor
conn = pymysql.connect(
    host='localhost',
    user='coinuser',
    password='<PASS>',
    database='<LOCAL_DB>'
)
cursor = conn.cursor()

print("Started listening for messages...")

try:
    for message in consumer:
        data = message.value
        try:
            cursor.execute(
                "INSERT INTO coin_update_count (start_time, end_time, asset_id_base, asset_id_quote, update_count) VALUES (%s, %s, %s, %s, %s)",
                (
                    data["window"]["start"],
                    data["window"]["end"],
                    data.get("asset_id_base"),
                    data.get("asset_id_quote"),
                    data.get("update_count")
                )
            )
            conn.commit()
            print(f"Inserted coin_update_count - Base: {data.get('asset_id_base')}, Quote: {data.get('asset_id_quote')}, Count: {data.get('update_count')}")
        except Exception as e:
            print(f"Error inserting data: {str(e)}")
            print(f"Problematic data: {data}")
except KeyboardInterrupt:
    print("Shutting down consumer...")
finally:
    cursor.close()
    conn.close()
    consumer.close()
