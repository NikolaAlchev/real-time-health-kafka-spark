import json
import time
import random
import pandas as pd
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    security_protocol="PLAINTEXT"
)

csv_file_path = '../online.csv'

online_df = pd.read_csv(csv_file_path)
online_df = online_df.drop("Diabetes_012", axis=1)

for index, row in online_df.iterrows():
    key = index
    json_row = row.to_json()
    record = {"key": key, "value": json_row, "timestamp": int(time.time() * 1000)}

    print(json.dumps(record))
    producer.send(topic="health_data", value=json.dumps(record).encode("utf-8"))
    time.sleep(random.randint(500, 2000) / 1000.0)
