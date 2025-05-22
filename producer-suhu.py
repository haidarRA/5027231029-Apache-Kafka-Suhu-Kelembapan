from confluent_kafka import Producer
import json
import random
import time

producer = Producer({'bootstrap.servers': 'localhost:9092'})

gudang_ids = ['G1', 'G2', 'G3', 'G4']

def send_message(producer, topic, value):
    producer.produce(topic, value=json.dumps(value).encode('utf-8'))
    producer.flush()

while True:
    gudang_id = random.choice(gudang_ids)
    suhu = random.randint(70, 90)
    data = {'gudang_id': gudang_id, 'suhu': suhu}

    send_message(producer, 'sensor-suhu-gudang', data)
    print(f'Kirim: {data}')

    time.sleep(1)