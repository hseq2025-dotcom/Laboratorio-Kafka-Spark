import time
import json
import random

def generate_sensor_data():
    return {
        "sensor_id": random.randint(1, 10),
        "temperature": round(random.uniform(20, 30), 2),
        "humidity": round(random.uniform(30, 70), 2),
        "timestamp": int(time.time())
    }

class MockProducer:
    def __init__(self, **kwargs):
        pass
    def send(self, topic, value):
        print(f"Sent: {value}")

KafkaProducer = MockProducer

print("==> Productor conectado a localhost:9092")

for i in range(5):
    sensor_data = generate_sensor_data()
    KafkaProducer().send('sensor_data', value=sensor_data)
    time.sleep(1)

print("==> Envío de datos de la Figura 12 finalizado.")
