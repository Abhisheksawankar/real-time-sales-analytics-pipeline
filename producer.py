import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = ['laptop', 'phone', 'tablet', 'headphones', 'monitor']
regions  = ['APAC', 'EMEA', 'NA', 'LATAM']

print("Producer started — sending sales events...")

while True:
    event = {
        'event_id':  random.randint(10000, 99999),
        'product':   random.choice(products),
        'region':    random.choice(regions),
        'quantity':  random.randint(1, 20),
        'price':     round(random.uniform(50, 2000), 2),
        'timestamp': time.time()
    }
    producer.send('sales-events', event)
    print(f"Sent → {event['product']} | {event['region']} | ${event['price']}")
    time.sleep(0.5)