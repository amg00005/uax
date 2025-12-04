from kafka import KafkaProducer
import json, time, random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Zonas repartidas entre varios boroughs (ejemplo)
zones = [4,   13,  50, 161, 162, 80,  3,   5,   11, 116, ]

EVENTS_PER_SECOND = 5   # → 5 eventos por segundo ≈ 300 por minuto
SLEEP_TIME = 1 / EVENTS_PER_SECOND

while True:
    for _ in range(EVENTS_PER_SECOND):
        trip = {
            "pickup_datetime": datetime.utcnow().isoformat(),
            "passenger_count": random.randint(1, 4),
            "trip_distance": round(random.uniform(0.3, 12.0), 2),
            "fare_amount": round(random.uniform(4, 55), 2),
            "tip_amount": round(random.uniform(0, 15), 2),
            "PULocationID": random.choice(zones)
        }

        producer.send("taxi_trips", trip)
        print("sent:", trip)

    time.sleep(SLEEP_TIME)