import os, time, random, json, datetime as dt
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "nse_ticks")
UNIVERSE = os.getenv("UNIVERSE", "RELIANCE,TCS,INFY,HDFCBANK").split(",")

# Retry until Kafka is ready
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        break
    except NoBrokersAvailable:
        print("Kafka not ready, retrying...")
        time.sleep(5)

while True:
    symbol = random.choice(UNIVERSE)
    payload = {
        "symbol": symbol,
        "ts": dt.datetime.utcnow().isoformat() + "Z",
        "price": round(random.uniform(2000, 3000), 2),
        "volume": random.randint(100, 1000)
    }
    producer.send(TOPIC, payload)
    print("Sent:", payload)
    time.sleep(2)