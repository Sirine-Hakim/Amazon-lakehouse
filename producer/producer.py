import json
import time

from kafka import KafkaProducer

TOPIC = "reviews_raw"
FILE = "data/reviews_sample.jsonl"
DELAY = 0.001  # secondes entre deux messages

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    linger_ms=50,
)

sent = 0
start = time.time()

with open(FILE, "r", encoding="utf-8") as f:
    for line in f:
        review = json.loads(line)
        # Clé = id du produit : les avis d'un même produit restent dans la même partition
        producer.send(TOPIC, key=review["parent_asin"], value=review)
        sent += 1
        if sent % 5000 == 0:
            print(f"{sent} avis envoyés...")
        time.sleep(DELAY)

producer.flush()
print(f"Terminé : {sent} avis en {time.time() - start:.1f}s")
