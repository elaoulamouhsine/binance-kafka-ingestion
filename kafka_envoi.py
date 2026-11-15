import json
from confluent_kafka import Producer


class KafkaSender:
    def __init__(self, bootstrap: str, topic: str):
        self.topic = topic
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "zstd",
            "linger.ms": 20,
        })
        self.sent = 0
        self.errors = 0

    def _on_delivery(self, err, msg):
        if err is not None:
            self.errors += 1
            print(f"[Kafka] échec de livraison : {err}")

    def send(self, key: str, value: dict):
        payload = json.dumps(value, separators=(",", ":")).encode()
        while True:
            try:
                self.producer.produce(self.topic, key=key.encode(), value=payload, on_delivery=self._on_delivery)
                break
            except BufferError:
                self.producer.poll(0.1)
        self.sent += 1
        self.producer.poll(0)

    def close(self):
        remaining = self.producer.flush(30)
        print(f"[Kafka] fermeture : {self.sent} envoyés, {self.errors} erreurs, {remaining} non envoyés")
