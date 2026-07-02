import os
import json
import time
import uuid
import random
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC               = os.getenv("TOPIC", "orders")
MESSAGES_PER_SECOND = int(os.getenv("MESSAGES_PER_SECOND", "50"))

PRODUCER_CONFIG = {
    "bootstrap.servers":                     BOOTSTRAP_SERVERS,
    "acks":                                  "all",
    "enable.idempotence":                    True,
    "retries":                               10,
    "retry.backoff.ms":                      300,
    "linger.ms":                             5,
    "batch.size":                            32768,
    "compression.type":                      "snappy",
    "max.in.flight.requests.per.connection": 5,
    "delivery.timeout.ms":                   30000,
}

producer = Producer(PRODUCER_CONFIG)


def delivery_report(err, msg):
    if err:
        log.error("Delivery FAILED | key=%s error=%s", msg.key(), err)
    else:
        log.debug("Delivered | key=%s partition=%d offset=%d",
                  msg.key(), msg.partition(), msg.offset())


def generate_order() -> dict:
    return {
        "order_id":    str(uuid.uuid4()),
        "customer_id": random.randint(1000, 9999),
        "product":     random.choice(["laptop", "phone", "tablet", "headphones", "monitor"]),
        "amount":      round(random.uniform(10.0, 2000.0), 2),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "status":      "pending",
    }


def run():
    log.info("Producer starting | topic=%s rate=%d msg/s", TOPIC, MESSAGES_PER_SECOND)
    interval = 1.0 / MESSAGES_PER_SECOND
    sent = 0

    while True:
        try:
            order = generate_order()

            producer.produce(
                topic=TOPIC,
                key=str(order["customer_id"]),
                value=json.dumps(order).encode("utf-8"),
                callback=delivery_report,
            )
            sent += 1
            producer.poll(0)

            if sent % 500 == 0:
                producer.flush()
                log.info("Sent %d messages total", sent)

            time.sleep(interval)

        except BufferError:
            log.warning("Producer buffer full, flushing...")
            producer.flush()

        except KafkaException as exc:
            log.error("Kafka error: %s", exc)
            time.sleep(1)

        except KeyboardInterrupt:
            break

    log.info("Flushing remaining messages...")
    producer.flush()
    log.info("Producer shut down. Total sent: %d", sent)


if __name__ == "__main__":
    run()

x