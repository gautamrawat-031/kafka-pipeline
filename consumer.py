import os
import json
import time
import random
import logging
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer, KafkaException, TopicPartition

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC             = os.getenv("TOPIC", "orders")
DLQ_TOPIC         = os.getenv("DLQ_TOPIC", "orders.dlq")
GROUP_ID          = os.getenv("GROUP_ID", "orders-consumer-group")
MAX_RETRIES       = int(os.getenv("MAX_RETRIES", "3"))
PROCESSING_DELAY  = int(os.getenv("PROCESSING_DELAY_MS", "20")) / 1000.0

_seen_ids: set = set()

def is_duplicate(order_id: str) -> bool:
    if order_id in _seen_ids:
        return True
    _seen_ids.add(order_id)
    return False


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, reset_timeout: int = 30):
        self.failure_threshold    = failure_threshold
        self.reset_timeout        = reset_timeout
        self.consecutive_failures = 0
        self.state                = "CLOSED"
        self._opened_at           = None

    def call(self, fn, *args, **kwargs):
        if self.state == "OPEN":
            elapsed = (datetime.now(timezone.utc) - self._opened_at).total_seconds()
            if elapsed >= self.reset_timeout:
                log.info("CircuitBreaker → HALF_OPEN (testing downstream)")
                self.state = "HALF_OPEN"
            else:
                raise RuntimeError(
                    f"CircuitBreaker OPEN — reopens in {self.reset_timeout - int(elapsed)}s"
                )

        try:
            result = fn(*args, **kwargs)
            if self.state == "HALF_OPEN":
                log.info("CircuitBreaker → CLOSED (downstream recovered)")
                self.state = "CLOSED"
                self.consecutive_failures = 0
            return result

        except Exception as exc:
            self.consecutive_failures += 1
            if self.consecutive_failures >= self.failure_threshold:
                self.state      = "OPEN"
                self._opened_at = datetime.now(timezone.utc)
                log.warning("CircuitBreaker → OPEN after %d consecutive failures",
                            self.consecutive_failures)
            raise exc


circuit = CircuitBreaker(failure_threshold=5, reset_timeout=30)

_dlq_producer = Producer({
    "bootstrap.servers":  BOOTSTRAP_SERVERS,
    "acks":               "all",
    "enable.idempotence": True,
})


def send_to_dlq(raw_value: bytes, reason: str, attempt: int):
    envelope = {
        "original":  raw_value.decode("utf-8", errors="replace"),
        "reason":    reason,
        "attempt":   attempt,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
    _dlq_producer.produce(DLQ_TOPIC, value=json.dumps(envelope).encode("utf-8"))
    _dlq_producer.flush()
    log.warning("→ DLQ | reason=%s attempt=%d", reason, attempt)


def process_order(order: dict):
    time.sleep(PROCESSING_DELAY)
    if random.random() < 0.10:
        raise ValueError(f"Simulated processing error for order {order['order_id']}")
    log.info("Processed | order_id=%s product=%s amount=%.2f",
             order["order_id"], order["product"], order["amount"])


CONSUMER_CONFIG = {
    "bootstrap.servers":     BOOTSTRAP_SERVERS,
    "group.id":              GROUP_ID,
    "auto.offset.reset":     "earliest",
    "enable.auto.commit":    False,
    "fetch.min.bytes":       1024,
    "fetch.wait.max.ms":     500,
    "max.poll.interval.ms":  300000,
    "session.timeout.ms":    30000,
    "heartbeat.interval.ms": 3000,
}


def run():
    consumer = Consumer(CONSUMER_CONFIG)
    consumer.subscribe([TOPIC])
    log.info("Consumer started | group=%s topic=%s", GROUP_ID, TOPIC)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                log.error("Consumer error: %s", msg.error())
                continue

            attempt   = 0
            committed = False

            while attempt <= MAX_RETRIES and not committed:
                try:
                    order    = json.loads(msg.value())
                    order_id = order.get("order_id", "unknown")

                    if is_duplicate(order_id):
                        log.debug("Duplicate skipped | order_id=%s", order_id)
                        consumer.commit(asynchronous=False)
                        committed = True
                        break

                    circuit.call(process_order, order)
                    consumer.commit(asynchronous=False)
                    committed = True

                except RuntimeError as exc:
                    log.error("Circuit open, skipping to DLQ: %s", exc)
                    send_to_dlq(msg.value(), str(exc), attempt)
                    consumer.commit(asynchronous=False)
                    committed = True

                except (json.JSONDecodeError, KeyError) as exc:
                    log.error("Malformed message → DLQ: %s", exc)
                    send_to_dlq(msg.value(), f"parse_error: {exc}", attempt)
                    consumer.commit(asynchronous=False)
                    committed = True

                except Exception as exc:
                    attempt += 1
                    if attempt > MAX_RETRIES:
                        log.error("Max retries reached → DLQ: %s", exc)
                        send_to_dlq(msg.value(), str(exc), attempt)
                        consumer.commit(asynchronous=False)
                        committed = True
                    else:
                        backoff = 0.5 * (2 ** attempt)
                        log.warning("Retry %d/%d in %.1fs | error=%s",
                                    attempt, MAX_RETRIES, backoff, exc)
                        time.sleep(backoff)

    except KeyboardInterrupt:
        log.info("Shutdown signal received")
    finally:
        consumer.close()
        log.info("Consumer closed cleanly")


if __name__ == "__main__":
    run()