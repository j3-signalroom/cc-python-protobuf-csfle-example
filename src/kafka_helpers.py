from __future__ import annotations
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import time

from utilities import setup_logging


__copyright__  = "Copyright (c) 2026 Jeffrey Jonathan Jennings"
__credits__    = ["Jeffrey Jonathan Jennings"]
__license__    = "MIT"
__maintainer__ = "Jeffrey Jonathan Jennings"
__email__      = "j3@thej3.com"
__status__     = "dev"


# Setup module logging
logger = setup_logging()


def _base_kafka_config(cfg: dict) -> dict:
    return {
        "bootstrap.servers": cfg["bootstrap_servers"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms":   "PLAIN",
        "sasl.username":     cfg["kafka_api_key"],
        "sasl.password":     cfg["kafka_api_secret"],
    }


def ensure_topics(cfg: dict, topics: list[str], num_partitions: int = 6, replication_factor: int = 3) -> None:
    """
    Create topics that don't already exist.
    Confluent Cloud requires replication_factor=3.
    Existing topics are silently skipped.
    """
    admin = AdminClient(_base_kafka_config(cfg))

    # Find which topics already exist
    metadata   = admin.list_topics(timeout=10)
    existing   = set(metadata.topics.keys())
    to_create  = [t for t in topics if t not in existing]

    if not to_create:
        logger.info(f"  [Admin] All topics already exist: {topics}")
        return

    new_topics = [
        NewTopic(t, num_partitions=num_partitions, replication_factor=replication_factor)
        for t in to_create
    ]
    futures = admin.create_topics(new_topics)

    for topic, future in futures.items():
        try:
            future.result()  # blocks until done or raises
            logger.info(f"  [Admin] Created topic '{topic}' "
                        f"(partitions={num_partitions}, rf={replication_factor})")
        except Exception as exc:
            # TOPIC_ALREADY_EXISTS is benign — surface everything else
            if "TOPIC_ALREADY_EXISTS" in str(exc) or "already exists" in str(exc).lower():
                logger.info(f"  [Admin] Topic '{topic}' already exists — skipping")
            else:
                raise RuntimeError(f"Failed to create topic '{topic}': {exc}") from exc


def kafka_produce(cfg: dict, topic: str, key: str, value: bytes) -> None:
    p = Producer(_base_kafka_config(cfg))
    p.produce(topic, key=key.encode(), value=value)
    p.flush(timeout=30)
    logger.info(f"  [Kafka] Produced → topic='{topic}' key='{key}'")


def kafka_consume_one(cfg: dict, topic: str, group_id: str, timeout: int = 30) -> bytes | None:
    conf = {**_base_kafka_config(cfg), "group.id": group_id, "auto.offset.reset": "earliest"}
    c = Consumer(conf)
    c.subscribe([topic])
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            c.commit(msg)
            return msg.value()
    finally:
        c.close()
    return None
