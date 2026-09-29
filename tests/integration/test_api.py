import json
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2
import pytest
from kafka import KafkaConsumer, KafkaProducer
from fastapi.testclient import TestClient
from app.main import app

RUN_INTEGRATION = os.getenv("RUN_INTEGRATION_TESTS", "true").lower() == "true"

pytestmark = pytest.mark.skipif(
    not RUN_INTEGRATION,
    reason="Integration tests disabled. Set RUN_INTEGRATION_TESTS=true."
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales.orders")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "sales")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sales")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "sales")

TIMEOUT_SECONDS = 60

client = TestClient(app)


def test_health():
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json()["status"] == "UP"


def wait_for_kafka_message(order_id, timeout=TIMEOUT_SECONDS):
    """Lit le topic jusqu'à trouver le message dont l'order_id correspond."""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    try:
        deadline = time.time() + timeout
        while time.time() < deadline:
            for message in consumer:
                if message.value.get("order_id") == order_id:
                    return message.value
        return None
    finally:
        consumer.close()


def wait_for_postgres_row(order_id, timeout=TIMEOUT_SECONDS):
    """Interroge processed_orders jusqu'à ce que la commande apparaisse."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        connection = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT order_id, customer_id, product_id, quantity, "
                    "unit_price, total_amount, event_timestamp "
                    "FROM processed_orders WHERE order_id = %s",
                    (order_id,),
                )
                row = cursor.fetchone()
        finally:
            connection.close()
        if row:
            return row
        time.sleep(2)
    return None


# Test A : API -> Kafka
def test_api_kafka():
    response = client.post("/api/orders", json={
        "customer_id": "C001",
        "product_id": "P001",
        "quantity": 2,
        "unit_price": 100.0,
    })
    assert response.status_code == 201
    order_id = response.json()["order_id"]

    message = wait_for_kafka_message(order_id)

    assert message is not None, f"{order_id} introuvable dans le topic {KAFKA_TOPIC}"
    assert message["customer_id"] == "C001"
    assert message["product_id"] == "P001"
    assert message["quantity"] == 2
    assert message["unit_price"] == 100.0
    assert message["total_amount"] == 200.0


def test_api_unknown_product_does_not_produce_event():
    response = client.post("/api/orders", json={
        "customer_id": "C001",
        "product_id": "P999",
        "quantity": 1,
        "unit_price": 10.0,
    })
    assert response.status_code == 404


# Test B : Kafka -> Spark -> PostgreSQL
# Nécessite que le conteneur spark-streaming tourne (docker compose up).
def test_kafka_spark_postgresql():
    order_id = f"ORD-IT{uuid.uuid4().hex[:8].upper()}"
    event = {
        "order_id": order_id,
        "customer_id": "C-IT",
        "product_id": "P002",
        "quantity": 3,
        "unit_price": 89.90,
        "total_amount": 269.70,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    try:
        producer.send(KAFKA_TOPIC, value=event).get(timeout=10)
        producer.flush()
    finally:
        producer.close()

    row = wait_for_postgres_row(order_id)

    assert row is not None, f"{order_id} absent de processed_orders après {TIMEOUT_SECONDS}s"
    (db_order_id, customer_id, product_id, quantity,
     unit_price, total_amount, event_timestamp) = row
    assert db_order_id == order_id
    assert customer_id == "C-IT"
    assert product_id == "P002"
    assert quantity == 3
    assert float(unit_price) == 89.90
    assert float(total_amount) == 269.70
    assert event_timestamp is not None
