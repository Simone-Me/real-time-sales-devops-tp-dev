import os
import time

import httpx
import psycopg2
import pytest

RUN_E2E = os.getenv("RUN_E2E_TESTS", "true").lower() == "true"

pytestmark = pytest.mark.skipif(
    not RUN_E2E,
    reason="E2E tests disabled. Set RUN_E2E_TESTS=true."
)

API_URL = os.getenv("API_URL", "http://localhost:8000")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "sales")
POSTGRES_USER = os.getenv("POSTGRES_USER", "sales")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "sales")

TIMEOUT_SECONDS = 90
POLL_INTERVAL_SECONDS = 2


def fetch_processed_order(order_id):
    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=5,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT order_id, customer_id, product_id, quantity, "
                "unit_price, total_amount FROM processed_orders WHERE order_id = %s",
                (order_id,),
            )
            return cursor.fetchone()
    finally:
        connection.close()


def wait_until(fetch, timeout=TIMEOUT_SECONDS, interval=POLL_INTERVAL_SECONDS):
    """Polling avec timeout : renvoie le résultat dès qu'il est disponible, sinon None."""
    deadline = time.monotonic() + timeout
    while True:
        result = fetch()
        if result:
            return result
        if time.monotonic() >= deadline:
            return None
        time.sleep(interval)


def test_order_flows_from_api_to_postgresql():
    # Given : l'infrastructure est démarrée
    assert httpx.get(f"{API_URL}/api/health", timeout=10).json()["status"] == "UP"

    # When : une commande est envoyée à l'API
    response = httpx.post(
        f"{API_URL}/api/orders",
        json={
            "customer_id": "C100",
            "product_id": "P001",
            "quantity": 3,
            "unit_price": 100,
        },
        timeout=30,
    )
    assert response.status_code == 201
    order_id = response.json()["order_id"]

    # Then : la commande est traitée par Spark et enregistrée dans PostgreSQL
    row = wait_until(lambda: fetch_processed_order(order_id))

    assert row is not None, (
        f"Commande {order_id} absente de processed_orders après {TIMEOUT_SECONDS}s"
    )
    db_order_id, customer_id, product_id, quantity, unit_price, total_amount = row
    assert db_order_id == order_id
    assert customer_id == "C100"
    assert product_id == "P001"
    assert quantity == 3
    assert float(unit_price) == 100.0
    assert float(total_amount) == 300.0
