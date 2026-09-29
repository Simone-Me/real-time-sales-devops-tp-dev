import re
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.main import Order, OrderEvent, build_order_event

from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from app.main import app, create_kafka_producer


def test_order_total():
    order = Order(
        customer_id="C001",
        product_id="P001",
        quantity=2,
        unit_price=50.0,
    )
    event = build_order_event(order)
    assert event["total_amount"] == 100.0


def test_order_contains_order_id():
    order = Order(
        customer_id="C001",
        product_id="P001",
        quantity=1,
        unit_price=10.0,
    )
    event = build_order_event(order)
    assert event["order_id"].startswith("ORD-")


def test_quantity_must_be_positive():
    with pytest.raises(ValidationError):
        Order(
            customer_id="C001",
            product_id="P001",
            quantity=0,
            unit_price=10.0,
        )


def make_order(**overrides):
    data = {
        "customer_id": "C001",
        "product_id": "P001",
        "quantity": 1,
        "unit_price": 10.0,
    }
    data.update(overrides)
    return Order(**data)


# --- Calcul du montant total ---

@pytest.mark.parametrize(
    "quantity, unit_price, expected",
    [
        (1, 10.0, 10.0),
        (3, 49.90, 149.7),
        (10, 0.1, 1.0),
        (1000, 100000, 100000000.0),
    ],
)
def test_total_amount_calculation(quantity, unit_price, expected):
    event = build_order_event(make_order(quantity=quantity, unit_price=unit_price))
    assert event["total_amount"] == expected


def test_total_amount_is_rounded_to_two_decimals():
    event = build_order_event(make_order(quantity=3, unit_price=0.333))
    assert event["total_amount"] == 1.0


# --- Quantité ---

@pytest.mark.parametrize("quantity", [1, 5, 1000])
def test_positive_quantity_is_accepted(quantity):
    order = make_order(quantity=quantity)
    assert order.quantity == quantity


def test_zero_quantity_is_rejected():
    with pytest.raises(ValidationError):
        make_order(quantity=0)


@pytest.mark.parametrize("quantity", [-1, -100])
def test_negative_quantity_is_rejected(quantity):
    with pytest.raises(ValidationError):
        make_order(quantity=quantity)


def test_quantity_above_limit_is_rejected():
    with pytest.raises(ValidationError):
        make_order(quantity=1001)


# --- Prix ---

@pytest.mark.parametrize("unit_price", [0.01, 49.90, 100000])
def test_positive_price_is_accepted(unit_price):
    order = make_order(unit_price=unit_price)
    assert order.unit_price == unit_price


@pytest.mark.parametrize("unit_price", [0, -0.01, -50.0, 100000.01])
def test_invalid_price_is_rejected(unit_price):
    with pytest.raises(ValidationError):
        make_order(unit_price=unit_price)


def test_non_numeric_price_is_rejected():
    with pytest.raises(ValidationError):
        make_order(unit_price="abc")


# --- Identifiant client ---

def test_valid_customer_id_is_accepted():
    order = make_order(customer_id="C42")
    assert order.customer_id == "C42"


@pytest.mark.parametrize("customer_id", ["", "C"])
def test_too_short_customer_id_is_rejected(customer_id):
    with pytest.raises(ValidationError):
        make_order(customer_id=customer_id)


def test_missing_customer_id_is_rejected():
    with pytest.raises(ValidationError):
        Order(product_id="P001", quantity=1, unit_price=10.0)


# --- Identifiant produit ---

def test_valid_product_id_is_accepted():
    order = make_order(product_id="P004")
    assert order.product_id == "P004"


@pytest.mark.parametrize("product_id", ["", "P"])
def test_too_short_product_id_is_rejected(product_id):
    with pytest.raises(ValidationError):
        make_order(product_id=product_id)


def test_missing_product_id_is_rejected():
    with pytest.raises(ValidationError):
        Order(customer_id="C001", quantity=1, unit_price=10.0)


# --- Génération de l'identifiant de commande ---

def test_order_id_format():
    order_id = build_order_event(make_order())["order_id"]
    assert re.fullmatch(r"ORD-[0-9A-F]{10}", order_id)


def test_order_ids_are_unique():
    order = make_order()
    ids = {build_order_event(order)["order_id"] for _ in range(100)}
    assert len(ids) == 100


# --- Construction de l'événement ---

def test_event_contains_expected_fields():
    event = build_order_event(make_order())
    assert set(event.keys()) == {
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "total_amount",
        "timestamp",
    }


def test_event_copies_order_data():
    order = make_order(customer_id="C007", product_id="P002", quantity=4, unit_price=89.90)
    event = build_order_event(order)
    assert event["customer_id"] == "C007"
    assert event["product_id"] == "P002"
    assert event["quantity"] == 4
    assert event["unit_price"] == 89.90


def test_event_timestamp_is_iso_utc():
    before = datetime.now(timezone.utc)
    event = build_order_event(make_order())
    after = datetime.now(timezone.utc)

    timestamp = datetime.fromisoformat(event["timestamp"])
    assert timestamp.tzinfo is not None
    assert timestamp.utcoffset().total_seconds() == 0
    assert before <= timestamp <= after


def test_event_is_valid_order_event():
    event = build_order_event(make_order(quantity=2, unit_price=25.0))
    validated = OrderEvent(**event)
    assert validated.total_amount == 50.0
    
client = TestClient(app)
    
def test_health():
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "UP", "service": "sales-api"}


def test_products():
    response = client.get("/api/products")
    assert response.status_code == 200
    assert len(response.json()) == 4


def test_create_order_success():
    with patch("app.main.create_kafka_producer") as mock_factory:
        producer = MagicMock()
        mock_factory.return_value = producer

        response = client.post("/api/orders", json={
            "customer_id": "C001",
            "product_id": "P001",
            "quantity": 2,
            "unit_price": 50.0,
        })

    assert response.status_code == 201
    assert response.json()["total_amount"] == 100.0
    producer.send.assert_called_once()
    producer.close.assert_called_once()


def test_create_order_unknown_product():
    response = client.post("/api/orders", json={
        "customer_id": "C001",
        "product_id": "P999",
        "quantity": 1,
        "unit_price": 10.0,
    })
    assert response.status_code == 404


def test_create_kafka_producer():
    with patch("app.main.KafkaProducer") as mock_kafka:
        create_kafka_producer()
    mock_kafka.assert_called_once()




