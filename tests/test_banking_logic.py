import json
import pytest

def validate_transaction(tx: dict) -> bool:
    required_fields = ["account_from", "account_to", "amount", "currency"]
    for field in required_fields:
        if field not in tx or not tx[field]:
            return False
    if not isinstance(tx["amount"], (int, float)) or tx["amount"] <= 0:
        return False
    if tx["currency"] not in ["RUB", "USD", "EUR"]:
        return False
    return True

def serialize_payload(payload: dict) -> bytes:
    return json.dumps(payload).encode("utf-8")

def test_valid_transaction():
    valid_tx = {"account_from": "ACC-01", "account_to": "ACC-02", "amount": 100.0, "currency": "RUB"}
    assert validate_transaction(valid_tx) is True

def test_invalid_negative_amount():
    invalid_tx = {"account_from": "ACC-01", "account_to": "ACC-02", "amount": -10.0, "currency": "RUB"}
    assert validate_transaction(invalid_tx) is False

def test_kafka_payload_serialization():
    payload = {"tx": "123"}
    assert serialize_payload(payload) == b'{"tx": "123"}'
