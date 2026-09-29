import json
import pytest

def validate_transaction(tx: dict) -> bool:
    """
    Валидация бизнес-правил банковской транзакции перед отправкой в Kafka.
    """
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
    """
    Сериализация в формат Kafka, используемая в producer.py.
    """
    return json.dumps(payload).encode("utf-8")

# --- НАБОР UNIT-ТЕСТОВ PYTEST ---

def test_valid_transaction():
    """Тест: корректная банковская транзакция должна проходить валидацию."""
    valid_tx = {
        "account_from": "ACC-408178100001",
        "account_to": "ACC-408178100002",
        "amount": 15000.50,
        "currency": "RUB"
    }
    assert validate_transaction(valid_tx) is True

def test_invalid_negative_amount():
    """Тест: транзакция с отрицательной или нулевой суммой должна отклоняться."""
    invalid_tx = {
        "account_from": "ACC-408178100001",
        "account_to": "ACC-408178100002",
        "amount": -500.0,
        "currency": "RUB"
    }
    assert validate_transaction(invalid_tx) is False

def test_missing_fields():
    """Тест: транзакция без обязательного счета отправителя должна отклоняться."""
    incomplete_tx = {
        "account_to": "ACC-408178100002",
        "amount": 100.0,
        "currency": "USD"
    }
    assert validate_transaction(incomplete_tx) is False

def test_kafka_payload_serialization():
    """Тест: сериализатор producer.py должен возвращать валидный UTF-8 JSON в bytes."""
    payload = {"transaction_id": "TX-998811", "status": "PENDING"}
    serialized = serialize_payload(payload)
    assert isinstance(serialized, bytes)
    assert json.loads(serialized.decode("utf-8")) == payload
