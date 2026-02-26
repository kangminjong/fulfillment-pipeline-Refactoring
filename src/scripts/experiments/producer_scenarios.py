# src/producer_scenarios.py
import random
from datetime import datetime, timezone

def now_iso():
    return datetime.now(timezone.utc).isoformat()

ALL_PRODUCTS = [
    # ELEC
    "ELEC-001","ELEC-002","ELEC-003","ELEC-004","ELEC-005","ELEC-006","ELEC-007","ELEC-008","ELEC-009","ELEC-010",
    # CLOTH
    "CLOTH-001","CLOTH-002","CLOTH-003","CLOTH-004","CLOTH-005","CLOTH-006","CLOTH-007","CLOTH-008","CLOTH-009","CLOTH-010",
    # FOOD
    "FOOD-001","FOOD-002","FOOD-003","FOOD-004","FOOD-005","FOOD-006","FOOD-007","FOOD-008","FOOD-009","FOOD-010",
    # BOOK
    "BOOK-001","BOOK-002","BOOK-003","BOOK-004","BOOK-005","BOOK-006","BOOK-007","BOOK-008","BOOK-009","BOOK-010",
    # TEST
    "TEST-001","TEST-003","TEST-004","TEST-005","TEST-006","TEST-007","TEST-008","TEST-009","TEST-010",
]

def make_order_id(run_id: str, i: int) -> str:
    return f"{run_id}-ORD-{i:06d}"

def build_event(run_id: str, i: int, mode: str):
    pid = random.choice(ALL_PRODUCTS)

    base = {
        "run_id": run_id,
        "event_type": "ORDER_CREATED",
        "occurred_at": now_iso(),
        "order_id": make_order_id(run_id, i),
        "user_id": f"U-{random.randint(1, 50):03d}",
        "product_id": pid,
        "product_name": f"NAME-{pid}",
        "shipping_address": "서울시 강남구 테스트로 123",
        "current_stage": "PAYMENT",
        "current_status": "PAID",
        "created_at": now_iso(),
        "event_produced_at": now_iso(),
        "last_event_type": "ORDER_CREATED",
        "quantity": 1,
    }

    if mode == "VALID_ERROR":
        base["shipping_address"] = ""
    if mode == "EMPTY_JSON":
        return ""
    return base