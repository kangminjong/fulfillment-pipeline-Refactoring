# src/run_scenario.py
import os, time, json, random
from kafka import KafkaProducer
from producer_scenarios import build_event

RUN_ID = os.getenv("RUN_ID", "run-local-001")
MODE = os.getenv("MODE", "NORMAL")  # NORMAL/DUPLICATE/OUT_OF_ORDER/STOCK_OUT/VALID_ERROR/EMPTY_JSON
N = int(os.getenv("N", "50"))
SEED = int(os.getenv("SEED", "42"))

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "event")

random.seed(SEED)

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)

def send_one(i: int):
    evt = build_event(RUN_ID, i, MODE)
    if evt == "":
        producer.send(TOPIC, value=b"")
        print(f"SEND EMPTY i={i}", flush=True)
        return
    b = json.dumps(evt, ensure_ascii=False).encode("utf-8")
    k = evt["order_id"].encode("utf-8")
    producer.send(TOPIC, key=k, value=b)
    print(f"SEND mode={MODE} i={i} order_id={evt['order_id']} product_id={evt['product_id']}", flush=True)

if MODE == "DUPLICATE":
    for i in range(N):
        send_one(i)
        send_one(i)  # 같은 메시지 2번
elif MODE == "OUT_OF_ORDER":
    # i=0..N-1 만들고, occurred_at은 producer_scenarios에서 찍히지만
    # out-of-order는 "전송 순서"를 섞어서 재현 가능하게 만듦
    idx = list(range(N))
    random.shuffle(idx)
    for i in idx:
        send_one(i)
else:
    for i in range(N):
        send_one(i)

producer.flush()
print("DONE", flush=True)