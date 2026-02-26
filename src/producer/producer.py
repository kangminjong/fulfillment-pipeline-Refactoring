import time, json, os
from kafka import KafkaProducer
from dotenv import load_dotenv
from src.producer.data_factory import OrderGenerator
from datetime import datetime,timezone
load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_NAME = os.getenv("KAFKA_TOPIC")

def create_producer():
    return KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=20,
        batch_size=65536,
        retries=5,
    )

def make_msg(gen: OrderGenerator, mode: str, burst_count: int):
    if mode == "NORMAL":
        return gen.generate_normal()[0]
    if mode == "VALID_ERROR":
        return gen.generate_validation_error()[0]
    if mode == "STOCK_OUT":
        return gen.generate_out_of_stock()[0]
    if mode == "EMPTY_JSON":
        return {}  # consumer에서 empty 처리되게
    if mode == "USER_BURST":
        # burst는 한 번에 여러 개 만들어지므로 밖에서 따로 처리(아래 send_scenario에서)
        return None
    if mode == "PROD_BURST":
        return None
    raise ValueError(f"unknown mode={mode}")

def send_scenario(gen, producer, run_id: str, mode: str, n: int, flush_every: int, burst_count: int):
    total = 0
    t0 = time.time()

    if mode in ("USER_BURST", "PROD_BURST"):
        # burst는 "한 번에 여러 개" 생성되니까, 그 배치를 n번 반복
        for batch_idx in range(1, n + 1):
            batch = gen.generate_user_burst(burst_count) if mode == "USER_BURST" else gen.generate_product_burst(burst_count)
            for msg in batch:
                total += 1
                msg["run_id"] = run_id
                msg["seq"] = total
                msg["event_produced_at"] = datetime.now(timezone.utc).isoformat()
                producer.send(TOPIC_NAME, value=msg)

            if batch_idx % max(1, flush_every // burst_count) == 0:
                producer.flush()
                dt = time.time() - t0
                print(f"[{mode}] sent={total} elapsed={dt:.1f}s eps={total/dt:.1f}", flush=True)

        producer.flush()
        dt = time.time() - t0
        print(f"[{mode}] DONE sent={total} elapsed={dt:.1f}s avg_eps={total/dt:.1f}", flush=True)
        return

    # NORMAL / VALID_ERROR / STOCK_OUT / EMPTY_JSON
    for i in range(1, n + 1):
        msg = make_msg(gen, mode, burst_count)
        msg["run_id"] = run_id
        msg["seq"] = i
        producer.send(TOPIC_NAME, value=msg)
        total = i

        if i % flush_every == 0:
            producer.flush()
            dt = time.time() - t0
            print(f"[{mode}] sent={total} elapsed={dt:.1f}s eps={total/dt:.1f}", flush=True)

    producer.flush()
    dt = time.time() - t0
    print(f"[{mode}] DONE sent={total} elapsed={dt:.1f}s avg_eps={total/dt:.1f}", flush=True)

if __name__ == "__main__":
    gen = OrderGenerator()
    producer = create_producer()

    # ✅ 여기서 조절
    base = datetime.now().strftime("run_%Y%m%d_%H%M%S")
    FLUSH_EVERY = 2000
    BURST_COUNT = 8

    # 시나리오별 전송량
    PLAN = [
        ("NORMAL", 100000),
        ("EMPTY_JSON", 1000),
        ("VALID_ERROR", 1000),
        ("USER_BURST", 200),   # 200번 * 8개 = 1600개
        ("PROD_BURST", 200),   # 200번 * 8개 = 1600개
    ]

    try:
        for mode, n in PLAN:
            run_id = f"{base}-{mode.lower()}"
            send_scenario(gen, producer, run_id, mode, n, FLUSH_EVERY, BURST_COUNT)
            time.sleep(1)  # 시나리오 사이 잠깐 쉬기(선택)
    finally:
        producer.close()