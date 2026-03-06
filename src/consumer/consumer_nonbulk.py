import os
import json
import uuid
import psycopg2
import requests
from psycopg2.extras import Json, execute_values
from datetime import datetime, timezone
from collections import deque, defaultdict
from kafka import KafkaConsumer
from dotenv import load_dotenv
import time

load_dotenv()

# =========================================================
# ⚙️ [SYSTEM CONFIGURATION]
# =========================================================
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT"),
}

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_NAME = os.getenv("KAFKA_TOPIC")
GROUP_ID = os.getenv("KAFKA_GROUP_ID")
BATCH_COMMIT = int(os.getenv("BATCH_COMMIT", "100"))

# 🔔 [Slack 설정]
ENABLE_SLACK = os.getenv("ENABLE_SLACK", "TRUE").lower() == "true"
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()

# 🚨 [임계치 설정]
USER_BURST_WINDOW = 10.0
USER_BURST_LIMIT = 5
PROD_BURST_WINDOW = 1.0
PROD_BURST_LIMIT = 5

# 🚨 [리스크 코드 정의]
CODE_VALID = "FUL-VALID"  # 정보 누락
CODE_INV = "FUL-INV"  # 재고 부족
CODE_FRAUD_USER = "FUL-FRAUD-USER"  # 유저 도배
CODE_FRAUD_PROD = "FUL-FRAUD-PROD"  # 상품 폭주
CODE_EMPTY_JSON = "EMPTY_JSON"  # 빈 데이터

# =========================================================
# 3. SQL QUERIES
# =========================================================

SQL_SELECT_RAWID_BY_EVENTIDS = """
SELECT event_id, raw_id
FROM orders_raw
WHERE event_id = ANY(%s);
"""

# ✅ 주의: 이 SQL은 execute_values 전용(%s에 VALUES 목록이 들어감)
SQL_INSERT_RAW = """
INSERT INTO orders_raw (
  event_id,
  run_id, raw_payload, order_id,
  kafka_topic, kafka_partition, kafka_offset,
  ingested_at
)
VALUES %s
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset)
DO NOTHING;
"""

SQL_CHECK_STOCK = "SELECT stock FROM products WHERE product_id = %s"

SQL_UPSERT_ORDER = """
    INSERT INTO orders (
        order_id, user_id, product_id, product_name, shipping_address,
        current_stage, current_status, hold_reason_code, run_id,
        last_event_type, last_occurred_at, raw_reference_id, created_at,
        event_produced_at, latency_p_to_k_sec, latency_p_to_d_sec
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (order_id) DO UPDATE SET
        current_status = CASE
            WHEN orders.current_status = 'HOLD' OR EXCLUDED.current_status = 'HOLD' THEN 'HOLD'
            ELSE EXCLUDED.current_status
        END,
        hold_reason_code = COALESCE(orders.hold_reason_code, EXCLUDED.hold_reason_code),
        last_occurred_at = EXCLUDED.last_occurred_at
    WHERE orders.last_occurred_at <= EXCLUDED.last_occurred_at;
"""

SQL_INSERT_EVENT = """
    INSERT INTO events (
        event_id, run_id, order_id, event_type, current_status, reason_code,
        occurred_at, ingested_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (event_id) DO NOTHING;
"""

SQL_INSERT_ERROR_LOG = """
INSERT INTO pipeline_error_logs (
  run_id, error_type, kafka_topic, kafka_partition, kafka_offset, occurred_at
)
VALUES (%s, %s, %s, %s, %s, NOW())
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset)
DO NOTHING;
"""

# ✅ Slack 중복 방지용
SQL_TRY_INSERT_SLACK_LOG = """
INSERT INTO slack_alert_log (event_id, send_status, run_id, order_id, alert_data)
VALUES (%s, 'PENDING', %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id;
"""

SQL_UPDATE_SLACK_LOG = """
UPDATE slack_alert_log
SET send_status = %s,
    alert_data  = %s
WHERE event_id = %s;
"""

# [소급 적용]
SQL_QUARANTINE_USER = "UPDATE orders SET current_status='HOLD', hold_reason_code=%s WHERE user_id=%s AND created_at >= (%s - INTERVAL '1 minute')"
SQL_QUARANTINE_USER_EVT = "UPDATE events SET current_status='HOLD', reason_code=%s WHERE order_id IN (SELECT order_id FROM orders WHERE user_id=%s AND created_at >= (%s - INTERVAL '1 minute'))"
SQL_QUARANTINE_PROD = "UPDATE orders SET current_status='HOLD', hold_reason_code=%s WHERE product_id=%s AND created_at >= (%s - INTERVAL '1 minute')"
SQL_QUARANTINE_PROD_EVT = "UPDATE events SET current_status='HOLD', reason_code=%s WHERE order_id IN (SELECT order_id FROM orders WHERE product_id=%s AND created_at >= (%s - INTERVAL '1 minute'))"

SLEEP_MS = int(os.getenv("SLEEP_MS", "0"))
CRASH_AFTER_DB_COMMIT = os.getenv("CRASH_AFTER_DB_COMMIT", "false").lower() == "true"


# =========================================================
# 5. 핵심 로직 & 함수
# =========================================================

def parse_iso_datetime(value):
    if not value:
        return datetime.now(timezone.utc)

    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def send_slack_alert(cur, event_id, risk_reason, order_data):
    if not ENABLE_SLACK or not SLACK_WEBHOOK_URL:
        return

    order_id = order_data.get("order_id", "UNKNOWN")
    run_id = order_data.get("run_id", "no_run_id")

    payload = {
        "text": "*Risk Detected! Order Blocked*",
        "attachments": [
            {
                "color": "#ff0000",
                "fields": [
                    {"title": "Reason Code", "value": risk_reason, "short": True},
                    {"title": "Order ID", "value": order_id, "short": True},
                    {"title": "URL", "value": f"http://localhost:8000/orders/{order_id}", "short": True},
                    {"title": "Time", "value": str(datetime.now()), "short": False},
                ],
            }
        ],
    }

    # ✅ 1) DB 선점 (처음 event_id만 Slack 보내기)
    cur.execute(SQL_TRY_INSERT_SLACK_LOG, (event_id, run_id, order_id, Json(payload)))
    inserted = cur.fetchone()
    if not inserted:
        return  # 이미 처리된 event_id -> Slack 중복 방지

    # ✅ 2) Slack 전송
    status = "FAIL"
    try:
        r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=2)
        status = "SENT" if r.status_code == 200 else "FAIL"
    except Exception as e:
        print(f"Slack Error: {e}")

    # ✅ 3) 결과 업데이트
    cur.execute(SQL_UPDATE_SLACK_LOG, (status, Json(payload), event_id))


def check_risk_and_stock(cur, order_data, abuse_tracker, product_tracker):
    uid = str(order_data.get("user_id", "")).strip()
    pid = str(order_data.get("product_id", "")).strip()
    addr = str(order_data.get("shipping_address", "")).strip()
    curr_time = parse_iso_datetime(order_data.get("created_at"))

    if not uid or not pid or len(addr) < 5:
        return CODE_VALID

    cur.execute(SQL_CHECK_STOCK, (pid,))
    result = cur.fetchone()
    if not result:
        return CODE_VALID
    if result[0] <= 0:
        return CODE_INV

    u_q = abuse_tracker[uid]
    u_q.append(curr_time)
    sorted_times = sorted(u_q)
    u_q.clear()
    u_q.extend(sorted_times)
    while u_q and (curr_time - u_q[0]).total_seconds() > USER_BURST_WINDOW:
        u_q.popleft()
    if len(u_q) > USER_BURST_LIMIT:
        return CODE_FRAUD_USER

    p_q = product_tracker[pid]
    p_q.append(curr_time)
    sorted_prod = sorted(p_q)
    p_q.clear()
    p_q.extend(sorted_prod)
    while p_q and (curr_time - p_q[0]).total_seconds() > PROD_BURST_WINDOW:
        p_q.popleft()
    if len(p_q) > PROD_BURST_LIMIT:
        return CODE_FRAUD_PROD

    return None


def apply_quarantine(cur, risk_reason, order):
    at = parse_iso_datetime(order.get("created_at"))
    uid, pid = order.get("user_id"), order.get("product_id")

    if risk_reason == CODE_FRAUD_USER:
        cur.execute(SQL_QUARANTINE_USER, (CODE_FRAUD_USER, uid, at))
        cur.execute(SQL_QUARANTINE_USER_EVT, (CODE_FRAUD_USER, uid, at))
    elif risk_reason == CODE_FRAUD_PROD:
        cur.execute(SQL_QUARANTINE_PROD, (CODE_FRAUD_PROD, pid, at))
        cur.execute(SQL_QUARANTINE_PROD_EVT, (CODE_FRAUD_PROD, pid, at))


# =========================================================
# 메인 실행부 (Bulk Insert 미사용: 건별 INSERT/UPSERT)
# =========================================================
if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    abuse_tracker = defaultdict(deque)
    product_tracker = defaultdict(deque)

    BOOTSTRAP_SERVER_LIST = [s.strip() for s in (BOOTSTRAP_SERVERS or "").split(",") if s.strip()]

    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVER_LIST,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=None,
        consumer_timeout_ms=500,
    )

    print(f"[Consumer-NONBULK] Started. Monitoring: {TOPIC_NAME}")

    processed = 0
    pending = 0
    t0 = time.time()

    LOG_EVERY = int(os.getenv("LOG_EVERY", "1000"))
    DISABLE_RISK = os.getenv("DISABLE_RISK", "false").lower() == "true"

    # ✅ NONBULK도 commit 주기를 bulk와 비슷하게 맞추기 위해 사용
    FLUSH_INTERVAL_SEC = float(os.getenv("FLUSH_INTERVAL_SEC", "3"))
    last_commit = time.time()

    PENDING_FLUSH_INTERVAL_SEC = float(os.getenv("PENDING_FLUSH_INTERVAL_SEC", "3"))
    last_pending_flush = time.time()

    try:
        while True:
            now = time.time()

            for message in consumer:
                # ----------------------------------------------
                # 1) JSON 파싱
                # ----------------------------------------------
                try:
                    order = json.loads(message.value.decode("utf-8"))
                except Exception:
                    # BAD_JSON 로깅
                    try:
                        with conn.cursor() as cur:
                            cur.execute(
                                SQL_INSERT_ERROR_LOG,
                                ("no_run_id", "BAD_JSON", message.topic, message.partition, message.offset),
                            )
                        pending += 1
                        processed += 1

                        now = time.time()
                        if pending >= BATCH_COMMIT or (now - last_pending_flush) >= PENDING_FLUSH_INTERVAL_SEC:
                            conn.commit()
                            consumer.commit()
                            pending = 0
                            last_pending_flush = now
                            last_commit = now

                            if CRASH_AFTER_DB_COMMIT:
                                print("CRASH_AFTER_DB_COMMIT=TRUE -> 강제 종료", flush=True)
                                os._exit(1)

                    except Exception:
                        conn.rollback()
                    continue

                # ----------------------------------------------
                # 2) EMPTY_JSON 처리
                # ----------------------------------------------
                is_empty_payload = not order or (isinstance(order, dict) and set(order.keys()) <= {"run_id", "seq"})
                if is_empty_payload:
                    try:
                        run_id_for_log = order.get("run_id", "no_run_id") if isinstance(order, dict) else "no_run_id"
                        with conn.cursor() as cur:
                            cur.execute(
                                SQL_INSERT_ERROR_LOG,
                                (run_id_for_log, CODE_EMPTY_JSON, message.topic, message.partition, message.offset),
                            )
                        pending += 1
                        processed += 1

                        now = time.time()
                        if pending >= BATCH_COMMIT or (now - last_pending_flush) >= PENDING_FLUSH_INTERVAL_SEC:
                            conn.commit()
                            consumer.commit()
                            pending = 0
                            last_pending_flush = now
                            last_commit = now

                            if CRASH_AFTER_DB_COMMIT:
                                print("CRASH_AFTER_DB_COMMIT=TRUE -> 강제 종료", flush=True)
                                os._exit(1)

                        if processed % LOG_EVERY == 0:
                            dt = time.time() - t0
                            print(f"[Consumer-NONBULK] processed={processed} eps={processed/dt:.1f}", flush=True)

                    except Exception as e:
                        conn.rollback()
                        print(f"Error Logging Failed: {e}")
                    continue

                # ----------------------------------------------
                # 3) 정상 메시지 처리 (건별 DB INSERT/UPSERT)
                # ----------------------------------------------
                event_id = str(
                    uuid.uuid5(uuid.NAMESPACE_DNS, f"{message.topic}_{message.partition}_{message.offset}")
                )
                run_id = (order.get("run_id") or "no_run_id").strip()

                occurred_at = parse_iso_datetime(
                    order.get("occurred_at") or order.get("event_produced_at") or order.get("created_at")
                )
                ingested_at = datetime.now(timezone.utc)

                try:
                    with conn.cursor() as cur:
                        if SLEEP_MS > 0:
                            time.sleep(SLEEP_MS / 1000.0)

                        risk_reason = None if DISABLE_RISK else check_risk_and_stock(
                            cur, order, abuse_tracker, product_tracker
                        )

                        current_status = "HOLD" if risk_reason else order.get("current_status", "PAID")
                        current_stage = order.get("current_stage", "PAYMENT")

                        t_prod = parse_iso_datetime(order.get("event_produced_at"))
                        t_cre = parse_iso_datetime(order.get("created_at"))

                        lat_p_to_k = (ingested_at - t_prod).total_seconds()
                        lat_p_to_d = (datetime.now(timezone.utc) - t_prod).total_seconds()

                        # 3-1) orders_raw 저장 (1건씩)
                        raw_row = [
                            (
                                event_id,
                                run_id,
                                Json(order),
                                order.get("order_id"),
                                message.topic,
                                message.partition,
                                message.offset,
                                ingested_at,
                            )
                        ]
                        execute_values(cur, SQL_INSERT_RAW, raw_row, page_size=1)

                        # 3-2) raw_id 조회 (event_id -> raw_id)
                        cur.execute(SQL_SELECT_RAWID_BY_EVENTIDS, ([event_id],))
                        fetched = cur.fetchone()
                        raw_id = fetched[1] if fetched else None

                        if raw_id is None:
                            raise RuntimeError(f"raw_id not found for event_id={event_id}")

                        # 3-3) orders 단건 UPSERT
                        cur.execute(
                            SQL_UPSERT_ORDER,
                            (
                                order.get("order_id"),
                                order.get("user_id"),
                                order.get("product_id"),
                                order.get("product_name"),
                                order.get("shipping_address"),
                                current_stage,
                                current_status,
                                risk_reason,
                                run_id,
                                "ORDER_CREATED",
                                occurred_at,
                                raw_id,
                                t_cre,
                                t_prod,
                                lat_p_to_k,
                                lat_p_to_d,
                            ),
                        )

                        # 3-4) events 단건 INSERT
                        cur.execute(
                            SQL_INSERT_EVENT,
                            (
                                event_id,
                                run_id,
                                order.get("order_id"),
                                "ORDER_CREATED",
                                current_status,
                                risk_reason,
                                occurred_at,
                                ingested_at,
                            ),
                        )

                        # 3-5) 소급 격리 / Slack
                        if risk_reason in [CODE_FRAUD_USER, CODE_FRAUD_PROD]:
                            apply_quarantine(cur, risk_reason, order)

                        if risk_reason and ENABLE_SLACK:
                            send_slack_alert(cur, event_id, risk_reason, order)

                    processed += 1

                    # ✅ commit 조건 (BATCH_COMMIT 또는 시간 간격)
                    now = time.time()
                    need_commit = (processed % BATCH_COMMIT == 0) or ((now - last_commit) >= FLUSH_INTERVAL_SEC)

                    if need_commit:
                        conn.commit()
                        consumer.commit()
                        last_commit = now

                        if CRASH_AFTER_DB_COMMIT:
                            print("CRASH_AFTER_DB_COMMIT=TRUE -> 강제 종료", flush=True)
                            os._exit(1)

                    if processed % LOG_EVERY == 0:
                        dt = time.time() - t0
                        print(f"[Consumer-NONBULK] processed={processed} eps={processed/dt:.1f}", flush=True)

                    if risk_reason:
                        print(f"[BLOCK] {risk_reason} -> Order: {order.get('order_id')}")

                except Exception as e:
                    conn.rollback()
                    print(f"Processing Error: {e}")

            # consumer timeout( idle ) 이후 pending만 있으면 커밋
            now = time.time()
            if pending > 0 and (pending >= BATCH_COMMIT or (now - last_pending_flush) >= PENDING_FLUSH_INTERVAL_SEC):
                try:
                    conn.commit()
                    consumer.commit()
                    pending = 0
                    last_pending_flush = now
                    last_commit = now

                    if CRASH_AFTER_DB_COMMIT:
                        print("CRASH_AFTER_DB_COMMIT=TRUE -> 강제 종료", flush=True)
                        os._exit(1)

                except Exception as e:
                    conn.rollback()
                    print(f"Pending Commit Error: {e}")

            # idle에서도 너무 오래 commit 안 했으면 commit
            if (now - last_commit) >= FLUSH_INTERVAL_SEC:
                try:
                    conn.commit()
                    consumer.commit()
                    last_commit = now

                    if CRASH_AFTER_DB_COMMIT:
                        print("CRASH_AFTER_DB_COMMIT=TRUE -> 강제 종료", flush=True)
                        os._exit(1)

                except Exception as e:
                    conn.rollback()
                    print(f"Idle Commit Error: {e}")

    except KeyboardInterrupt:
        print("Consumer Stopped")
        try:
            conn.commit()
            consumer.commit()
        except Exception:
            conn.rollback()

    finally:
        conn.close()
        consumer.close()