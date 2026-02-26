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
USER_BURST_WINDOW = 10.0; USER_BURST_LIMIT = 5
PROD_BURST_WINDOW = 1.0;  PROD_BURST_LIMIT = 5

# 🚨 [리스크 코드 정의]
CODE_VALID       = "FUL-VALID"       # 정보 누락
CODE_INV         = "FUL-INV"         # 재고 부족
CODE_FRAUD_USER  = "FUL-FRAUD-USER"  # 유저 도배
CODE_FRAUD_PROD  = "FUL-FRAUD-PROD"  # 상품 폭주
CODE_EMPTY_JSON  = "EMPTY_JSON"      # 빈 데이터

# =========================================================
# 3. SQL QUERIES
# =========================================================

SQL_SELECT_RAWID_BY_EVENTIDS = """
SELECT event_id, raw_id
FROM orders_raw
WHERE event_id = ANY(%s);
"""

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

# [핵심 1] 에러 로그 적재 (Empty JSON 용)
SQL_UPSERT_SLACK_LOG = """
INSERT INTO slack_alert_log (event_id, send_status, run_id, order_id, alert_data)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (event_id) DO UPDATE
SET send_status = EXCLUDED.send_status,
    alert_data  = EXCLUDED.alert_data;
"""

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
SQL_UPSERT_ORDER_BULK = """
INSERT INTO orders (
  order_id, user_id, product_id, product_name, shipping_address,
  current_stage, current_status, hold_reason_code, run_id,
  last_event_type, last_occurred_at, raw_reference_id, created_at,
  event_produced_at, latency_p_to_k_sec, latency_p_to_d_sec
)
VALUES %s
ON CONFLICT (order_id) DO UPDATE SET
  current_status = CASE
    WHEN orders.current_status = 'HOLD' OR EXCLUDED.current_status = 'HOLD' THEN 'HOLD'
    ELSE EXCLUDED.current_status
  END,
  hold_reason_code = COALESCE(orders.hold_reason_code, EXCLUDED.hold_reason_code),
  last_occurred_at = EXCLUDED.last_occurred_at
WHERE orders.last_occurred_at <= EXCLUDED.last_occurred_at;
"""


# [수정] 제공해주신 CREATE TABLE 스키마에 맞춘 7개 컬럼 매핑
SQL_INSERT_EVENT = """
    INSERT INTO events (
        event_id, run_id, order_id, event_type, current_status, reason_code, 
        occurred_at, ingested_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (event_id) DO NOTHING
"""

SQL_INSERT_EVENT_BULK = """
INSERT INTO events (
  event_id, run_id, order_id, event_type, current_status, reason_code,
  occurred_at, ingested_at
)
VALUES %s
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

# ✅ 1) 처음 보는 event_id만 "선점(INSERT)" 시도
SQL_TRY_INSERT_SLACK_LOG = """
INSERT INTO slack_alert_log (event_id, send_status, run_id, order_id, alert_data)
VALUES (%s, 'PENDING', %s, %s, %s)
ON CONFLICT (event_id) DO NOTHING
RETURNING event_id;
"""

# ✅ 2) Slack 전송 결과 업데이트
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
    if not value: return datetime.now(timezone.utc)
    if isinstance(value, datetime): return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))

def send_slack_alert(cur, event_id, risk_reason, order_data):
    if not ENABLE_SLACK or not SLACK_WEBHOOK_URL:
        return

    order_id = order_data.get("order_id", "UNKNOWN")
    run_id = order_data.get("run_id", "no_run_id")

    payload = {
        "text": "*Risk Detected! Order Blocked*",
        "attachments": [{
            "color": "#ff0000",
            "fields": [
                {"title": "Reason Code", "value": risk_reason, "short": True},
                {"title": "Order ID", "value": order_id, "short": True},
                {"title": "URL", "value": f"http://localhost:8000/orders/{order_id}", "short": True},
                {"title": "Time", "value": str(datetime.now()), "short": False},
            ]
        }]
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
    if not result: return CODE_VALID
    if result[0] <= 0: return CODE_INV

    u_q = abuse_tracker[uid]
    u_q.append(curr_time)
    sorted_times = sorted(u_q)
    u_q.clear(); u_q.extend(sorted_times)
    while u_q and (curr_time - u_q[0]).total_seconds() > USER_BURST_WINDOW: u_q.popleft()
    if len(u_q) > USER_BURST_LIMIT: return CODE_FRAUD_USER

    p_q = product_tracker[pid]
    p_q.append(curr_time)
    sorted_prod = sorted(p_q)
    p_q.clear(); p_q.extend(sorted_prod)
    while p_q and (curr_time - p_q[0]).total_seconds() > PROD_BURST_WINDOW: p_q.popleft()
    if len(p_q) > PROD_BURST_LIMIT: return CODE_FRAUD_PROD

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
# 메인 실행부
# =========================================================
if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    abuse_tracker = defaultdict(deque)
    product_tracker = defaultdict(deque)

    consumer = KafkaConsumer(
        TOPIC_NAME, 
        bootstrap_servers=[BOOTSTRAP_SERVERS], 
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    print(f"[Consumer] Started. Monitoring: {TOPIC_NAME}")
    try:
        LOG_EVERY = int(os.getenv("LOG_EVERY", "1000"))
        DISABLE_RISK = os.getenv("DISABLE_RISK", "false").lower() == "true"

       
        processed = 0
        pending = 0
        t0 = time.time()
            
        DB_BATCH_SIZE = int(os.getenv("DB_BATCH_SIZE", "1000"))
        raw_rows = []
        event_ids = []
        order_pre_rows = []
        event_rows = []
        
        for message in consumer:
           
            order = message.value
            is_empty_payload = (
            not order
            or (isinstance(order, dict) and set(order.keys()) <= {"run_id", "seq"})
            )
            if is_empty_payload:
                try:
                    run_id_for_log = order.get("run_id", "no_run_id") if isinstance(order, dict) else "no_run_id"
                    with conn.cursor() as cur:
                        cur.execute(SQL_INSERT_ERROR_LOG, (
                        run_id_for_log, CODE_EMPTY_JSON, message.topic, message.partition, message.offset
                        ))
                    pending += 1
                    processed += 1
                    
                    if pending >= BATCH_COMMIT and len(raw_rows) == 0:
                        conn.commit()
                        consumer.commit()
                        pending = 0
                    if CRASH_AFTER_DB_COMMIT:
                        print("CRASH_AFTER_DB_COMMIT=TRUE -> 강제 종료", flush=True)
                        os._exit(1)                
                    if processed % LOG_EVERY == 0:
                        dt = time.time() - t0
                        print(f"[Consumer] processed={processed} eps={processed/dt:.1f}", flush=True)
                except Exception as e:
                    conn.rollback()
                    print(f"Error Logging Failed: {e}")         
                continue 
            event_id = str(uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"{message.topic}_{message.partition}_{message.offset}"
            ))
            run_id = (order.get("run_id") or "no_run_id").strip()
                
            occurred_at = parse_iso_datetime(
            order.get("occurred_at") or order.get("event_produced_at") or order.get("created_at")
            )
                
            ingested_at = datetime.now(timezone.utc)

            try:
                flushed = False
                with conn.cursor() as cur:
                    if SLEEP_MS > 0:
                        time.sleep(SLEEP_MS / 1000.0)
                    risk_reason = None if DISABLE_RISK else check_risk_and_stock(cur, order, abuse_tracker, product_tracker)
                    current_status = "HOLD" if risk_reason else order.get('current_status', 'PAID')
                    current_stage = order.get('current_stage', 'PAYMENT')
                    
                    t_prod = parse_iso_datetime(order.get('event_produced_at'))
                    t_cre = parse_iso_datetime(order.get('created_at'))
                    raw_rows.append((
                        event_id,
                        run_id, Json(order), order.get("order_id"),
                        message.topic, message.partition, message.offset,
                        ingested_at
                    ))
                    event_ids.append(event_id)
                    # 3. Orders 테이블 저장
                    
                    t_prod = parse_iso_datetime(order.get('event_produced_at'))
                    t_cre  = parse_iso_datetime(order.get('created_at'))

                    lat_p_to_k = (ingested_at - t_prod).total_seconds()
                    lat_p_to_d = (datetime.now(timezone.utc) - t_prod).total_seconds()
                    # cur.execute(SQL_UPSERT_ORDER, (
                    #     order.get('order_id'), order.get('user_id'), order.get('product_id'), 
                    #     order.get('product_name'), order.get('shipping_address'), 
                    #     current_stage, current_status, risk_reason, run_id,
                    #     "ORDER_CREATED", occurred_at, raw_id, t_cre, t_prod, lat_p_to_k, lat_p_to_d
                    # ))
                    order_pre_rows.append((
                        event_id,  # 나중에 raw_id 매핑 키
                        order.get('order_id'), order.get('user_id'), order.get('product_id'),
                        order.get('product_name'), order.get('shipping_address'),
                        current_stage, current_status, risk_reason, run_id,
                        "ORDER_CREATED", occurred_at,
                        t_cre, t_prod, lat_p_to_k, lat_p_to_d
                    ))
                    
                    event_rows.append((
                        event_id, run_id, order.get('order_id'),
                        "ORDER_CREATED", current_status, risk_reason,
                        occurred_at, ingested_at
                    ))
                    # 4. Events 테이블 저장 (제공해주신 스키마 7개 컬럼에 맞춰 수정)
                    # cur.execute(SQL_INSERT_EVENT, (
                    #     event_id,                 # event_id
                    #     run_id,
                    #     order.get('order_id'),  # order_id
                    #     "ORDER_CREATED",        # event_type
                    #     current_status,         # current_status
                    #     risk_reason,            # reason_code
                    #     occurred_at,                  # occurred_at
                    #     ingested_at                   # ingested_at
                    # ))

                    if risk_reason in [CODE_FRAUD_USER, CODE_FRAUD_PROD]:
                        apply_quarantine(cur, risk_reason, order)

                    if risk_reason and ENABLE_SLACK:
                        send_slack_alert(cur, event_id, risk_reason, order)
                    

                    # ✅ 배치 크기 도달 시 3테이블 한 번에 flush
                    if len(raw_rows) >= DB_BATCH_SIZE:
                        execute_values(cur, SQL_INSERT_RAW, raw_rows, page_size=DB_BATCH_SIZE)
                        cur.execute(SQL_SELECT_RAWID_BY_EVENTIDS, (event_ids,))
                        raw_map = dict(cur.fetchall())
                        latest_by_order = {}

                        for r in order_pre_rows:
                            eid = r[0]
                            raw_id = raw_map.get(eid)
                            if raw_id is None:
                                continue

                            order_id = r[1]
                            last_occ = r[11]  # occurred_at

                            row = (
                                r[1], r[2], r[3], r[4], r[5],
                                r[6], r[7], r[8], r[9],
                                r[10], r[11], raw_id,
                                r[12], r[13], r[14], r[15]
                            )

                            prev = latest_by_order.get(order_id)
                            if (prev is None) or (prev[0] <= last_occ):
                                latest_by_order[order_id] = (last_occ, row)

                        final_orders = [v[1] for v in latest_by_order.values()]

                        if final_orders:
                            execute_values(cur, SQL_UPSERT_ORDER_BULK, final_orders, page_size=DB_BATCH_SIZE)

                        if event_rows:
                            execute_values(cur, SQL_INSERT_EVENT_BULK, event_rows, page_size=DB_BATCH_SIZE)

                        flushed = True
                if flushed:
                    conn.commit()
                    consumer.commit()

                    raw_rows.clear()
                    event_ids.clear()
                    order_pre_rows.clear()
                    event_rows.clear()
                pending += 1
                processed += 1

                # if pending >= BATCH_COMMIT:
                #     conn.commit()
                #     consumer.commit()
                #     pending = 0

                if processed % LOG_EVERY == 0:
                    dt = time.time() - t0
                    print(f"[Consumer] processed={processed} eps={processed/dt:.1f}", flush=True)

                if CRASH_AFTER_DB_COMMIT:
                    print("CRASH_AFTER_DB_COMMIT=TRUE -> 강제 종료", flush=True)
                    os._exit(1)
                        
                if risk_reason:
                    print(f"[BLOCK] {risk_reason} -> Order: {order.get('order_id')}")

            except Exception as e:
                conn.rollback()
                print(f"Processing Error: {e}")

    except KeyboardInterrupt:
        print("Consumer Stopped")
        if raw_rows:
            with conn.cursor() as cur:
                execute_values(cur, SQL_INSERT_RAW, raw_rows, page_size=DB_BATCH_SIZE)

                cur.execute(SQL_SELECT_RAWID_BY_EVENTIDS, (event_ids,))
                raw_map = dict(cur.fetchall())

                final_orders = []
                latest_by_order = {}  # order_id -> (last_occurred_at, row_tuple)

                for r in order_pre_rows:
                    eid = r[0]
                    raw_id = raw_map.get(eid)
                    if raw_id is None:
                        continue

                    order_id = r[1]
                    last_occ = r[11]  # occurred_at

                    row = (
                        r[1], r[2], r[3], r[4], r[5],
                        r[6], r[7], r[8], r[9],
                        r[10], r[11], raw_id,
                        r[12], r[13], r[14], r[15]
                    )

                    prev = latest_by_order.get(order_id)
                    if (prev is None) or (prev[0] <= last_occ):
                        latest_by_order[order_id] = (last_occ, row)

                final_orders = [v[1] for v in latest_by_order.values()]

                if final_orders:
                    execute_values(cur, SQL_UPSERT_ORDER_BULK, final_orders, page_size=DB_BATCH_SIZE)
                if event_rows:
                    execute_values(cur, SQL_INSERT_EVENT_BULK, event_rows, page_size=DB_BATCH_SIZE)
                
            conn.commit()
            consumer.commit()
    finally:
        conn.close()
        consumer.close()