CREATE TABLE IF NOT EXISTS pipeline_error_logs (
    log_id SERIAL PRIMARY KEY,
    error_type VARCHAR NOT NULL,    -- 'EMPTY_JSON'만 들어갈 예정
    run_id TEXT, 
    kafka_topic VARCHAR,
    kafka_partition INT,
    kafka_offset BIGINT,                -- 위치 추적용
    occurred_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE TABLE slack_alert_log (
    event_id VARCHAR(50) PRIMARY KEY,
    send_status VARCHAR(20) NOT NULL, --알림 전송 상태 ( sent, fail )
    run_id TEXT,                         -- ✅ 추가
    order_id TEXT,
    alert_data JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS orders_raw (
    raw_id           BIGSERIAL PRIMARY KEY,
    event_id         TEXT NOT NULL UNIQUE,      -- ✅ 추가(매핑 키)
    raw_payload      JSONB NOT NULL,
    order_id         TEXT,
    run_id           TEXT NOT NULL,

    kafka_topic      TEXT NOT NULL,
    kafka_partition  INT  NOT NULL,
    kafka_offset     BIGINT NOT NULL,
    ingested_at      TIMESTAMPTZ DEFAULT now(),

    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS orders (
    -- 1. 주문 기본 정보
    order_id            TEXT PRIMARY KEY,
    user_id             TEXT NOT NULL,
    product_id          TEXT NOT NULL,
    product_name        TEXT NOT NULL,
    shipping_address    TEXT NOT NULL,
    
    -- 2. 비즈니스 시각 및 상태 (운영용)
    current_stage       TEXT NOT NULL,          
    current_status      TEXT NOT NULL,          
    last_event_type     TEXT NOT NULL,          
    last_occurred_at    TIMESTAMPTZ NOT NULL,   -- [비즈니스 시각] 이벤트 발생 시각
    
    -- 3. [추가] 시스템 성능 측정 시각 (엔지니어링용)
    event_produced_at   TIMESTAMPTZ NULL,   -- [T0] 주문을 모으기 시작한 시간
    latency_p_to_k_sec  NUMERIC(10, 4) NULL,    -- [T1 - T0] 주문이 consumer에 들어
    latency_p_to_d_sec  NUMERIC(10, 4) NULL,    -- [T2 - T0] 전체 지연
    
    -- 4. 리스크 관리
    hold_reason_code    TEXT NULL,              -- 'FUL-FRAUD-USER'
    hold_ops_status     TEXT NULL,
    hold_ops_note       TEXT NULL,
    hold_ops_operator   TEXT NULL,
    hold_ops_updated_at TIMESTAMPTZ NULL,
    
    -- 5. 시스템 기록
    raw_reference_id    BIGINT NOT NULL,
    created_at          TIMESTAMPTZ DEFAULT now(), -- [T2] DB 최종 적재 시각
    run_id TEXT NULL,
    
    CONSTRAINT fk_orders_raw FOREIGN KEY (raw_reference_id) REFERENCES orders_raw(raw_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_run_id ON orders(run_id);

CREATE TABLE IF NOT EXISTS events (
    event_id        TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL,
    order_id        TEXT NULL,             -- orders table order_id
    event_type      TEXT NOT NULL,         -- 
    current_status  TEXT NOT NULL,         -- HOLD
    reason_code     TEXT NULL,             -- FUL-INV, FUL-VALID
    
    occurred_at     TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ DEFAULT now(),
    
    ops_status      TEXT NULL,
    ops_note        TEXT NULL,
    ops_operator    TEXT NULL,
    ops_updated_at  TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id);

CREATE TABLE IF NOT EXISTS metrics_window (
  -- ------------------------------------------
  -- (A) 집계 윈도우 키
  -- ------------------------------------------
  window_start   timestamptz NOT NULL,
  window_end     timestamptz NOT NULL,

  -- ------------------------------------------
  -- (B) "윈도우 내 발생량"(Flow) 지표
  --  - 이 30분 구간 동안 '몇 개가 발생했는가?'
  --  - SUM 집계가 자연스러운 값들
  -- ------------------------------------------
  orders         int NOT NULL,            -- (예) ORDER_CREATED 발생량
  payments       int NOT NULL,            -- (예) PAID 발생량
  shipped        int NOT NULL,            -- (예) SHIPPED 발생량
  holds          int NOT NULL,            -- (예) HOLD 발생량
  ingest_count   int NOT NULL,            -- (예) ingested_at 기준 유입 이벤트 수
  parse_errors   int NOT NULL,            -- (예) 파싱 오류 수(있으면)
  schema_missing int NOT NULL,            -- (예) 필수값 누락 수(있으면)

  -- 이벤트 관점 확장(윈도우 내)
  event_total    int NOT NULL DEFAULT 0,  -- (윈도우 내) 전체 이벤트 수(ingested 기준 등)
  event_errors   int NOT NULL DEFAULT 0,  -- (윈도우 내) reason_code 존재 이벤트 수

  -- ------------------------------------------
  -- (C) 비율 / 지연 같은 파생 지표
  --  - hold_rate: 보통 holds/orders 같은 파생치
  --  - delay_p95_sec: 지연(ingested - occurred)의 p95
  -- ------------------------------------------
  hold_rate      numeric(6,3) NOT NULL,
  delay_p95_sec  numeric(10,3) NOT NULL DEFAULT 0,

  -- ------------------------------------------
  -- (D) "현재 상태"(Snapshot) 지표
  --  - 특정 시점(window_end 근처)의 '현재 값'을 찍는 느낌
  --  - SUM이 아니라 "마지막값(last value)"이 자연스러운 값들
  -- ------------------------------------------
  backlog        int NOT NULL DEFAULT 0,  -- 완료/취소 제외한 현재 미처리(스냅샷)
  holds_now      int NOT NULL DEFAULT 0,  -- 현재 hold_reason_code 존재 주문 수(스냅샷)
  late_orders    int NOT NULL DEFAULT 0,  -- promised_delivery_date 지나고 미완료 주문 수(스냅샷)

  -- 알림 상태 스냅샷(ops_status/hold_ops_status 기반으로 파생)
  alerts_open     int NOT NULL DEFAULT 0,
  alerts_ack      int NOT NULL DEFAULT 0,
  alerts_resolved int NOT NULL DEFAULT 0,
  alerts_retry    int NOT NULL DEFAULT 0,

  -- ------------------------------------------
  -- (E) 메타
  -- ------------------------------------------
  created_at     timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY(window_start, window_end)
);

CREATE TABLE IF NOT EXISTS products (
  product_id   TEXT PRIMARY KEY,
  product_name TEXT NOT NULL,
  stock        INT  NOT NULL DEFAULT 0 CHECK (stock >= 0),
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);