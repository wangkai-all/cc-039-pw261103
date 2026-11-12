import os
import sqlite3
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.environ.get("DEMO_DB", os.path.join(ROOT, "data", "demo.db"))

NUM_PARTITIONS = 6
LEASE_TTL = 6.0          # 租约有效期（秒）
WORKER_DEAD_AFTER = 8.0  # 心跳超过该时长判定失联
LOOP_INTERVAL = 0.4      # 工作进程主循环间隔（秒）

ACCOUNTS = ["alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
  partition    INTEGER NOT NULL,
  offset       INTEGER NOT NULL,
  msg_id       TEXT NOT NULL,
  account      TEXT NOT NULL,
  amount       INTEGER NOT NULL,   -- 金额，单位：分
  payload      TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  created_at   REAL NOT NULL,
  PRIMARY KEY (partition, offset)
);

CREATE TABLE IF NOT EXISTS assignments (
  partition        INTEGER PRIMARY KEY,
  owner_id         TEXT,                 -- NULL 表示空闲
  epoch            INTEGER NOT NULL DEFAULT 0,  -- 任期编号，每次分配递增
  lease_expires_at REAL,                 -- 租约到期时间（unix 秒）
  status           TEXT NOT NULL DEFAULT 'active'  -- active | stopped
);

CREATE TABLE IF NOT EXISTS positions (
  partition   INTEGER PRIMARY KEY,
  next_offset INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS dedup (
  partition    INTEGER NOT NULL,
  offset       INTEGER NOT NULL,
  msg_id       TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  worker_id    TEXT NOT NULL,
  epoch        INTEGER NOT NULL,
  processed_at REAL NOT NULL,
  PRIMARY KEY (partition, offset)
);

CREATE TABLE IF NOT EXISTS ledger (
  account TEXT PRIMARY KEY,
  balance INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ledger_entries (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  account    TEXT NOT NULL,
  amount     INTEGER NOT NULL,
  partition  INTEGER NOT NULL,
  offset     INTEGER NOT NULL,
  msg_id     TEXT NOT NULL,
  worker_id  TEXT NOT NULL,
  epoch      INTEGER NOT NULL,
  created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS workers (
  worker_id      TEXT PRIMARY KEY,
  pid            INTEGER,
  started_at     REAL,
  last_heartbeat REAL,
  state          TEXT NOT NULL DEFAULT 'running',  -- running | stopped | crashed
  fault_mode     TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS conflicts (
  partition     INTEGER PRIMARY KEY,
  offset        INTEGER NOT NULL,
  expected_hash TEXT NOT NULL,
  actual_hash   TEXT NOT NULL,
  detail        TEXT,
  detected_at   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  ts         REAL NOT NULL,
  kind       TEXT NOT NULL,
  partition  INTEGER,
  worker_id  TEXT,
  message    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT
);
"""


def now() -> float:
    return time.time()


def connect() -> sqlite3.Connection:
    """每个进程各自打开连接；autocommit 模式，事务用显式 BEGIN/COMMIT。"""
    conn = sqlite3.connect(DB_PATH, timeout=15, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=15000")
    return conn


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = connect()
    try:
        conn.executescript(SCHEMA)
        for p in range(NUM_PARTITIONS):
            conn.execute("INSERT OR IGNORE INTO assignments(partition) VALUES (?)", (p,))
            conn.execute("INSERT OR IGNORE INTO positions(partition) VALUES (?)", (p,))
    finally:
        conn.close()


def log_event(conn, kind: str, message: str, partition=None, worker_id=None) -> None:
    conn.execute(
        "INSERT INTO events(ts, kind, partition, worker_id, message) VALUES (?,?,?,?,?)",
        (now(), kind, partition, worker_id, message),
    )


class Tx:
    """显式事务上下文：BEGIN IMMEDIATE 立即拿写锁，保证检查与写入原子。"""

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        self.conn.execute("BEGIN IMMEDIATE")
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.conn.execute("COMMIT")
        else:
            self.conn.execute("ROLLBACK")
        return False
