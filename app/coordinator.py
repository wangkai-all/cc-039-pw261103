"""协调逻辑：分区认领、撤销、转交、消息生产。工作进程与 API 进程共用。"""
import hashlib
import json
import math
import os
import random
import uuid

from .db import (
    ACCOUNTS,
    LEASE_TTL,
    NUM_PARTITIONS,
    WORKER_DEAD_AFTER,
    Tx,
    log_event,
    now,
)


def alive_workers(conn, t=None):
    t = t if t is not None else now()
    return conn.execute(
        "SELECT * FROM workers WHERE state='running' AND last_heartbeat > ?",
        (t - WORKER_DEAD_AFTER,),
    ).fetchall()


def quota_for(n_alive: int) -> int:
    return math.ceil(NUM_PARTITIONS / max(1, n_alive))


def register_worker(conn, worker_id: str, pid: int) -> None:
    with Tx(conn):
        conn.execute(
            """INSERT INTO workers(worker_id, pid, started_at, last_heartbeat, state, fault_mode)
               VALUES (?,?,?,?, 'running', '')
               ON CONFLICT(worker_id) DO UPDATE SET
                 pid=excluded.pid, started_at=excluded.started_at,
                 last_heartbeat=excluded.last_heartbeat, state='running', fault_mode=''""",
            (worker_id, pid, now(), now()),
        )
        log_event(conn, "worker", f"工作进程 {worker_id} 已启动（pid {pid}）", worker_id=worker_id)


def heartbeat(conn, worker_id: str) -> None:
    """心跳 + 续租：把名下所有活跃分区的租约顺延。进程行被重置清掉时自动重新登记。"""
    t = now()
    with Tx(conn):
        cur = conn.execute(
            "UPDATE workers SET last_heartbeat=?, state='running' WHERE worker_id=?",
            (t, worker_id),
        )
        if cur.rowcount == 0:
            conn.execute(
                "INSERT INTO workers(worker_id, pid, started_at, last_heartbeat, state, fault_mode) "
                "VALUES (?,?,?,?, 'running', '')",
                (worker_id, os.getpid(), t, t),
            )
        conn.execute(
            "UPDATE assignments SET lease_expires_at=? WHERE owner_id=? AND status='active'",
            (t + LEASE_TTL, worker_id),
        )


def claim_partitions(conn, worker_id: str):
    """认领空闲或租约到期的分区，直到达到公平份额。交接分两步：先撤销旧归属，再登记新持有者。"""
    claimed = []
    with Tx(conn):
        t = now()
        quota = quota_for(len(alive_workers(conn, t)))
        (owned,) = conn.execute(
            "SELECT COUNT(*) FROM assignments WHERE owner_id=? AND status='active'",
            (worker_id,),
        ).fetchone()

        # 第一步：撤销租约到期的旧归属
        expired = conn.execute(
            """SELECT partition, owner_id, epoch FROM assignments
               WHERE status='active' AND owner_id IS NOT NULL AND lease_expires_at < ?""",
            (t,),
        ).fetchall()
        for row in expired:
            conn.execute(
                """UPDATE assignments SET owner_id=NULL, lease_expires_at=NULL
                   WHERE partition=? AND owner_id=? AND lease_expires_at < ?""",
                (row["partition"], row["owner_id"], t),
            )
            log_event(
                conn, "revoke",
                f"{row['owner_id']} 租约到期，撤销其对分区 {row['partition']} 的归属（任期 {row['epoch']}）",
                partition=row["partition"], worker_id=worker_id,
            )

        # 第二步：登记新持有者，任期编号递增。每轮最多认领一个，
        # 让多个进程在几轮循环内自然达到均衡，而不是先抢光再回吐。
        if owned < quota:
            cand = conn.execute(
                """SELECT partition FROM assignments
                   WHERE status='active' AND owner_id IS NULL
                   ORDER BY partition LIMIT 1"""
            ).fetchone()
            if cand is not None:
                cur = conn.execute(
                    """UPDATE assignments SET owner_id=?, epoch=epoch+1, lease_expires_at=?
                       WHERE partition=? AND owner_id IS NULL""",
                    (worker_id, t + LEASE_TTL, cand["partition"]),
                )
                if cur.rowcount != 0:
                    epoch = conn.execute(
                        "SELECT epoch FROM assignments WHERE partition=?", (cand["partition"],)
                    ).fetchone()["epoch"]
                    log_event(
                        conn, "assign",
                        f"分区 {cand['partition']} 登记给 {worker_id}，任期 {epoch}",
                        partition=cand["partition"], worker_id=worker_id,
                    )
                    claimed.append({"partition": cand["partition"], "epoch": epoch})
    return claimed


def release_all(conn, worker_id: str) -> None:
    """优雅退出：立即释放全部分区，不必等租约到期。"""
    with Tx(conn):
        rows = conn.execute(
            "SELECT partition FROM assignments WHERE owner_id=?", (worker_id,)
        ).fetchall()
        conn.execute(
            "UPDATE assignments SET owner_id=NULL, lease_expires_at=NULL WHERE owner_id=?",
            (worker_id,),
        )
        conn.execute("UPDATE workers SET state='stopped' WHERE worker_id=?", (worker_id,))
        for r in rows:
            log_event(
                conn, "release",
                f"{worker_id} 优雅退出，释放分区 {r['partition']}",
                partition=r["partition"], worker_id=worker_id,
            )


def force_transfer(conn, partition: int, target: str):
    """强制转交：先撤销旧归属，再把分区登记给目标进程（任期递增）。"""
    with Tx(conn):
        row = conn.execute(
            "SELECT * FROM assignments WHERE partition=?", (partition,)
        ).fetchone()
        if row is None or row["status"] != "active":
            raise ValueError(f"分区 {partition} 不在活跃状态，无法转交")
        old = row["owner_id"]
        if old == target:
            raise ValueError(f"分区 {partition} 已属于 {target}")
        conn.execute(
            "UPDATE assignments SET owner_id=NULL, lease_expires_at=NULL WHERE partition=?",
            (partition,),
        )
        if old:
            log_event(
                conn, "revoke",
                f"强制转交：撤销 {old} 对分区 {partition} 的归属（任期 {row['epoch']}）",
                partition=partition, worker_id=target,
            )
        conn.execute(
            """UPDATE assignments SET owner_id=?, epoch=epoch+1, lease_expires_at=?
               WHERE partition=? AND owner_id IS NULL""",
            (target, now() + LEASE_TTL, partition),
        )
        epoch = conn.execute(
            "SELECT epoch FROM assignments WHERE partition=?", (partition,)
        ).fetchone()["epoch"]
        log_event(
            conn, "assign",
            f"分区 {partition} 转交给 {target}，任期 {epoch}",
            partition=partition, worker_id=target,
        )
    return old, epoch


def append_message(conn, partition: int, account=None, amount=None, msg_id=None):
    """本地消息代理：向分区末尾追加一条消息。"""
    account = account or random.choice(ACCOUNTS)
    amount = amount if amount is not None else random.randint(100, 20000)
    msg_id = msg_id or uuid.uuid4().hex[:12]
    payload = json.dumps(
        {"order_id": msg_id, "account": account, "amount": amount}, ensure_ascii=False
    )
    content_hash = hashlib.sha256(
        f"{msg_id}|{account}|{amount}|{payload}".encode()
    ).hexdigest()[:16]
    with Tx(conn):
        row = conn.execute(
            "SELECT COALESCE(MAX(offset), -1) + 1 AS next_off FROM messages WHERE partition=?",
            (partition,),
        ).fetchone()
        conn.execute(
            """INSERT INTO messages(partition, offset, msg_id, account, amount, payload, content_hash, created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (partition, row["next_off"], msg_id, account, amount, payload, content_hash, now()),
        )
    return {"partition": partition, "offset": row["next_off"], "msg_id": msg_id,
            "account": account, "amount": amount}


def produce(conn, count: int):
    """轮流向各分区追加 count 条消息。"""
    with Tx(conn):
        row = conn.execute("SELECT value FROM meta WHERE key='produce_rr'").fetchone()
        rr = int(row["value"]) if row else 0
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('produce_rr', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(rr + count),),
        )
    made = []
    for i in range(count):
        made.append(append_message(conn, (rr + i) % NUM_PARTITIONS))
    return made


def seed_messages(conn, per_partition=8):
    for p in range(NUM_PARTITIONS):
        for _ in range(per_partition):
            append_message(conn, p)
