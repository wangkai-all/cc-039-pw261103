"""工作进程：认领分区、按 offset 顺序处理消息。

核心不变式：核对持有者/任期/租约 + 写业务结果 + 推进位置，全部放在同一个
SQLite 事务里（BEGIN IMMEDIATE 拿写锁，与协调方的撤销/登记互斥），
因此分区转交后旧持有者的写入与位置提交都会被拒绝。
"""
import argparse
import os
import signal
import sys
import time

from .db import LOOP_INTERVAL, WORKER_DEAD_AFTER, Tx, connect, init_db, log_event, now
from . import coordinator

STOP = False


def _handle_signal(signum, frame):
    global STOP
    STOP = True


def fenced_process_one(conn, worker_id: str, partition: int):
    """处理分区的下一条消息。返回 (结果, 消息行或 None)。

    结果: processed / skipped / conflict / empty / fenced
    """
    fault = conn.execute(
        "SELECT fault_mode FROM workers WHERE worker_id=?", (worker_id,)
    ).fetchone()["fault_mode"]

    with Tx(conn):
        # ---- fencing： holder / epoch / lease 三重核对（在写锁保护下）----
        a = conn.execute(
            "SELECT * FROM assignments WHERE partition=?", (partition,)
        ).fetchone()
        t = now()
        if (
            a is None
            or a["status"] != "active"
            or a["owner_id"] != worker_id
            or a["lease_expires_at"] is None
            or a["lease_expires_at"] <= t
        ):
            raise _Fenced()

        epoch = a["epoch"]
        pos = conn.execute(
            "SELECT next_offset FROM positions WHERE partition=?", (partition,)
        ).fetchone()["next_offset"]
        msg = conn.execute(
            "SELECT * FROM messages WHERE partition=? AND offset=?", (partition, pos)
        ).fetchone()
        if msg is None:
            return "empty", None

        # ---- 去重：同一 (partition, offset) 只入账一次 ----
        d = conn.execute(
            "SELECT * FROM dedup WHERE partition=? AND offset=?", (partition, pos)
        ).fetchone()
        if d is not None:
            if d["content_hash"] == msg["content_hash"]:
                conn.execute(
                    "UPDATE positions SET next_offset=? WHERE partition=?",
                    (pos + 1, partition),
                )
                log_event(
                    conn, "skip",
                    f"分区 {partition} offset {pos} 重复投递且内容一致，跳过入账",
                    partition=partition, worker_id=worker_id,
                )
                return "skipped", msg
            conn.execute(
                """INSERT OR REPLACE INTO conflicts(partition, offset, expected_hash, actual_hash, detail, detected_at)
                   VALUES (?,?,?,?,?,?)""",
                (
                    partition, pos, d["content_hash"], msg["content_hash"],
                    f"offset {pos} 已按内容哈希 {d['content_hash']} 入账，"
                    f"再次送达的内容哈希变为 {msg['content_hash']}",
                    now(),
                ),
            )
            conn.execute(
                "UPDATE assignments SET status='stopped', owner_id=NULL, lease_expires_at=NULL "
                "WHERE partition=?",
                (partition,),
            )
            log_event(
                conn, "conflict",
                f"分区 {partition} offset {pos} 重复投递内容不一致，分区已停止，等待人工处理",
                partition=partition, worker_id=worker_id,
            )
            return "conflict", msg

        # ---- 业务写入：去重记录 + 入账流水 + 账户余额 + 推进位置 ----
        conn.execute(
            "INSERT INTO dedup(partition, offset, msg_id, content_hash, worker_id, epoch, processed_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (partition, pos, msg["msg_id"], msg["content_hash"], worker_id, epoch, now()),
        )
        conn.execute(
            "INSERT INTO ledger_entries(account, amount, partition, offset, msg_id, worker_id, epoch, created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (msg["account"], msg["amount"], partition, pos, msg["msg_id"], worker_id, epoch, now()),
        )
        conn.execute(
            "INSERT INTO ledger(account, balance) VALUES (?,?) "
            "ON CONFLICT(account) DO UPDATE SET balance=balance+excluded.balance",
            (msg["account"], msg["amount"]),
        )
        conn.execute(
            "UPDATE positions SET next_offset=? WHERE partition=?", (pos + 1, partition)
        )

        if fault == "crash_before_commit":
            _consume_fault(conn, worker_id)
            # 事务未提交，进程直接退出 → 回滚，消息将被重新处理
            os._exit(1)

    # 事务已提交（with 块正常退出即 COMMIT）。下面的确认事件就是“ack”。
    if fault == "crash_after_commit":
        os._exit(1)  # 提交后、确认前退出：数据已落库，不会重复入账

    log_event(
        conn, "process",
        f"分区 {partition} offset {pos} 入账：{msg['account']} +{msg['amount']/100:.2f} 元（任期 {epoch}）",
        partition=partition, worker_id=worker_id,
    )
    return "processed", msg


class _Fenced(Exception):
    pass


def _consume_fault(conn, worker_id: str):
    conn.execute("UPDATE workers SET fault_mode='' WHERE worker_id=?", (worker_id,))


def zombie_attempt(conn, worker_id: str, partition: int):
    """故障注入：模拟进程卡顿/网络分区——持有租约但停止心跳，
    等分区被转交后，旧任期的迟到写入与位置提交必须被拒绝。"""
    a = conn.execute(
        "SELECT * FROM assignments WHERE partition=?", (partition,)
    ).fetchone()
    if a is None or a["owner_id"] != worker_id:
        return
    epoch = a["epoch"]
    _consume_fault(conn, worker_id)
    log_event(
        conn, "fault",
        f"{worker_id} 模拟卡顿：持有分区 {partition}（任期 {epoch}）后停止心跳，"
        f"稍后将携带旧任期迟到写入",
        partition=partition, worker_id=worker_id,
    )
    time.sleep(WORKER_DEAD_AFTER + 2)  # 不心跳：租约到期，分区被其他进程接管

    # 旧进程“醒来”，试图用旧任期写入业务结果并推进位置
    rejected_detail = None
    with Tx(conn):  # 在写锁下重新核对：持有者 / 任期 / 租约
        cur = conn.execute(
            "SELECT * FROM assignments WHERE partition=?", (partition,)
        ).fetchone()
        still_mine = (
            cur is not None
            and cur["owner_id"] == worker_id
            and cur["epoch"] == epoch
            and cur["lease_expires_at"] is not None
            and cur["lease_expires_at"] > now()
        )
        if not still_mine:
            if cur is not None and cur["owner_id"] == worker_id:
                rejected_detail = "租约已过期"
            elif cur is not None:
                rejected_detail = f"当前持有者 {cur['owner_id']}，任期 {cur['epoch']}"
            else:
                rejected_detail = "分区记录缺失"
        # 注意：此处故意不写任何业务数据——fencing 核对失败即放弃
    if rejected_detail is not None:
        log_event(
            conn, "reject",
            f"拒绝 {worker_id} 对分区 {partition} 的迟到写入与位置提交："
            f"旧任期 {epoch} 已失效（{rejected_detail}）",
            partition=partition, worker_id=worker_id,
        )
    # 恢复心跳，重新参与认领
    coordinator.heartbeat(conn, worker_id)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    args = parser.parse_args()
    worker_id = args.id

    init_db()
    conn = connect()
    coordinator.register_worker(conn, worker_id, os.getpid())

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    while not STOP:
        try:
            coordinator.heartbeat(conn, worker_id)
            coordinator.claim_partitions(conn, worker_id)
            rows = conn.execute(
                "SELECT partition FROM assignments WHERE owner_id=? AND status='active' "
                "ORDER BY partition",
                (worker_id,),
            ).fetchall()
            fault = conn.execute(
                "SELECT fault_mode FROM workers WHERE worker_id=?", (worker_id,)
            ).fetchone()["fault_mode"]
            for r in rows:
                p = r["partition"]
                if fault == "zombie":
                    zombie_attempt(conn, worker_id, p)
                    fault = ""
                    break  # 归属已变化，下一轮重新读取
                try:
                    fenced_process_one(conn, worker_id, p)
                except _Fenced:
                    log_event(
                        conn, "reject",
                        f"{worker_id} 对分区 {p} 的写入被 fencing 拒绝（已失去归属或租约过期）",
                        partition=p, worker_id=worker_id,
                    )
                if STOP:
                    break
        except Exception as exc:  # 让进程活着，错误可见
            log_event(conn, "error", f"{worker_id} 主循环异常：{exc!r}", worker_id=worker_id)
            time.sleep(1)
        time.sleep(LOOP_INTERVAL)

    coordinator.release_all(conn, worker_id)
    sys.exit(0)


if __name__ == "__main__":
    main()
