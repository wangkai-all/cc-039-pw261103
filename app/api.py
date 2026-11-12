"""FastAPI 服务：状态快照、工作进程管理、演示操作入口、一致性核对。"""
import asyncio
import os
import signal
import subprocess
import sys
import time

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import coordinator
from .db import (
    ACCOUNTS,
    DB_PATH,
    LEASE_TTL,
    NUM_PARTITIONS,
    ROOT,
    WORKER_DEAD_AFTER,
    Tx,
    connect,
    init_db,
    log_event,
    now,
)

FAULT_MODES = {"crash_before_commit", "crash_after_commit", "zombie"}
WORKER_LOG_DIR = os.path.join(ROOT, "data")

app = FastAPI(title="分区消费演示")

# ---------------------------------------------------------------- 后台任务


async def rebalancer():
    """每 2 秒：标记失联进程；在存活进程间做温和再均衡（差值 >=2 时收回一个分区）。"""
    while True:
        await asyncio.sleep(2)
        try:
            conn = connect()
            try:
                t = now()
                with Tx(conn):
                    dead = conn.execute(
                        "SELECT worker_id FROM workers WHERE state='running' AND last_heartbeat < ?",
                        (t - WORKER_DEAD_AFTER,),
                    ).fetchall()
                    for d in dead:
                        conn.execute(
                            "UPDATE workers SET state='crashed' WHERE worker_id=?",
                            (d["worker_id"],),
                        )
                        log_event(
                            conn, "crash",
                            f"{d['worker_id']} 心跳丢失，判定失联；其分区将在租约到期后被回收",
                            worker_id=d["worker_id"],
                        )
                    conn.execute(
                        "DELETE FROM events WHERE id < (SELECT COALESCE(MAX(id),0) FROM events) - 500"
                    )

                alive = coordinator.alive_workers(conn)
                if len(alive) >= 2:
                    counts = {}
                    for w in alive:
                        (c,) = conn.execute(
                            "SELECT COUNT(*) FROM assignments WHERE owner_id=? AND status='active'",
                            (w["worker_id"],),
                        ).fetchone()
                        counts[w["worker_id"]] = c
                    hi = max(counts, key=counts.get)
                    lo = min(counts, key=counts.get)
                    row = conn.execute(
                        "SELECT value FROM meta WHERE key='rebalance_last'"
                    ).fetchone()
                    last = float(row["value"]) if row else 0.0
                    if counts[hi] - counts[lo] >= 2 and t - last >= 3:
                        with Tx(conn):
                            part = conn.execute(
                                """SELECT partition FROM assignments
                                   WHERE owner_id=? AND status='active' AND lease_expires_at > ?
                                   ORDER BY partition LIMIT 1""",
                                (hi, t),
                            ).fetchone()
                            if part:
                                conn.execute(
                                    "UPDATE assignments SET owner_id=NULL, lease_expires_at=NULL "
                                    "WHERE partition=?",
                                    (part["partition"],),
                                )
                                conn.execute(
                                    "INSERT INTO meta(key, value) VALUES ('rebalance_last', ?) "
                                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                                    (str(t),),
                                )
                                log_event(
                                    conn, "revoke",
                                    f"再均衡：收回 {hi} 的分区 {part['partition']}，等待 {lo} 认领",
                                    partition=part["partition"],
                                )
            finally:
                conn.close()
        except Exception:
            pass


async def autoproducer():
    """自动生产：开启后每 1.5 秒追加 1 条消息。"""
    while True:
        await asyncio.sleep(1.5)
        try:
            conn = connect()
            try:
                row = conn.execute("SELECT value FROM meta WHERE key='autoproduce'").fetchone()
                if row and row["value"] == "on":
                    coordinator.produce(conn, 1)
            finally:
                conn.close()
        except Exception:
            pass


@app.on_event("startup")
async def startup():
    init_db()
    asyncio.create_task(rebalancer())
    asyncio.create_task(autoproducer())


# ---------------------------------------------------------------- 工具


def _pid_of_worker(worker_id: str) -> int:
    conn = connect()
    try:
        row = conn.execute(
            "SELECT pid FROM workers WHERE worker_id=?", (worker_id,)
        ).fetchone()
    finally:
        conn.close()
    if row is None or row["pid"] is None:
        raise HTTPException(404, f"工作进程 {worker_id} 不存在")
    return row["pid"]


def _check_worker_pid(worker_id: str, pid: int) -> None:
    """杀进程前核对 /proc，避免误杀复用的 pid。"""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            cmdline = f.read().decode(errors="replace")
    except FileNotFoundError:
        raise HTTPException(409, f"pid {pid} 已不存在")
    if "app.worker" not in cmdline or worker_id not in cmdline:
        raise HTTPException(409, f"pid {pid} 不是 {worker_id} 的工作进程，拒绝操作")


def _fmt_event(r):
    return {
        "id": r["id"],
        "ts": r["ts"],
        "kind": r["kind"],
        "partition": r["partition"],
        "worker_id": r["worker_id"],
        "message": r["message"],
    }


# ---------------------------------------------------------------- 状态


@app.get("/api/state")
def get_state():
    conn = connect()
    try:
        t = now()
        workers = []
        for w in conn.execute("SELECT * FROM workers ORDER BY started_at").fetchall():
            parts = [
                r["partition"]
                for r in conn.execute(
                    "SELECT partition FROM assignments WHERE owner_id=? AND status='active' "
                    "ORDER BY partition",
                    (w["worker_id"],),
                ).fetchall()
            ]
            alive = w["state"] == "running" and w["last_heartbeat"] > t - WORKER_DEAD_AFTER
            workers.append(
                {
                    "worker_id": w["worker_id"],
                    "pid": w["pid"],
                    "state": w["state"],
                    "alive": alive,
                    "hb_age": round(t - w["last_heartbeat"], 1) if w["last_heartbeat"] else None,
                    "fault_mode": w["fault_mode"],
                    "partitions": parts,
                }
            )

        partitions = []
        for p in range(NUM_PARTITIONS):
            a = conn.execute(
                "SELECT * FROM assignments WHERE partition=?", (p,)
            ).fetchone()
            pos = conn.execute(
                "SELECT next_offset FROM positions WHERE partition=?", (p,)
            ).fetchone()["next_offset"]
            (total,) = conn.execute(
                "SELECT COUNT(*) FROM messages WHERE partition=?", (p,)
            ).fetchone()
            last = conn.execute(
                "SELECT * FROM ledger_entries WHERE partition=? ORDER BY id DESC LIMIT 1",
                (p,),
            ).fetchone()
            conflict = conn.execute(
                "SELECT * FROM conflicts WHERE partition=?", (p,)
            ).fetchone()
            partitions.append(
                {
                    "partition": p,
                    "status": a["status"],
                    "owner_id": a["owner_id"],
                    "epoch": a["epoch"],
                    "lease_in": round(max(0.0, a["lease_expires_at"] - t), 1)
                    if a["lease_expires_at"]
                    else None,
                    "next_offset": pos,
                    "total_messages": total,
                    "last_entry": dict(last) if last else None,
                    "conflict": dict(conflict) if conflict else None,
                }
            )

        ledger = [
            dict(r)
            for r in conn.execute("SELECT * FROM ledger ORDER BY account").fetchall()
        ]
        entries = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM ledger_entries ORDER BY id DESC LIMIT 12"
            ).fetchall()
        ]
        events = [
            _fmt_event(r)
            for r in conn.execute("SELECT * FROM events ORDER BY id DESC LIMIT 60").fetchall()
        ]
        auto = conn.execute("SELECT value FROM meta WHERE key='autoproduce'").fetchone()
        return {
            "now": t,
            "lease_ttl": LEASE_TTL,
            "workers": workers,
            "partitions": partitions,
            "ledger": ledger,
            "entries": entries,
            "events": events,
            "autoproduce": bool(auto and auto["value"] == "on"),
            "accounts": ACCOUNTS,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------- 进程管理


class WorkerReq(BaseModel):
    worker_id: str | None = None


@app.post("/api/workers")
def start_worker(req: WorkerReq):
    conn = connect()
    try:
        with Tx(conn):
            if req.worker_id:
                wid = req.worker_id
            else:
                existing = {
                    r["worker_id"]
                    for r in conn.execute("SELECT worker_id FROM workers").fetchall()
                }
                wid = next((f"w{i}" for i in range(1, 10) if f"w{i}" not in existing), None)
                if wid is None:
                    raise HTTPException(400, "工作进程数量已达上限")
            row = conn.execute(
                "SELECT * FROM workers WHERE worker_id=?", (wid,)
            ).fetchone()
            if row:
                fresh = (
                    row["state"] == "running"
                    and row["last_heartbeat"]
                    and row["last_heartbeat"] > now() - WORKER_DEAD_AFTER
                )
                starting = row["state"] == "starting" and row["started_at"] and row["started_at"] > now() - 15
                if fresh or starting:
                    raise HTTPException(409, f"{wid} 已在运行或正在启动")
            # 先在库中占位，避免并发启动同一 id
            conn.execute(
                """INSERT INTO workers(worker_id, pid, started_at, last_heartbeat, state, fault_mode)
                   VALUES (?, NULL, ?, 0, 'starting', '')
                   ON CONFLICT(worker_id) DO UPDATE SET
                     pid=NULL, started_at=excluded.started_at, last_heartbeat=0,
                     state='starting', fault_mode=''""",
                (wid, now()),
            )
    finally:
        conn.close()

    os.makedirs(WORKER_LOG_DIR, exist_ok=True)
    log_path = os.path.join(WORKER_LOG_DIR, f"worker-{wid}.log")
    with open(log_path, "ab") as logf:
        subprocess.Popen(
            [sys.executable, "-m", "app.worker", "--id", wid],
            cwd=ROOT,
            stdout=logf,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    return {"ok": True, "worker_id": wid}


@app.post("/api/workers/{wid}/stop")
def stop_worker(wid: str):
    pid = _pid_of_worker(wid)
    _check_worker_pid(wid, pid)
    os.kill(pid, signal.SIGTERM)  # 优雅退出：进程自行释放分区
    return {"ok": True}


@app.post("/api/workers/{wid}/kill")
def kill_worker(wid: str):
    pid = _pid_of_worker(wid)
    _check_worker_pid(wid, pid)
    os.kill(pid, signal.SIGKILL)  # 模拟崩溃：只能等租约到期回收
    conn = connect()
    try:
        log_event(conn, "fault", f"{wid} 被强制结束（SIGKILL），分区等待租约到期", worker_id=wid)
    finally:
        conn.close()
    return {"ok": True}


# ---------------------------------------------------------------- 故障注入


class FaultReq(BaseModel):
    mode: str


@app.post("/api/workers/{wid}/fault")
def arm_fault(wid: str, req: FaultReq):
    if req.mode not in FAULT_MODES:
        raise HTTPException(400, f"未知故障模式：{req.mode}")
    conn = connect()
    try:
        row = conn.execute(
            "SELECT worker_id FROM workers WHERE worker_id=?", (wid,)
        ).fetchone()
        if row is None:
            raise HTTPException(404, f"工作进程 {wid} 不存在")
        conn.execute(
            "UPDATE workers SET fault_mode=? WHERE worker_id=?", (req.mode, wid)
        )
        label = {
            "crash_before_commit": "事务提交前退出",
            "crash_after_commit": "提交后、确认前退出",
            "zombie": "旧任期迟到写入",
        }[req.mode]
        log_event(conn, "fault", f"已向 {wid} 注入故障：{label}（下一条消息触发）", worker_id=wid)
    finally:
        conn.close()
    return {"ok": True}


# ---------------------------------------------------------------- 演示操作


class TransferReq(BaseModel):
    target: str


@app.post("/api/partitions/{p}/transfer")
def transfer_partition(p: int, req: TransferReq):
    conn = connect()
    try:
        alive = {w["worker_id"] for w in coordinator.alive_workers(conn)}
        if req.target not in alive:
            raise HTTPException(400, f"目标进程 {req.target} 不在存活状态")
        try:
            old, epoch = coordinator.force_transfer(conn, p, req.target)
        except ValueError as e:
            raise HTTPException(400, str(e))
    finally:
        conn.close()
    return {"ok": True, "from": old, "to": req.target, "epoch": epoch}


@app.post("/api/partitions/{p}/replay")
def replay_partition(p: int):
    """模拟重复投递：位置回退一格，下一条消息将再次送达（内容不变）。"""
    conn = connect()
    try:
        with Tx(conn):
            pos = conn.execute(
                "SELECT next_offset FROM positions WHERE partition=?", (p,)
            ).fetchone()["next_offset"]
            if pos == 0:
                raise HTTPException(400, "该分区还没有已处理的消息，无法重放")
            conn.execute(
                "UPDATE positions SET next_offset=next_offset-1 WHERE partition=?", (p,)
            )
            log_event(
                conn, "fault",
                f"模拟重复投递：分区 {p} 位置回退到 offset {pos - 1}，消息将再次送达",
                partition=p,
            )
    finally:
        conn.close()
    return {"ok": True}


@app.post("/api/partitions/{p}/tamper")
def tamper_partition(p: int):
    """模拟重复投递且内容被篡改：修改上一条已处理消息的内容并回退位置。"""
    conn = connect()
    try:
        with Tx(conn):
            pos = conn.execute(
                "SELECT next_offset FROM positions WHERE partition=?", (p,)
            ).fetchone()["next_offset"]
            if pos == 0:
                raise HTTPException(400, "该分区还没有已处理的消息，无法篡改重放")
            off = pos - 1
            msg = conn.execute(
                "SELECT * FROM messages WHERE partition=? AND offset=?", (p, off)
            ).fetchone()
            if msg is None:
                raise HTTPException(400, "找不到对应消息")
            new_amount = msg["amount"] + 100000  # 篡改金额 +1000.00
            import hashlib, json as _json

            payload = _json.dumps(
                {"order_id": msg["msg_id"], "account": msg["account"], "amount": new_amount},
                ensure_ascii=False,
            )
            new_hash = hashlib.sha256(
                f"{msg['msg_id']}|{msg['account']}|{new_amount}|{payload}".encode()
            ).hexdigest()[:16]
            conn.execute(
                "UPDATE messages SET amount=?, payload=?, content_hash=? "
                "WHERE partition=? AND offset=?",
                (new_amount, payload, new_hash, p, off),
            )
            conn.execute(
                "UPDATE positions SET next_offset=? WHERE partition=?", (off, p)
            )
            log_event(
                conn, "fault",
                f"模拟篡改重放：分区 {p} offset {off} 金额被改动后再次送达",
                partition=p,
            )
    finally:
        conn.close()
    return {"ok": True}


@app.post("/api/partitions/{p}/resolve")
def resolve_conflict(p: int):
    """人工处理冲突：跳过该消息（不入账），分区恢复活跃。"""
    conn = connect()
    try:
        with Tx(conn):
            c = conn.execute(
                "SELECT * FROM conflicts WHERE partition=?", (p,)
            ).fetchone()
            if c is None:
                raise HTTPException(400, "该分区没有待处理的冲突")
            conn.execute(
                "UPDATE positions SET next_offset=? WHERE partition=?", (c["offset"] + 1, p)
            )
            conn.execute("DELETE FROM conflicts WHERE partition=?", (p,))
            conn.execute(
                "UPDATE assignments SET status='active' WHERE partition=?", (p,)
            )
            log_event(
                conn, "resolve",
                f"分区 {p} 冲突已人工处理：跳过 offset {c['offset']}（不入账），分区恢复活跃",
                partition=p,
            )
    finally:
        conn.close()
    return {"ok": True}


class ProduceReq(BaseModel):
    count: int = 20


@app.post("/api/produce")
def produce_messages(req: ProduceReq):
    conn = connect()
    try:
        made = coordinator.produce(conn, max(1, min(500, req.count)))
        log_event(conn, "produce", f"生产了 {len(made)} 条新消息")
    finally:
        conn.close()
    return {"ok": True, "count": len(made)}


class ToggleReq(BaseModel):
    on: bool


@app.post("/api/autoproduce")
def toggle_autoproduce(req: ToggleReq):
    conn = connect()
    try:
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('autoproduce', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("on" if req.on else "off",),
        )
        log_event(conn, "produce", "自动生产已" + ("开启" if req.on else "关闭"))
    finally:
        conn.close()
    return {"ok": True}


@app.post("/api/reset")
def reset_demo():
    # 先杀掉所有工作进程
    conn = connect()
    try:
        rows = conn.execute("SELECT worker_id, pid FROM workers WHERE pid IS NOT NULL").fetchall()
    finally:
        conn.close()
    for r in rows:
        try:
            _check_worker_pid(r["worker_id"], r["pid"])
            os.kill(r["pid"], signal.SIGKILL)
        except Exception:
            pass
    time.sleep(0.5)

    conn = connect()
    try:
        with Tx(conn):
            for table in (
                "messages", "assignments", "positions", "dedup", "ledger",
                "ledger_entries", "workers", "conflicts", "events", "meta",
            ):
                conn.execute(f"DELETE FROM {table}")
            for p in range(NUM_PARTITIONS):
                conn.execute("INSERT INTO assignments(partition) VALUES (?)", (p,))
                conn.execute("INSERT INTO positions(partition) VALUES (?)", (p,))
        coordinator.seed_messages(conn, per_partition=8)
        log_event(conn, "reset", "演示已重置：6 个分区，每分区 8 条消息")
    finally:
        conn.close()
    return {"ok": True}


# ---------------------------------------------------------------- 一致性核对


@app.get("/api/verify")
def verify():
    conn = connect()
    try:
        checks = []

        # 1. 去重记录与入账流水一一对应（每条消息恰好入账一次）
        (orphan_entries,) = conn.execute(
            """SELECT COUNT(*) FROM ledger_entries le
               LEFT JOIN dedup d ON d.partition=le.partition AND d.offset=le.offset
               WHERE d.partition IS NULL"""
        ).fetchone()
        (orphan_dedup,) = conn.execute(
            """SELECT COUNT(*) FROM dedup d
               LEFT JOIN ledger_entries le ON d.partition=le.partition AND d.offset=le.offset
               WHERE le.partition IS NULL"""
        ).fetchone()
        checks.append({
            "name": "每条已处理消息恰好入账一次（去重记录 ↔ 入账流水一一对应）",
            "ok": orphan_entries == 0 and orphan_dedup == 0,
            "detail": f"无对应去重记录的流水 {orphan_entries} 条，无对应流水的去重记录 {orphan_dedup} 条",
        })

        # 2. 同一 offset 不会被入账两次
        dup = conn.execute(
            "SELECT partition, offset, COUNT(*) c FROM ledger_entries GROUP BY partition, offset HAVING c > 1"
        ).fetchall()
        checks.append({
            "name": "同一 (分区, offset) 不存在重复入账",
            "ok": len(dup) == 0,
            "detail": "无重复" if not dup else f"发现 {len(dup)} 处重复",
        })

        # 3. 账户余额 == 入账流水合计
        mism1 = conn.execute(
            """SELECT l.account a, l.balance, COALESCE(s.total,0) t FROM ledger l
               LEFT JOIN (SELECT account, SUM(amount) total FROM ledger_entries GROUP BY account) s
               ON s.account=l.account WHERE l.balance != COALESCE(s.total,0)"""
        ).fetchall()
        mism2 = conn.execute(
            """SELECT s.account a FROM (SELECT account, SUM(amount) total FROM ledger_entries GROUP BY account) s
               LEFT JOIN ledger l ON l.account=s.account WHERE l.account IS NULL"""
        ).fetchall()
        checks.append({
            "name": "账户余额与入账流水合计一致",
            "ok": len(mism1) == 0 and len(mism2) == 0,
            "detail": "全部一致"
            if not mism1 and not mism2
            else "不一致账户：" + ", ".join([r["a"] for r in mism1] + [r["a"] for r in mism2]),
        })

        # 4. 活跃分区的消费位置连续且与去重记录吻合（按 offset 递增处理）
        cont_bad = []
        for p in range(NUM_PARTITIONS):
            a = conn.execute(
                "SELECT status FROM assignments WHERE partition=?", (p,)
            ).fetchone()
            if a["status"] != "active":
                continue
            pos = conn.execute(
                "SELECT next_offset FROM positions WHERE partition=?", (p,)
            ).fetchone()["next_offset"]
            agg = conn.execute(
                "SELECT COUNT(*) c, COALESCE(MIN(offset),0) mn, COALESCE(MAX(offset),-1) mx "
                "FROM dedup WHERE partition=?",
                (p,),
            ).fetchone()
            if not (agg["c"] == pos and (pos == 0 or (agg["mn"] == 0 and agg["mx"] == pos - 1))):
                cont_bad.append(p)
        checks.append({
            "name": "各活跃分区按 offset 顺序连续消费（位置与去重记录吻合）",
            "ok": len(cont_bad) == 0,
            "detail": "全部连续" if not cont_bad else f"分区 {cont_bad} 不连续",
        })

        # 5. 每个分区的入账顺序严格递增
        order_bad = conn.execute(
            """SELECT DISTINCT partition FROM (
                 SELECT partition, offset,
                        LAG(offset) OVER (PARTITION BY partition ORDER BY id) prev
                 FROM ledger_entries
               ) WHERE prev IS NOT NULL AND offset <= prev"""
        ).fetchall()
        checks.append({
            "name": "每个分区内的入账顺序严格按 offset 递增",
            "ok": len(order_bad) == 0,
            "detail": "顺序正确" if not order_bad else f"分区 {[r['partition'] for r in order_bad]} 顺序异常",
        })

        # 6. 已停止分区都有冲突记录
        stopped_no_conflict = conn.execute(
            """SELECT a.partition FROM assignments a
               LEFT JOIN conflicts c ON c.partition=a.partition
               WHERE a.status='stopped' AND c.partition IS NULL"""
        ).fetchall()
        checks.append({
            "name": "停止的分区均有冲突记录可查",
            "ok": len(stopped_no_conflict) == 0,
            "detail": "符合" if not stopped_no_conflict else f"分区 {[r['partition'] for r in stopped_no_conflict]} 缺冲突记录",
        })

        return {"ok": all(c["ok"] for c in checks), "checks": checks}
    finally:
        conn.close()


# ---------------------------------------------------------------- 前端

_dist = os.path.join(ROOT, "frontend", "dist")
if os.path.isdir(_dist):
    app.mount("/assets", StaticFiles(directory=os.path.join(_dist, "assets")), name="assets")

    @app.get("/{full_path:path}")
    def spa(full_path: str):
        return FileResponse(os.path.join(_dist, "index.html"))
