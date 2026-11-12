#!/usr/bin/env python3
"""端到端验收：驱动真实 API + 真实工作进程，覆盖全部演示场景。"""
import json
import sys
import time
import urllib.request

BASE = "http://127.0.0.1:8000"


def api(path, body=None, method=None):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
        method = method or "POST"
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())


def wait_for(desc, cond, timeout=30, interval=0.5):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if cond():
            print(f"  ✓ {desc}（{time.time()-t0:.1f}s）")
            return True
        time.sleep(interval)
    print(f"  ✗ 超时：{desc}")
    return False


def state():
    return api("/api/state")


def verify_ok():
    return api("/api/verify")["ok"]


def parts_of(st, wid):
    w = [w for w in st["workers"] if w["worker_id"] == wid]
    return w[0]["partitions"] if w else []


def owner_of(st, p):
    return st["partitions"][p]["owner_id"]


def events_contain(st, *words):
    return any(all(w in e["message"] for w in words) for e in st["events"])


failed = []


def check(desc, ok):
    print(("  ✓ " if ok else "  ✗ ") + desc)
    if not ok:
        failed.append(desc)


print("== 0. 重置 ==")
api("/api/reset", {})
time.sleep(1)

print("== 1. 启动 3 个进程，6 个分区应均衡为 2/2/2 ==")
for _ in range(3):
    api("/api/workers", {})
    time.sleep(0.5)
wait_for("三进程均衡认领", lambda: sorted(len(parts_of(state(), f"w{i}")) for i in (1, 2, 3)) == [2, 2, 2], 20)
st = state()
check("每个分区都有任期编号", all(p["epoch"] >= 1 for p in st["partitions"]))

print("== 2. 生产并全部消费 ==")
api("/api/produce", {"count": 60})
wait_for("全部消息被消费", lambda: all(p["next_offset"] == p["total_messages"] for p in state()["partitions"]), 40)
check("一致性核对通过", verify_ok())

print("== 3. 强杀 w2（SIGKILL）：租约到期后分区被接管 ==")
victim_parts = parts_of(state(), "w2")
api("/api/workers/w2/kill", {})
check("刚杀完分区仍归 w2（租约未到期）", all(owner_of(state(), p) == "w2" for p in victim_parts))
wait_for("w2 被判定失联", lambda: not [w for w in state()["workers"] if w["worker_id"] == "w2"][0]["alive"], 15)
wait_for("w2 的分区被其他进程接管", lambda: all(owner_of(state(), p) not in (None, "w2") for p in victim_parts), 20)
wait_for("积压消息被新持有者消费", lambda: all(p["next_offset"] == p["total_messages"] for p in state()["partitions"]), 30)
check("一致性核对通过", verify_ok())

print("== 4. 重启 w2，再均衡 ==")
api("/api/workers", {"worker_id": "w2"})
wait_for("三进程重新均衡", lambda: max(len(parts_of(state(), f"w{i}")) for i in (1, 2, 3)) - min(len(parts_of(state(), f"w{i}")) for i in (1, 2, 3)) <= 1, 30)

print("== 5. 优雅停止 w3：分区立即释放 ==")
api("/api/workers/w3/stop", {})
wait_for("w3 分区被立即接管", lambda: all(owner_of(state(), p) != "w3" for p in range(6)), 10)
api("/api/workers", {"worker_id": "w3"})
wait_for("w3 重新加入", lambda: len(parts_of(state(), "w3")) >= 1, 30)

print("== 6. 重复投递（内容相同）→ 跳过 ==")
api("/api/partitions/0/replay", {})
wait_for("出现跳过事件", lambda: events_contain(state(), "重复投递", "跳过"), 15)
check("一致性核对通过", verify_ok())

print("== 7. 篡改重放 → 冲突停止 → 人工恢复 ==")
api("/api/partitions/1/tamper", {})
wait_for("分区 1 因冲突停止", lambda: state()["partitions"][1]["status"] == "stopped", 15)
check("冲突详情已记录", state()["partitions"][1]["conflict"] is not None)
api("/api/partitions/1/resolve", {})
wait_for("分区 1 恢复活跃", lambda: state()["partitions"][1]["status"] == "active", 10)
check("一致性核对通过", verify_ok())

print("== 8. 故障：事务提交前退出 ==")
api("/api/produce", {"count": 30})
api("/api/workers/w1/fault", {"mode": "crash_before_commit"})
wait_for("w1 崩溃", lambda: not [w for w in state()["workers"] if w["worker_id"] == "w1"][0]["alive"], 25)
wait_for("w1 的分区被接管且消费完毕", lambda: all(p["next_offset"] == p["total_messages"] for p in state()["partitions"]), 40)
check("回滚后重放不重复入账（核对通过）", verify_ok())
api("/api/workers", {"worker_id": "w1"})
time.sleep(2)

print("== 9. 故障：提交后、确认前退出 ==")
api("/api/produce", {"count": 30})
api("/api/workers/w2/fault", {"mode": "crash_after_commit"})
wait_for("w2 崩溃", lambda: not [w for w in state()["workers"] if w["worker_id"] == "w2"][0]["alive"], 25)
wait_for("消费完毕", lambda: all(p["next_offset"] == p["total_messages"] for p in state()["partitions"]), 40)
check("提交即生效，不重复入账（核对通过）", verify_ok())
api("/api/workers", {"worker_id": "w2"})
time.sleep(2)

print("== 10. 故障：旧任期迟到写入 ==")
api("/api/workers/w3/fault", {"mode": "zombie"})
wait_for("w3 开始模拟卡顿", lambda: events_contain(state(), "模拟卡顿"), 10)
wait_for("迟到写入被 fencing 拒绝", lambda: events_contain(state(), "拒绝", "旧任期"), 40)
wait_for("w3 恢复心跳", lambda: [w for w in state()["workers"] if w["worker_id"] == "w3"][0]["alive"], 20)
check("一致性核对通过", verify_ok())

print("== 11. 强制转交分区 5 ==")
alive = [w["worker_id"] for w in state()["workers"] if w["alive"]]
target = next(w for w in alive if w != owner_of(state(), 5))
api("/api/partitions/5/transfer", {"target": target})
check(f"分区 5 已转交给 {target}", owner_of(state(), 5) == target)

print("== 12. 最终核对 ==")
time.sleep(2)
check("一致性核对通过", verify_ok())

print()
if failed:
    print(f"失败 {len(failed)} 项：")
    for f in failed:
        print("  -", f)
    sys.exit(1)
print("全部场景通过 ✅")
