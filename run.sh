#!/usr/bin/env bash
# 一键启动本地演示：API + 前端（首次运行会安装依赖并构建前端）
set -e
cd "$(dirname "$0")"

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
.venv/bin/pip install -q -r requirements.txt

if [ ! -d frontend/dist ]; then
  echo "首次运行，构建前端…"
  (cd frontend && npm install && npm run build)
fi

# 初始化数据库并播种（幂等）
.venv/bin/python - <<'PY'
from app.db import init_db, connect
from app import coordinator
init_db()
conn = connect()
n = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
if n == 0:
    coordinator.seed_messages(conn, per_partition=8)
    coordinator.log_event(conn, "reset", "初始化：6 个分区，每分区 8 条消息")
conn.close()
PY

exec .venv/bin/python -m uvicorn app.api:app --host 127.0.0.1 --port "${PORT:-8000}"
