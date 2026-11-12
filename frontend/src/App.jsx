import React, { useEffect, useMemo, useState } from 'react'

// 分类色（固定顺序，跟随进程身份，不随状态变化）
const WORKER_COLORS = ['#3987e5', '#d95926', '#199e70', '#c98500', '#d55181', '#008300', '#9085e9', '#e66767']
// 状态色（专用，不与分类色混用）
const ST = { good: '#0ca30c', warning: '#fab219', serious: '#ec835a', critical: '#d03b3b' }

const FAULT_LABEL = {
  crash_before_commit: '事务提交前退出',
  crash_after_commit: '提交后、确认前退出',
  zombie: '旧任期迟到写入',
}

const EVENT_KIND = {
  assign: ['分配', ST.good],
  revoke: ['撤销', ST.serious],
  release: ['释放', ST.serious],
  crash: ['失联', ST.critical],
  reject: ['拒绝', ST.critical],
  conflict: ['冲突', ST.critical],
  fault: ['故障', ST.warning],
  skip: ['跳过', ST.warning],
  process: ['入账', 'var(--text-muted)'],
  produce: ['生产', 'var(--text-muted)'],
  resolve: ['恢复', ST.good],
  worker: ['进程', 'var(--text-muted)'],
  reset: ['重置', ST.warning],
  error: ['错误', ST.critical],
}

const fmt = (cents) => '¥' + (cents / 100).toFixed(2)
const fmtTime = (ts) => new Date(ts * 1000).toLocaleTimeString('zh-CN', { hour12: false })

async function api(path, body) {
  const opt = body === undefined ? {} : {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }
  const r = await fetch(path, opt)
  const d = await r.json().catch(() => ({}))
  if (!r.ok) throw new Error(d.detail || r.statusText)
  return d
}

export default function App() {
  const [state, setState] = useState(null)
  const [verify, setVerify] = useState(null)
  const [error, setError] = useState(null)

  const refresh = async () => {
    try {
      setState(await api('/api/state'))
    } catch (e) { setError(e.message) }
  }
  const runVerify = async () => {
    try { setVerify(await api('/api/verify')) } catch (e) { setError(e.message) }
  }
  const act = async (path, body) => {
    setError(null)
    try { await api(path, body ?? {}); await refresh() } catch (e) { setError(e.message) }
  }

  useEffect(() => {
    refresh(); runVerify()
    const t1 = setInterval(refresh, 1000)
    const t2 = setInterval(runVerify, 5000)
    return () => { clearInterval(t1); clearInterval(t2) }
  }, [])

  const workerColor = useMemo(() => {
    const m = {}
    ;(state?.workers ?? []).forEach((w, i) => { m[w.worker_id] = WORKER_COLORS[i % WORKER_COLORS.length] })
    return m
  }, [state?.workers])

  if (!state) return <div className="loading">正在连接本地演示服务…</div>

  const aliveWorkers = state.workers.filter((w) => w.alive)
  const totalBalance = state.ledger.reduce((s, l) => s + l.balance, 0)

  return (
    <div className="page">
      <header className="topbar">
        <div>
          <h1>分区消费演示</h1>
          <p className="sub">本地消息代理 · 6 个分区 · 租约 + 任期 fencing · 单库事务（入账 / 去重 / 位置）</p>
        </div>
        <div className="top-actions">
          <button onClick={() => act('/api/produce', { count: 20 })}>生产 20 条</button>
          <button
            className={state.autoproduce ? 'toggle on' : 'toggle'}
            onClick={() => act('/api/autoproduce', { on: !state.autoproduce })}
          >
            自动生产 {state.autoproduce ? '开' : '关'}
          </button>
          <button onClick={runVerify}>核对一致性</button>
          <button
            className="danger"
            onClick={() => { if (confirm('确定重置？将结束所有工作进程并重新播种。')) act('/api/reset') }}
          >重置演示</button>
        </div>
      </header>

      {error && <div className="errorbar" onClick={() => setError(null)}>操作失败：{error}（点击关闭）</div>}

      <section className="panel">
        <div className="panel-head">
          <h2>工作进程</h2>
          <button className="small" onClick={() => act('/api/workers')}>＋ 启动 Worker</button>
        </div>
        <div className="workers">
          {state.workers.length === 0 && <div className="hint">还没有工作进程，点击「＋ 启动 Worker」。</div>}
          {state.workers.map((w) => (
            <WorkerCard key={w.worker_id} w={w} color={workerColor[w.worker_id]} act={act} />
          ))}
        </div>
      </section>

      <section className="panel">
        <div className="panel-head"><h2>分区（归属 / 任期 / 租约 / 消费位置）</h2></div>
        <div className="partitions">
          {state.partitions.map((p) => (
            <PartitionCard key={p.partition} p={p} color={workerColor[p.owner_id]} aliveWorkers={aliveWorkers} act={act} />
          ))}
        </div>
      </section>

      <div className="bottom-grid">
        <section className="panel">
          <div className="panel-head"><h2>账户账本</h2><span className="hint">合计 {fmt(totalBalance)}</span></div>
          <table className="ledger">
            <thead><tr><th>账户</th><th className="num">余额</th><th></th></tr></thead>
            <tbody>
              {state.ledger.length === 0 && <tr><td colSpan="3" className="hint">暂无入账</td></tr>}
              {state.ledger.map((l) => {
                const max = Math.max(...state.ledger.map((x) => x.balance), 1)
                return (
                  <tr key={l.account}>
                    <td>{l.account}</td>
                    <td className="num">{fmt(l.balance)}</td>
                    <td className="barcell"><div className="bar" style={{ width: `${(l.balance / max) * 100}%` }} /></td>
                  </tr>
                )
              })}
            </tbody>
          </table>
          <h3>最近入账</h3>
          <table className="entries">
            <thead><tr><th>分区/offset</th><th>账户</th><th className="num">金额</th><th>进程</th><th>任期</th></tr></thead>
            <tbody>
              {state.entries.map((e) => (
                <tr key={e.id}>
                  <td>P{e.partition} #{e.offset}</td>
                  <td>{e.account}</td>
                  <td className="num">{fmt(e.amount)}</td>
                  <td><span className="dot" style={{ background: workerColor[e.worker_id] }} />{e.worker_id}</td>
                  <td>{e.epoch}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>

        <section className="panel">
          <div className="panel-head"><h2>一致性核对</h2><span className="hint">每 5 秒自动核对</span></div>
          {verify && (
            <ul className="checks">
              <li className={verify.ok ? 'check ok' : 'check bad'}>
                <b>{verify.ok ? '✓ 全部通过' : '✗ 存在异常'}</b>
              </li>
              {verify.checks.map((c, i) => (
                <li key={i} className={c.ok ? 'check ok' : 'check bad'}>
                  {c.ok ? '✓' : '✗'} {c.name}
                  <span className="detail">{c.detail}</span>
                </li>
              ))}
            </ul>
          )}
        </section>

        <section className="panel events-panel">
          <div className="panel-head"><h2>事件流</h2></div>
          <ul className="events">
            {state.events.map((e) => {
              const [label, color] = EVENT_KIND[e.kind] || [e.kind, 'var(--text-muted)']
              return (
                <li key={e.id}>
                  <span className="ts">{fmtTime(e.ts)}</span>
                  <span className="kind" style={{ color, borderColor: color }}>{label}</span>
                  <span className="msg">{e.message}</span>
                </li>
              )
            })}
          </ul>
        </section>
      </div>
    </div>
  )
}

function WorkerCard({ w, color, act }) {
  const stateInfo = !w.alive && w.state === 'running'
    ? ['心跳异常', ST.warning]
    : w.state === 'running'
      ? ['运行中', ST.good]
      : w.state === 'starting'
        ? ['启动中', ST.warning]
        : w.state === 'stopped'
          ? ['已停止', 'var(--text-muted)']
          : ['已失联', ST.critical]
  return (
    <div className="worker" style={{ borderTopColor: color }}>
      <div className="worker-head">
        <span className="dot big" style={{ background: color }} />
        <b>{w.worker_id}</b>
        <span className="badge" style={{ color: stateInfo[1], borderColor: stateInfo[1] }}>{stateInfo[0]}</span>
      </div>
      <div className="worker-meta">
        <span>pid {w.pid ?? '—'}</span>
        <span>心跳 {w.hb_age == null ? '—' : w.hb_age + 's 前'}</span>
      </div>
      <div className="worker-parts">
        分区：{w.partitions.length ? w.partitions.map((p) => <span key={p} className="chip">P{p}</span>) : '无'}
      </div>
      {w.fault_mode && <div className="fault-armed">故障已注入：{FAULT_LABEL[w.fault_mode]}</div>}
      <div className="worker-actions">
        {w.alive ? (
          <>
            <button className="small" onClick={() => act(`/api/workers/${w.worker_id}/stop`)}>优雅停止</button>
            <button className="small warn" onClick={() => act(`/api/workers/${w.worker_id}/kill`)}>强制崩溃</button>
          </>
        ) : (
          <button className="small" onClick={() => act('/api/workers', { worker_id: w.worker_id })}>重启</button>
        )}
      </div>
      {w.alive && (
        <div className="worker-actions faults">
          <span className="hint">注入：</span>
          <button className="tiny" onClick={() => act(`/api/workers/${w.worker_id}/fault`, { mode: 'crash_before_commit' })}>提交前退出</button>
          <button className="tiny" onClick={() => act(`/api/workers/${w.worker_id}/fault`, { mode: 'crash_after_commit' })}>提交后退出</button>
          <button className="tiny" onClick={() => act(`/api/workers/${w.worker_id}/fault`, { mode: 'zombie' })}>僵尸迟到</button>
        </div>
      )}
    </div>
  )
}

function PartitionCard({ p, color, aliveWorkers, act }) {
  const [target, setTarget] = useState('')
  const statusInfo = p.status === 'stopped'
    ? ['已停止', ST.critical]
    : p.owner_id
      ? ['活跃', ST.good]
      : ['空闲', ST.warning]
  const pct = p.total_messages ? Math.round((p.next_offset / p.total_messages) * 100) : 0
  const others = aliveWorkers.filter((w) => w.worker_id !== p.owner_id)

  return (
    <div className={p.status === 'stopped' ? 'partition stopped' : 'partition'}>
      <div className="part-head">
        <b>分区 P{p.partition}</b>
        <span className="badge" style={{ color: statusInfo[1], borderColor: statusInfo[1] }}>{statusInfo[0]}</span>
      </div>
      <div className="part-owner">
        {p.owner_id ? (
          <>
            <span className="dot big" style={{ background: color }} />
            <b>{p.owner_id}</b>
            <span className="hint">任期 {p.epoch}</span>
            <span className="hint">租约 {p.lease_in == null ? '—' : p.lease_in + 's'}</span>
          </>
        ) : (
          <span className="hint">无持有者（任期 {p.epoch}）</span>
        )}
      </div>
      <div className="progress">
        <div className="progress-fill" style={{ width: pct + '%', background: color || 'var(--text-muted)' }} />
      </div>
      <div className="part-meta">
        <span>下一条 offset <b>{p.next_offset}</b> / 共 {p.total_messages} 条</span>
        <span>{pct}%</span>
      </div>
      {p.last_entry && (
        <div className="part-last">
          最近入账：{p.last_entry.account} {fmt(p.last_entry.amount)}（#{p.last_entry.offset} · {p.last_entry.worker_id}）
        </div>
      )}
      {p.conflict && (
        <div className="conflict">
          <b>内容冲突 @ offset {p.conflict.offset}</b>
          <div>{p.conflict.detail}</div>
          <button className="small" onClick={() => act(`/api/partitions/${p.partition}/resolve`)}>跳过该消息并恢复</button>
        </div>
      )}
      <div className="part-actions">
        <select value={target} onChange={(e) => setTarget(e.target.value)}>
          <option value="">转交给…</option>
          {others.map((w) => <option key={w.worker_id} value={w.worker_id}>{w.worker_id}</option>)}
        </select>
        <button
          className="small"
          disabled={!target || p.status !== 'active'}
          onClick={() => act(`/api/partitions/${p.partition}/transfer`, { target })}
        >转交</button>
        <button className="tiny" disabled={p.status !== 'active'} onClick={() => act(`/api/partitions/${p.partition}/replay`)}>重放重复</button>
        <button className="tiny warn" disabled={p.status !== 'active'} onClick={() => act(`/api/partitions/${p.partition}/tamper`)}>篡改重放</button>
      </div>
    </div>
  )
}
