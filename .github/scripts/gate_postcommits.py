#!/usr/bin/env python3
"""Postcommits gate: sync runner labels from PR-check queue, write HTML report."""

from __future__ import annotations

import argparse
import html
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_CONFIG = {
    "ignore_pr_check_older_than_hours": 6,
    "limited_mode_when_pr_check_queue_above": 13,
    "reserved_postcommit_runners_in_limited_mode": 5,
}


def api_request(method: str, url: str, token: str, data: dict | None = None) -> Any:
    body = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            **({"Content-Type": "application/json"} if data is not None else {}),
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read().decode()
        return json.loads(raw) if raw else {}


def fmt_wait(seconds: float) -> str:
    if seconds >= 86400:
        return f"{seconds / 86400:.1f}d"
    if seconds >= 3600:
        return f"{seconds / 3600:.1f}h"
    return f"{int(seconds // 60)}m"


def esc(value: Any) -> str:
    return html.escape(str(value))


def load_config(config_raw: str, legacy_threshold: str) -> tuple[str, dict[str, Any]]:
    if not config_raw.strip():
        config_raw = json.dumps(DEFAULT_CONFIG, separators=(",", ":"))
    config = json.loads(config_raw)

    ignore_hours = int(config.get("ignore_pr_check_older_than_hours", config.get("max_wait_hours", 6)))
    limited_when_above = config.get("limited_mode_when_pr_check_queue_above")
    if limited_when_above is None:
        limited_when_above = config.get("limit_gate_when_pr_check_queue_above", legacy_threshold or "13")
    limited_when_above = int(limited_when_above)
    reserved_in_limited = int(
        config.get("reserved_postcommit_runners_in_limited_mode")
        or config.get("reserved_postcommit_runners")
        or config.get("postcommit_runners_on_high_queue")
        or config.get("partial_slots", 5)
    )
    parsed = {
        "ignore_pr_check_older_than_hours": ignore_hours,
        "limited_mode_when_pr_check_queue_above": limited_when_above,
        "reserved_postcommit_runners_in_limited_mode": reserved_in_limited,
        "rules_summary": (
            f"Count PR-check queued (1m..{ignore_hours}h). "
            f"If count<={limited_when_above} → OPEN (postcommit on all ghrun). "
            f"If count>{limited_when_above} → LIMITED "
            f"(postcommit on {reserved_in_limited} runners only)."
        ),
    }
    return config_raw, parsed


def classify_queue(runs_payload: dict, ignore_hours: int, now_ts: float) -> dict[str, list[dict]]:
    max_age = ignore_hours * 3600
    all_runs = []
    for run in runs_payload.get("workflow_runs", []):
        updated = run["updated_at"].replace("Z", "+00:00")
        waiting = now_ts - datetime.fromisoformat(updated).timestamp()
        name = run.get("name", "Unknown")
        if name == "PR-check":
            if waiting <= 60:
                gate_status = "ignored_fresh"
            elif waiting > max_age:
                gate_status = "ignored_old"
            else:
                gate_status = "counted"
        else:
            gate_status = "not_counted"
        all_runs.append(
            {
                "id": run["id"],
                "name": name,
                "updated_at": run["updated_at"],
                "html_url": run["html_url"],
                "waiting_seconds": waiting,
                "waiting_human": fmt_wait(waiting),
                "gate_status": gate_status,
            }
        )
    all_runs.sort(key=lambda r: r["waiting_seconds"])
    return {
        "all": all_runs,
        "counted": [r for r in all_runs if r["gate_status"] == "counted"],
        "ignored_too_old": [r for r in all_runs if r["gate_status"] == "ignored_old"],
        "ignored_too_fresh": [r for r in all_runs if r["gate_status"] == "ignored_fresh"],
    }


def decide_mode(queue_size: int, cfg: dict[str, Any]) -> tuple[str, str]:
    threshold = cfg["limited_mode_when_pr_check_queue_above"]
    reserved = cfg["reserved_postcommit_runners_in_limited_mode"]
    if queue_size > threshold:
        return (
            "limited",
            f"PR-check queue ({queue_size}) > {threshold}. "
            f"LIMITED: postcommit on {reserved} runner(s) only.",
        )
    return (
        "open",
        f"PR-check queue ({queue_size}) <= {threshold}. "
        "OPEN: postcommit on all ghrun runners.",
    )


def ghrun_runners(runners_payload: dict) -> list[dict]:
    result = []
    for runner in runners_payload.get("runners", []):
        labels = [lb["name"] for lb in runner.get("labels", [])]
        if "ghrun" not in labels:
            continue
        result.append(
            {
                "id": runner["id"],
                "name": runner.get("name", ""),
                "labels": labels,
                "has_postcommit": "postcommit" in labels,
            }
        )
    return sorted(result, key=lambda r: r["id"])


def plan_runner_actions(ghrun: list[dict], gate_mode: str, reserved: int) -> tuple[list[str], list[dict]]:
    keep_ids = [str(r["id"]) for r in ghrun[:reserved]]
    actions = []
    for runner in ghrun:
        should = gate_mode == "open" or str(runner["id"]) in keep_ids
        had = runner["has_postcommit"]
        if should and not had:
            action = "add_postcommit"
        elif not should and had:
            action = "remove_postcommit"
        else:
            action = "no_change"
        actions.append(
            {
                **runner,
                "action": action,
                "should_have_postcommit": should,
                "had_postcommit": had,
            }
        )
    return keep_ids, actions


def apply_runner_actions(repo: str, token: str, actions: list[dict], dry_run: bool) -> None:
    base = f"https://api.github.com/repos/{repo}/actions/runners"
    for item in actions:
        runner_id = item["id"]
        name = item["name"]
        action = item["action"]
        if action == "no_change":
            print(f"Runner {runner_id} ({name}): no change")
            continue
        if action == "add_postcommit":
            print(f"Runner {runner_id} ({name}): add postcommit")
            if not dry_run:
                api_request("POST", f"{base}/{runner_id}/labels", token, {"labels": ["postcommit"]})
        elif action == "remove_postcommit":
            print(f"Runner {runner_id} ({name}): remove postcommit")
            if not dry_run:
                api_request("DELETE", f"{base}/{runner_id}/labels/postcommit", token)


def _action_stats(actions: list[dict]) -> dict[str, int]:
    stats = {"add_postcommit": 0, "remove_postcommit": 0, "no_change": 0}
    for item in actions:
        stats[item["action"]] = stats.get(item["action"], 0) + 1
    return stats


GATE_STATUS_LABELS = {
    "counted": ("counted", "считается"),
    "ignored_old": ("ignored-old", "игнор (zombie)"),
    "ignored_fresh": ("ignored-fresh", "игнор (≤60s)"),
    "not_counted": ("not-counted", "—"),
}


def _queue_table(runs: list[dict], empty_text: str) -> str:
    if not runs:
        return f'<p class="empty">{esc(empty_text)}</p>'

    by_wf: dict[str, list[dict]] = {}
    for run in runs:
        by_wf.setdefault(run["name"], []).append(run)
    for wf_runs in by_wf.values():
        wf_runs.sort(key=lambda r: r["waiting_seconds"])

    wf_names = sorted(by_wf.keys(), key=lambda name: (name != "PR-check", name.lower()))
    max_rows = max(len(by_wf[name]) for name in wf_names)

    header = "".join(
        f'<th><span class="wf-name">{esc(name)}</span>'
        f'<span class="wf-count">{len(by_wf[name])}</span></th>'
        for name in wf_names
    )

    body_rows = []
    for row_idx in range(max_rows):
        cells = []
        for name in wf_names:
            wf_runs = by_wf[name]
            if row_idx >= len(wf_runs):
                cells.append('<td class="empty-cell"></td>')
                continue
            run = wf_runs[row_idx]
            status_cls, status_label = GATE_STATUS_LABELS[run["gate_status"]]
            cells.append(
                f'<td class="{status_cls}">'
                f'<div class="cell-wait">{esc(run["waiting_human"])}</div>'
                f'<div class="cell-run"><a href="{esc(run["html_url"])}">{esc(run["id"])}</a></div>'
                f'<div class="cell-gate"><span class="pill {status_cls}">{esc(status_label)}</span></div>'
                f"</td>"
            )
        body_rows.append(f"<tr>{''.join(cells)}</tr>")

    return (
        '<table class="queue-matrix">'
        f"<thead><tr>{header}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody></table>"
    )


def _runner_changes_table(actions: list[dict]) -> str:
    changed = [item for item in actions if item["action"] != "no_change"]
    if not changed:
        return '<p class="empty">Label postcommit не меняли — все runner\'ы уже в нужном состоянии.</p>'
    rows = []
    for item in changed:
        if item["action"] == "add_postcommit":
            action_cls, action_label = "add", "+ postcommit"
        else:
            action_cls, action_label = "remove", "− postcommit"
        rows.append(
            f'<tr class="{action_cls}">'
            f'<td><strong>{esc(item["name"] or item["id"])}</strong></td>'
            f'<td>{esc(item["id"])}</td>'
            f'<td><span class="pill {action_cls}">{esc(action_label)}</span></td>'
            f"</tr>"
        )
    return (
        '<table class="queue-table">'
        "<thead><tr><th>Runner</th><th>ID</th><th>Действие</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table>"
    )


def build_html(meta: dict, queue: dict, actions: list[dict], dry_run: bool, runners_note: str) -> str:
    mode = meta["gate_mode"]
    threshold = meta["limited_mode_when_pr_check_queue_above"]
    queue_size = meta["queue_size"]
    reserved = meta["reserved_postcommit_runners_in_limited_mode"]
    ignore_h = meta["ignore_pr_check_older_than_hours"]
    workflow_url = f"https://github.com/{meta['repository']}/actions/runs/{meta['workflow_run']}"

    if mode == "open":
        mode_title = "Gate открыт"
        mode_hint = f"Очередь PR-check небольшая ({queue_size} ≤ {threshold}). Postcommit могут идти на всех runner'ах."
        mode_effect = f"Label <code>postcommit</code> на всех {meta['ghrun_count']} ghrun runner'ах."
    else:
        mode_title = "Gate ограничен"
        mode_hint = f"Очередь PR-check высокая ({queue_size} &gt; {threshold}). Postcommit только на части runner'ов."
        mode_effect = (
            f"Label <code>postcommit</code> только на {reserved} из {meta['ghrun_count']} ghrun runner'ах "
            f"(IDs: {esc(meta['keep_runner_ids'] or '—')})."
        )

    bar_pct = min(100, int(queue_size / max(threshold, 1) * 100)) if threshold else 0
    bar_over = queue_size > threshold
    stats = _action_stats(actions)
    dry_badge = '<span class="tag tag-dry">dry-run</span>' if dry_run else ""
    warn = f'<div class="alert">{esc(runners_note)}</div>' if runners_note else ""

    changed = stats["add_postcommit"] + stats["remove_postcommit"]
    flow_runners_text = (
        "Все ghrun runner'ы с label postcommit."
        if mode == "open"
        else f"Только {reserved} runner'ов с label postcommit."
    )
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Postcommits gate — {esc(mode.upper())}</title>
  <style>
    :root {{
      --bg: #f4f6f8;
      --surface: #fff;
      --text: #1a2332;
      --muted: #5f6b7a;
      --border: #dde3ea;
      --open: #1a7f37;
      --open-bg: #e6f4ea;
      --limited: #9a6700;
      --limited-bg: #fff8e1;
      --accent: #0969da;
      --add: #1a7f37;
      --remove: #cf222e;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0; font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg); color: var(--text); line-height: 1.5;
    }}
    .wrap {{ max-width: 980px; margin: 0 auto; padding: 24px 20px 48px; }}
    .hero {{
      background: var(--surface); border: 1px solid var(--border); border-radius: 16px;
      padding: 28px 28px 24px; margin-bottom: 20px;
      border-left: 6px solid {'var(--limited)' if bar_over else 'var(--open)'};
    }}
    .hero-top {{ display: flex; flex-wrap: wrap; gap: 12px; align-items: center; margin-bottom: 12px; }}
    h1 {{ margin: 0; font-size: 1.5rem; }}
    .status {{
      font-size: .85rem; font-weight: 700; letter-spacing: .04em;
      padding: 6px 12px; border-radius: 999px;
      background: {'var(--limited-bg)' if bar_over else 'var(--open-bg)'};
      color: {'var(--limited)' if bar_over else 'var(--open)'};
    }}
    .tag {{ font-size: .75rem; padding: 4px 8px; border-radius: 6px; background: #eaeef2; color: var(--muted); }}
    .tag-dry {{ background: #ddf4ff; color: #0550ae; }}
    .hero-lead {{ font-size: 1.05rem; margin: 0 0 8px; }}
    .hero-effect {{ color: var(--muted); margin: 0; }}
    .meta-line {{ margin-top: 16px; font-size: .85rem; color: var(--muted); }}
    .meta-line a {{ color: var(--accent); }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin-bottom: 20px; }}
    .metric {{
      background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 16px;
    }}
    .metric-val {{ font-size: 1.75rem; font-weight: 700; line-height: 1.1; }}
    .metric-label {{ font-size: .8rem; color: var(--muted); margin-top: 4px; }}
    .section {{
      background: var(--surface); border: 1px solid var(--border); border-radius: 12px;
      padding: 20px; margin-bottom: 16px;
    }}
    .section h2 {{ margin: 0 0 14px; font-size: 1.05rem; }}
    .flow {{ display: flex; flex-wrap: wrap; gap: 8px; align-items: stretch; }}
    .flow-step {{
      flex: 1 1 160px; background: #f8fafc; border: 1px solid var(--border); border-radius: 10px; padding: 14px;
    }}
    .flow-step.active {{ border-color: var(--accent); background: #eef6ff; }}
    .flow-num {{ font-size: .7rem; color: var(--muted); text-transform: uppercase; letter-spacing: .06em; }}
    .flow-title {{ font-weight: 600; margin-top: 4px; }}
    .flow-text {{ font-size: .85rem; color: var(--muted); margin-top: 6px; }}
    .flow-arrow {{ align-self: center; color: var(--muted); font-size: 1.2rem; padding: 0 2px; }}
    .bar-wrap {{ margin-top: 8px; }}
    .bar-labels {{ display: flex; justify-content: space-between; font-size: .8rem; color: var(--muted); margin-bottom: 6px; }}
    .bar {{
      height: 12px; background: #eaeef2; border-radius: 999px; overflow: hidden;
    }}
    .bar-fill {{
      height: 100%; width: {bar_pct}%;
      background: {'linear-gradient(90deg,#f59e0b,#d97706)' if bar_over else 'linear-gradient(90deg,#34d399,#059669)'};
      border-radius: 999px;
    }}
    .alert {{
      background: #fff8c5; border: 1px solid #d4a72c; border-radius: 10px; padding: 12px 14px; margin-bottom: 16px;
    }}
    .summary-row {{ display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 14px; }}
    .chip {{ font-size: .82rem; padding: 6px 10px; border-radius: 999px; background: #f1f5f9; }}
    .chip.add {{ background: #dcfce7; color: var(--add); }}
    .chip.remove {{ background: #fee2e2; color: var(--remove); }}
    .queue-table {{
      width: 100%; border-collapse: collapse; font-size: .88rem;
    }}
    .queue-table th {{
      text-align: left; padding: 10px 12px; background: #f8fafc;
      border-bottom: 1px solid var(--border); font-size: .78rem; color: var(--muted);
      text-transform: uppercase; letter-spacing: .04em;
    }}
    .queue-table td {{
      padding: 10px 12px; border-bottom: 1px solid var(--border); vertical-align: middle;
    }}
    .queue-table tr:last-child td {{ border-bottom: none; }}
    .queue-table tr.add {{ background: #f0fdf4; }}
    .queue-table tr.remove {{ background: #fef2f2; }}
    .queue-matrix {{
      width: 100%; border-collapse: separate; border-spacing: 0; font-size: .85rem;
      table-layout: fixed;
    }}
    .queue-matrix th {{
      text-align: left; padding: 12px 14px; background: #f8fafc;
      border: 1px solid var(--border); border-left: none; vertical-align: top;
    }}
    .queue-matrix th:first-child {{ border-left: 1px solid var(--border); border-radius: 10px 0 0 0; }}
    .queue-matrix th:last-child {{ border-radius: 0 10px 0 0; }}
    .wf-name {{ display: block; font-size: .9rem; font-weight: 600; color: var(--text); }}
    .wf-count {{
      display: inline-block; margin-top: 4px; font-size: .72rem; font-weight: 600;
      padding: 2px 8px; border-radius: 999px; background: #eef2f7; color: var(--muted);
    }}
    .queue-matrix td {{
      padding: 10px 14px; border: 1px solid var(--border); border-top: none; border-left: none;
      vertical-align: top;
    }}
    .queue-matrix td:first-child {{ border-left: 1px solid var(--border); }}
    .queue-matrix tr:last-child td:first-child {{ border-radius: 0 0 0 10px; }}
    .queue-matrix tr:last-child td:last-child {{ border-radius: 0 0 10px 0; }}
    .queue-matrix td.counted {{ background: #eef6ff; }}
    .queue-matrix td.ignored-old {{ background: #fffbeb; }}
    .queue-matrix td.empty-cell {{ background: #fafbfc; }}
    .cell-wait {{
      font-weight: 700; font-variant-numeric: tabular-nums; margin-bottom: 4px;
    }}
    .cell-run {{ margin-bottom: 6px; }}
    .cell-run a {{ color: var(--accent); text-decoration: none; }}
    .cell-run a:hover {{ text-decoration: underline; }}
    .cell-gate {{ margin-top: 2px; }}
    details {{ border: 1px solid var(--border); border-radius: 10px; padding: 0; overflow: hidden; margin-top: 10px; }}
    summary {{
      cursor: pointer; padding: 12px 14px; background: #f8fafc; font-weight: 600; list-style: none;
    }}
    summary::-webkit-details-marker {{ display: none; }}
    .details-body {{ padding: 12px 14px 14px; }}
    .pill {{ font-size: .75rem; padding: 2px 8px; border-radius: 999px; background: #eef2f7; color: var(--muted); }}
    .pill.counted {{ background: #dbeafe; color: #1e40af; }}
    .pill.ignored-old {{ background: #fef3c7; color: #92400e; }}
    .pill.ignored-fresh {{ background: #f1f5f9; color: var(--muted); }}
    .pill.add {{ background: #dcfce7; color: var(--add); }}
    .pill.remove {{ background: #fee2e2; color: var(--remove); }}
    .empty {{ color: var(--muted); font-size: .9rem; margin: 0; }}
    code {{ background: #eef2f7; padding: 2px 6px; border-radius: 4px; font-size: .85em; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div class="hero-top">
        <h1>Postcommits gate</h1>
        <span class="status">{esc(mode.upper())}</span>
        {dry_badge}
      </div>
      <p class="hero-lead">{mode_title}. {mode_hint}</p>
      <p class="hero-effect"><strong>Что сделали:</strong> {mode_effect}</p>
      <div class="meta-line">{esc(meta['check_time'])} · <a href="{esc(workflow_url)}">workflow run</a></div>
    </div>

    {warn}

    <div class="grid">
      <div class="metric">
        <div class="metric-val">{queue_size}</div>
        <div class="metric-label">PR-check в очереди (считаются)</div>
      </div>
      <div class="metric">
        <div class="metric-val">{threshold}</div>
        <div class="metric-label">Порог → LIMITED если больше</div>
      </div>
      <div class="metric">
        <div class="metric-val">{meta['excluded_too_old']}</div>
        <div class="metric-label">Игнор (&gt; {ignore_h}h, zombie)</div>
      </div>
      <div class="metric">
        <div class="metric-val">{changed}</div>
        <div class="metric-label">Runner'ов изменено</div>
      </div>
    </div>

    <div class="section">
      <h2>Как приняли решение</h2>
      <div class="flow">
        <div class="flow-step">
          <div class="flow-num">Шаг 1</div>
          <div class="flow-title">Считаем очередь</div>
          <div class="flow-text">PR-check в <code>queued</code>, ждут от 1 мин до {ignore_h} ч.</div>
        </div>
        <div class="flow-arrow">→</div>
        <div class="flow-step active">
          <div class="flow-num">Шаг 2</div>
          <div class="flow-title">{queue_size} vs порог {threshold}</div>
          <div class="bar-wrap">
            <div class="bar-labels"><span>0</span><span>порог {threshold}</span></div>
            <div class="bar"><div class="bar-fill"></div></div>
          </div>
        </div>
        <div class="flow-arrow">→</div>
        <div class="flow-step active">
          <div class="flow-num">Шаг 3</div>
          <div class="flow-title">Режим {esc(mode.upper())}</div>
          <div class="flow-text">{flow_runners_text}</div>
        </div>
      </div>
    </div>

    <div class="section">
      <h2>Очередь workflow runs</h2>
      <p class="empty" style="margin-bottom:12px">Каждый workflow — отдельный столбец. Внутри столбца сортировка по queue time ↑ (меньше ждёт — выше). Gate считает только PR-check «считается».</p>
      {_queue_table(queue['all'], 'Очередь пуста.')}
    </div>

    <div class="section">
      <h2>Runner sync</h2>
      <div class="summary-row">
        <span class="chip">всего ghrun: {meta['ghrun_count']}</span>
        <span class="chip">postcommit до sync: {meta['postcommit_count_before']}</span>
        <span class="chip add">+ postcommit: {stats['add_postcommit']}</span>
        <span class="chip remove">− postcommit: {stats['remove_postcommit']}</span>
        <span class="chip">без изменений: {stats['no_change']}</span>
      </div>
      {_runner_changes_table(actions)}
    </div>

    <div class="section">
      <h2>Конфиг</h2>
      <details>
        <summary>POSTCOMMIT_GATE_CONFIG</summary>
        <div class="details-body"><code>{esc(meta['config_raw'])}</code></div>
      </details>
    </div>
  </div>
</body>
</html>"""


def write_step_summary(meta: dict, summary_path: str | None) -> None:
    if not summary_path:
        return
    lines = [
        "## Postcommits gate",
        "",
        f"**Decision:** `{meta['gate_mode']}` — {meta['decision']}",
        "",
        "📄 HTML report: artifact `postcommits-gate-report` → `index.html`",
        "",
        "| Setting | Value |",
        "|---------|-------|",
        f"| Gate mode | `{meta['gate_mode']}` |",
        f"| PR-check counted | {meta['queue_size']} |",
        f"| limited_mode_when_pr_check_queue_above | {meta['limited_mode_when_pr_check_queue_above']} |",
        f"| reserved_postcommit_runners_in_limited_mode | {meta['reserved_postcommit_runners_in_limited_mode']} |",
        f"| ghrun / postcommit before | {meta['ghrun_count']} / {meta['postcommit_count_before']} |",
    ]
    if meta["gate_mode"] == "limited":
        lines.append(f"| Runner IDs with postcommit | {meta['keep_runner_ids'] or '—'} |")
    with open(summary_path, "a", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def run(args: argparse.Namespace) -> int:
    repo = os.environ.get("GITHUB_REPOSITORY", "ydb-platform/ydb")
    queue_token = os.environ.get("GITHUB_TOKEN", "")
    runners_token = os.environ.get("GH_PERSONAL_ACCESS_TOKEN", os.environ.get("GITHUB_TOKEN", ""))
    if not queue_token:
        print("GITHUB_TOKEN is required", file=sys.stderr)
        return 1

    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)

    config_raw, cfg = load_config(
        os.environ.get("POSTCOMMIT_GATE_CONFIG", ""),
        os.environ.get("POSTCOMMIT_QUEUE_THRESHOLD", ""),
    )
    now_ts = datetime.now(timezone.utc).timestamp()

    print("Gate config:", cfg["rules_summary"])
    queue_payload = api_request(
        "GET",
        f"https://api.github.com/repos/{repo}/actions/runs?per_page=1000&page=1&status=queued",
        queue_token,
    )
    queue = classify_queue(queue_payload, cfg["ignore_pr_check_older_than_hours"], now_ts)
    queue_size = len(queue["counted"])
    gate_mode, decision = decide_mode(queue_size, cfg)

    print(f"PR-check counted: {queue_size}, ignored old: {len(queue['ignored_too_old'])}")
    print(f"Gate {gate_mode.upper()}: {decision}")

    runners_note = ""
    total_runners = ghrun_count = postcommit_count = 0
    keep_ids: list[str] = []
    actions: list[dict] = []

    try:
        runners_payload = api_request(
            "GET",
            f"https://api.github.com/repos/{repo}/actions/runners?per_page=100&page=1",
            runners_token,
        )
        ghrun = ghrun_runners(runners_payload)
        total_runners = runners_payload.get("total_count", 0)
        ghrun_count = len(ghrun)
        postcommit_count = sum(1 for r in ghrun if r["has_postcommit"])
        keep_ids, actions = plan_runner_actions(
            ghrun,
            gate_mode,
            cfg["reserved_postcommit_runners_in_limited_mode"],
        )
        apply_runner_actions(repo, runners_token, actions, args.dry_run)
    except urllib.error.HTTPError as exc:
        if exc.code == 403:
            runners_note = "Runners API unavailable (403). Queue stats are still real."
            print(f"WARN: {runners_note}", file=sys.stderr)
        else:
            raise

    meta = {
        "gate_mode": gate_mode,
        "decision": decision,
        "check_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "config_raw": config_raw,
        "rules_summary": cfg["rules_summary"],
        **cfg,
        "queue_size": queue_size,
        "excluded_too_old": len(queue["ignored_too_old"]),
        "excluded_too_fresh": len(queue["ignored_too_fresh"]),
        "total_runners": total_runners,
        "ghrun_count": ghrun_count,
        "postcommit_count_before": postcommit_count,
        "keep_runner_ids": ",".join(keep_ids),
        "workflow_run": os.environ.get("GITHUB_RUN_ID", "local-dry-run"),
        "repository": repo,
    }

    (report_dir / "queue.json").write_text(json.dumps(queue, indent=2), encoding="utf-8")
    (report_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    (report_dir / "index.html").write_text(
        build_html(meta, queue, actions, args.dry_run, runners_note),
        encoding="utf-8",
    )
    write_step_summary(meta, os.environ.get("GITHUB_STEP_SUMMARY"))

    print(f"Report: {report_dir / 'index.html'}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Postcommits gate")
    parser.add_argument("--dry-run", action="store_true", help="Do not change runner labels")
    parser.add_argument(
        "--report-dir",
        default=os.environ.get("GATE_REPORT_DIR", "gate-report"),
        help="Directory for HTML/JSON report",
    )
    return run(parser.parse_args())


if __name__ == "__main__":
    sys.exit(main())
