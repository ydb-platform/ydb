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
    all_pr = []
    for run in runs_payload.get("workflow_runs", []):
        if run.get("name") != "PR-check":
            continue
        updated = run["updated_at"].replace("Z", "+00:00")
        waiting = now_ts - datetime.fromisoformat(updated).timestamp()
        all_pr.append(
            {
                "id": run["id"],
                "name": run["name"],
                "updated_at": run["updated_at"],
                "html_url": run["html_url"],
                "waiting_seconds": waiting,
                "waiting_human": fmt_wait(waiting),
            }
        )
    return {
        "counted": [r for r in all_pr if 60 < r["waiting_seconds"] <= max_age],
        "ignored_too_old": [r for r in all_pr if r["waiting_seconds"] > max_age],
        "ignored_too_fresh": [r for r in all_pr if r["waiting_seconds"] <= 60],
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


def _runs_list(runs: list[dict], empty_text: str) -> str:
    if not runs:
        return f'<p class="empty">{esc(empty_text)}</p>'
    items = []
    for run in runs:
        items.append(
            f'<li><a href="{esc(run["html_url"])}">{esc(run["id"])}</a>'
            f'<span class="pill wait">{esc(run["waiting_human"])}</span></li>'
        )
    return f'<ul class="run-list">{"".join(items)}</ul>'


def _runner_cards(actions: list[dict]) -> str:
    if not actions:
        return '<p class="empty">Нет данных по runner\'ам.</p>'
    cards = []
    for item in actions:
        action = item["action"]
        if action == "add_postcommit":
            icon, label, cls = "+", "добавили postcommit", "changed add"
        elif action == "remove_postcommit":
            icon, label, cls = "−", "сняли postcommit", "changed remove"
        else:
            icon, label, cls = "=", "без изменений", "unchanged"
        postcommit_now = "да" if item["should_have_postcommit"] else "нет"
        cards.append(
            f'<div class="runner-card {cls}">'
            f'<div class="runner-icon">{icon}</div>'
            f'<div class="runner-body">'
            f'<div class="runner-title">{esc(item["name"] or item["id"])}</div>'
            f'<div class="runner-sub">ID {esc(item["id"])} · postcommit: <strong>{postcommit_now}</strong></div>'
            f'<div class="runner-action">{esc(label)}</div>'
            f"</div></div>"
        )
    return f'<div class="runner-grid">{"".join(cards)}</div>'


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
    .runner-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 10px; }}
    .runner-card {{
      display: flex; gap: 10px; border: 1px solid var(--border); border-radius: 10px; padding: 12px; background: #fafbfc;
    }}
    .runner-card.changed.add {{ border-color: #86efac; }}
    .runner-card.changed.remove {{ border-color: #fecaca; }}
    .runner-icon {{
      width: 28px; height: 28px; border-radius: 8px; display: flex; align-items: center; justify-content: center;
      font-weight: 700; background: #eaeef2; flex-shrink: 0;
    }}
    .changed.add .runner-icon {{ background: #dcfce7; color: var(--add); }}
    .changed.remove .runner-icon {{ background: #fee2e2; color: var(--remove); }}
    .runner-title {{ font-weight: 600; font-size: .9rem; }}
    .runner-sub {{ font-size: .78rem; color: var(--muted); }}
    .runner-action {{ font-size: .78rem; margin-top: 4px; }}
    .summary-row {{ display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 14px; }}
    .chip {{ font-size: .82rem; padding: 6px 10px; border-radius: 999px; background: #f1f5f9; }}
    .chip.add {{ background: #dcfce7; color: var(--add); }}
    .chip.remove {{ background: #fee2e2; color: var(--remove); }}
    details {{ border: 1px solid var(--border); border-radius: 10px; padding: 0; overflow: hidden; margin-top: 10px; }}
    summary {{
      cursor: pointer; padding: 12px 14px; background: #f8fafc; font-weight: 600; list-style: none;
    }}
    summary::-webkit-details-marker {{ display: none; }}
    .details-body {{ padding: 12px 14px 14px; }}
    .run-list {{ list-style: none; padding: 0; margin: 0; }}
    .run-list li {{
      display: flex; justify-content: space-between; gap: 12px; padding: 8px 0;
      border-bottom: 1px solid var(--border); font-size: .88rem;
    }}
    .run-list li:last-child {{ border-bottom: none; }}
    .pill {{ font-size: .75rem; padding: 2px 8px; border-radius: 999px; background: #eef2f7; color: var(--muted); }}
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
      <h2>Runner'ы</h2>
      <div class="summary-row">
        <span class="chip">всего ghrun: {meta['ghrun_count']}</span>
        <span class="chip">postcommit до sync: {meta['postcommit_count_before']}</span>
        <span class="chip add">+ postcommit: {stats['add_postcommit']}</span>
        <span class="chip remove">− postcommit: {stats['remove_postcommit']}</span>
        <span class="chip">без изменений: {stats['no_change']}</span>
      </div>
      {_runner_cards(actions)}
    </div>

    <div class="section">
      <h2>Очередь PR-check</h2>
      <details open>
        <summary>Считаются в gate ({len(queue['counted'])})</summary>
        <div class="details-body">{_runs_list(queue['counted'], 'Нет — gate видит пустую очередь.')}</div>
      </details>
      <details>
        <summary>Игнор: слишком старые (&gt; {ignore_h}h) — {len(queue['ignored_too_old'])}</summary>
        <div class="details-body">
          <p class="empty" style="margin-bottom:8px">Обычно broken re-run / zombie. На gate не влияют.</p>
          {_runs_list(queue['ignored_too_old'], 'Нет.')}
        </div>
      </details>
      <details>
        <summary>Игнор: слишком свежие (≤ 60s) — {len(queue['ignored_too_fresh'])}</summary>
        <div class="details-body">{_runs_list(queue['ignored_too_fresh'], 'Нет.')}</div>
      </details>
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
        f"https://api.github.com/repos/{repo}/actions/runs?per_page=1000&page=1&status=queued&event=pull_request_target",
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
