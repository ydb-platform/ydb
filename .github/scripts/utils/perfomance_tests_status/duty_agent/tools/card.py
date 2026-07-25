"""Render duty card markdown / JSON."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .context import focus_report_url, kind_of, selection_summary


def build_card_payload(
    ctx: dict[str, Any],
    *,
    sandbox: dict[str, Any],
    history: dict[str, Any],
    label: dict[str, Any],
    gh: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema": "perf-duty-card/v1",
        "summary": selection_summary(ctx),
        "kind": kind_of(ctx),
        "selection": ctx.get("selection"),
        "suite_now": ctx.get("suite_now"),
        "sticky_query": ctx.get("sticky_query"),
        "label": label,
        "sandbox": {
            "url": sandbox.get("url"),
            "fetched": sandbox.get("fetched"),
            "error": sandbox.get("error"),
            "primary": sandbox.get("primary"),
            "fingerprints": sandbox.get("fingerprints"),
            "quotes": sandbox.get("quotes"),
            "query_hits": sandbox.get("query_hits"),
        },
        "history": history,
        "gh": gh,
        "report": ctx.get("report"),
        "links": ctx.get("links"),
        "hints": ctx.get("hints"),
        "next_steps": _next_steps(ctx, label, sandbox),
    }


def _next_steps(ctx: dict[str, Any], label: dict[str, Any], sandbox: dict[str, Any]) -> list[str]:
    steps: list[str] = []
    hyp = label.get("hypothesis") or ""
    kind = kind_of(ctx)
    url = focus_report_url(ctx)
    if "infra" in (label.get("labels") or []) or hyp.startswith("infra"):
        steps.append("Check cluster / sandbox node health around focus_run.ts (infra mid-suite).")
        steps.append("Do not open a product mute until infra is ruled out.")
    if "chronic_fail" in (label.get("labels") or []) or "chronic_in_window" in (label.get("labels") or []):
        steps.append("Chronic fail in window — search existing mute / ticket before new alert.")
    if "fresh_fail_spike" in (label.get("labels") or []):
        steps.append("Fresh fail spike — bisect sha vs previous green in suite_history.")
    if kind == "tpcc":
        steps.append("Open DataLens link from context.links.datalens if present.")
        if "lat_capped" in (label.get("labels") or []):
            steps.append("Lat capped → treat as broken; confirm WH / cluster load.")
    if url and not sandbox.get("fetched"):
        steps.append(f"Manually open sandbox report: {url}")
    elif url:
        steps.append("Confirm Allure error text matches fingerprint quotes above.")
    if not steps:
        steps.append("Triage suite_now.reasons and sticky_query; fetch sandbox if URL available.")
    steps.append("Human decision: mute / ticket / wait for next wave — harness must not mute.")
    return steps


def render_markdown(card: dict[str, Any]) -> str:
    sel = card.get("selection") or {}
    fr = sel.get("focus_run") or {}
    lab = card.get("label") or {}
    sb = card.get("sandbox") or {}
    lines = [
        f"# Duty card — {card.get('summary') or 'incident'}",
        "",
        f"- **kind**: `{card.get('kind')}`",
        f"- **hypothesis**: `{lab.get('hypothesis')}` (confidence {lab.get('confidence')})",
        f"- **labels**: {', '.join(f'`{x}`' for x in (lab.get('labels') or [])) or '—'}",
        f"- **branch / db / suite**: `{sel.get('branch')}` / `{sel.get('db')}` / `{sel.get('suite')}`",
        f"- **focus**: `{fr.get('label') or fr.get('day') or '—'}` · sha `{fr.get('sha') or '—'}`",
        f"- **sandbox**: {sb.get('url') or '—'}",
        "",
        "## Suite now",
        "",
        "```json",
        json.dumps(card.get("suite_now") or {}, indent=2, ensure_ascii=False),
        "```",
        "",
        "## Sandbox fingerprints",
        "",
        f"- primary: `{sb.get('primary')}`",
        f"- all: {', '.join(f'`{x}`' for x in (sb.get('fingerprints') or [])) or '—'}",
        f"- fetched: `{sb.get('fetched')}`" + (f" · error: {sb.get('error')}" if sb.get("error") else ""),
        "",
    ]
    quotes = sb.get("quotes") or []
    if quotes:
        lines.append("### Quotes")
        lines.append("")
        for q in quotes:
            lines.append(f"> {q}")
            lines.append("")
    lines.extend(
        [
            "## History signals",
            "",
            "```json",
            json.dumps(card.get("history") or {}, indent=2, ensure_ascii=False),
            "```",
            "",
            "## Next steps",
            "",
        ]
    )
    for i, s in enumerate(card.get("next_steps") or [], 1):
        lines.append(f"{i}. {s}")
    gh = card.get("gh") or {}
    if gh.get("enabled"):
        lines.extend(["", "## GitHub search", "", f"Query: `{gh.get('query')}`", ""])
        for it in gh.get("items") or []:
            lines.append(f"- [{it.get('state')}] [{it.get('title')}]({it.get('url')})")
        if gh.get("error"):
            lines.append(f"- error: {gh.get('error')}")
    report = card.get("report") or {}
    if report.get("url"):
        lines.extend(["", f"Report: {report.get('url')}"])
    links = card.get("links") or {}
    if links.get("datalens"):
        lines.append(f"DataLens: {links.get('datalens')}")
    lines.append("")
    return "\n".join(lines)


def write_card(path: Path, card: dict[str, Any], *, also_json: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_markdown(card), encoding="utf-8")
    if also_json:
        jp = path.with_suffix(".json") if path.suffix.lower() == ".md" else path.with_name(path.name + ".json")
        jp.write_text(json.dumps(card, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
