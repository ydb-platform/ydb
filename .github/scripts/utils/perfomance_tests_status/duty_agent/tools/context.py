"""Load and validate perf-duty-context/v1 packs (olap + tpcc)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


SCHEMA = "perf-duty-context/v1"
KINDS = frozenset({"olap", "tpcc"})


class ContextError(ValueError):
    """Invalid or unsupported duty context pack."""


def load_context(path: Path | str) -> dict[str, Any]:
    p = Path(path)
    if not p.is_file():
        raise ContextError(f"context file not found: {p}")
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ContextError(f"invalid JSON: {e}") from e
    if not isinstance(data, dict):
        raise ContextError("context root must be an object")
    validate_context(data)
    return data


def validate_context(data: dict[str, Any]) -> None:
    schema = data.get("schema")
    if schema != SCHEMA:
        raise ContextError(f"unsupported schema {schema!r}; want {SCHEMA!r}")
    report = data.get("report") or {}
    if not isinstance(report, dict):
        raise ContextError("report must be an object")
    kind = report.get("kind")
    if kind not in KINDS:
        raise ContextError(f"report.kind must be one of {sorted(KINDS)}; got {kind!r}")
    sel = data.get("selection") or {}
    if not isinstance(sel, dict):
        raise ContextError("selection must be an object")
    for key in ("branch", "db", "suite"):
        if not sel.get(key):
            raise ContextError(f"selection.{key} is required")


def kind_of(ctx: dict[str, Any]) -> str:
    return str((ctx.get("report") or {}).get("kind") or "")


def focus_report_url(ctx: dict[str, Any]) -> str | None:
    sel = ctx.get("selection") or {}
    fr = sel.get("focus_run") or {}
    url = fr.get("report") if isinstance(fr, dict) else None
    if isinstance(url, str) and url.strip():
        return url.strip()
    return None


def selection_summary(ctx: dict[str, Any]) -> str:
    sel = ctx.get("selection") or {}
    fr = sel.get("focus_run") or {}
    bits = [
        kind_of(ctx),
        str(sel.get("branch") or ""),
        str(sel.get("db") or ""),
        str(sel.get("suite") or ""),
    ]
    if isinstance(fr, dict):
        label = fr.get("label") or fr.get("day") or fr.get("sha")
        if label:
            bits.append(str(label))
    return " · ".join(b for b in bits if b)
