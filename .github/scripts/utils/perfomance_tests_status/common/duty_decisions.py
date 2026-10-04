"""Duty decisions index (wait_next_wave reports) for OLAP / TPC-C dashboards.

Published by ``dutyctl upload-report`` to public S3:

  https://storage.yandexcloud.net/workload-log/perfomance_tests_status/duty_decisions/index.json

Generate fetches the index over HTTPS (no AWS keys) and attaches
``duty_decision`` onto matching ``now_runs`` / suite rows.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

DEFAULT_BUCKET = "workload-log"
DEFAULT_ENDPOINT = "https://storage.yandexcloud.net"
DECISIONS_PREFIX = "perfomance_tests_status/duty_decisions"
INDEX_KEY = f"{DECISIONS_PREFIX}/index.json"
DECISIONS_SCHEMA = "perf-duty-decisions/v1"


def decisions_index_url(
    *,
    bucket: str = DEFAULT_BUCKET,
    endpoint: str = DEFAULT_ENDPOINT,
) -> str:
    return f"{endpoint.rstrip('/')}/{bucket}/{INDEX_KEY}"


def focus_key(
    *,
    kind: str,
    branch: str,
    db: str,
    suite: str,
    label: str,
) -> str:
    """Stable join key: kind|branch|db|suite|label."""
    return "|".join(
        [
            str(kind or "").strip().lower(),
            str(branch or "").strip(),
            str(db or "").strip(),
            str(suite or "").strip(),
            str(label or "").strip(),
        ]
    )


def sanitize_path_segment(value: str) -> str:
    """Safe S3 path segment (no slashes)."""
    s = str(value or "").strip()
    out = []
    for ch in s:
        if ch.isalnum() or ch in "._-@":
            out.append(ch)
        else:
            out.append("_")
    return "".join(out).strip("_") or "unknown"


def by_focus_key(
    *,
    kind: str,
    branch: str,
    db: str,
    suite: str,
    label: str,
) -> str:
    parts = [
        sanitize_path_segment(kind.lower() if kind else "unknown"),
        sanitize_path_segment(branch),
        sanitize_path_segment(db),
        sanitize_path_segment(suite),
        sanitize_path_segment(label),
    ]
    return f"{DECISIONS_PREFIX}/by_focus/{'/'.join(parts)}.json"


def empty_index() -> dict[str, Any]:
    return {"schema": DECISIONS_SCHEMA, "updated_at": None, "items": {}}


def normalize_index(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return empty_index()
    items = data.get("items")
    if not isinstance(items, dict):
        items = {}
    return {
        "schema": str(data.get("schema") or DECISIONS_SCHEMA),
        "updated_at": data.get("updated_at"),
        "items": dict(items),
    }


def merge_decision_into_index(
    index: dict[str, Any] | None,
    decision: dict[str, Any],
    *,
    updated_at: str | None = None,
) -> dict[str, Any]:
    """Return a new index with ``decision`` upserted by ``focus_key``."""
    out = normalize_index(index)
    key = str(decision.get("focus_key") or "")
    if not key:
        kind = str(decision.get("kind") or "")
        key = focus_key(
            kind=kind,
            branch=str(decision.get("branch") or ""),
            db=str(decision.get("db") or ""),
            suite=str(decision.get("suite") or ""),
            label=str(decision.get("label") or ""),
        )
        decision = {**decision, "focus_key": key}
    if key:
        out["items"][key] = dict(decision)
    if updated_at:
        out["updated_at"] = updated_at
    elif decision.get("updated_at"):
        out["updated_at"] = decision["updated_at"]
    return out


def fetch_duty_decisions_index(
    *,
    url: str | None = None,
    timeout: float = 20.0,
) -> tuple[dict[str, Any], str | None]:
    """GET public index. Returns (index, warning_or_None)."""
    target = url or decisions_index_url()
    req = urllib.request.Request(
        target,
        headers={"User-Agent": "ydb-perf-duty-decisions/1.0"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return empty_index(), None
        return empty_index(), f"duty_decisions index HTTP {e.code}: {e.reason}"
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        return empty_index(), f"duty_decisions index fetch failed: {e}"
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        return empty_index(), f"duty_decisions index invalid JSON: {e}"
    return normalize_index(data), None


def _run_label(run: dict[str, Any]) -> str:
    return str(
        run.get("label")
        or run.get("day")
        or run.get("Version")
        or run.get("version")
        or ""
    ).strip()


def _lookup(
    items: dict[str, Any],
    *,
    kind: str,
    branch: str,
    db: str,
    suite: str,
    label: str,
) -> dict[str, Any] | None:
    if not label:
        return None
    key = focus_key(kind=kind, branch=branch, db=db, suite=suite, label=label)
    hit = items.get(key)
    return dict(hit) if isinstance(hit, dict) else None


def attach_duty_decisions_to_report(
    data: dict[str, Any],
    index: dict[str, Any] | None,
    *,
    kind: str,
) -> int:
    """Attach ``duty_decision`` onto matching now_runs / suite rows.

    Returns number of suite rows that received at least one decision.
    """
    idx = normalize_index(index)
    items = idx.get("items") or {}
    if not isinstance(items, dict) or not items:
        data["duty_decisions"] = []
        return 0

    kind_l = str(kind or "").lower()
    attached_rows = 0
    seen_keys: list[str] = []

    def _attach_item(item: dict[str, Any]) -> bool:
        suite = str(item.get("suite") or "")
        db = str(item.get("db") or "")
        branch = str(item.get("branch") or "")
        hit_any = False
        runs = [r for r in (item.get("now_runs") or []) if isinstance(r, dict)]
        for run in runs:
            label = _run_label(run)
            dec = _lookup(
                items,
                kind=kind_l,
                branch=branch,
                db=db,
                suite=suite,
                label=label,
            )
            if not dec:
                continue
            # Only surface wait_next_wave for the badge (plan scope).
            if str(dec.get("resolution") or "") != "wait_next_wave":
                continue
            run["duty_decision"] = dec
            hit_any = True
            fk = str(dec.get("focus_key") or "")
            if fk and fk not in seen_keys:
                seen_keys.append(fk)

        # Suite-level: prefer decision on the latest now_run label.
        if runs:
            latest = runs[-1]
            if isinstance(latest.get("duty_decision"), dict):
                item["duty_decision"] = dict(latest["duty_decision"])
            else:
                # Fallback: label on item / day fields
                label = _run_label(latest) or str(item.get("day") or item.get("label") or "")
                dec = _lookup(
                    items,
                    kind=kind_l,
                    branch=branch,
                    db=db,
                    suite=suite,
                    label=label,
                )
                if dec and str(dec.get("resolution") or "") == "wait_next_wave":
                    item["duty_decision"] = dec
                    hit_any = True
        return hit_any

    for key in ("inbox", "ok"):
        for item in data.get(key) or []:
            if not isinstance(item, dict):
                continue
            if _attach_item(item):
                attached_rows += 1
            fin = item.get("finished")
            if isinstance(fin, dict):
                fin.setdefault("suite", item.get("suite"))
                fin.setdefault("db", item.get("db"))
                fin.setdefault("branch", item.get("branch"))
                _attach_item(fin)

    data["duty_decisions"] = [
        items[k] for k in seen_keys if isinstance(items.get(k), dict)
    ]
    return attached_rows
