"""Extract node-down / disconnect timeline from kikimr log text."""

from __future__ import annotations

import re
from typing import Any

# 2026-07-25T21:34:32.607150+0300 / ...Z / space-separated
TS_RE = re.compile(
    r"(?P<ts>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)"
)

EVENT_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (
        "connection_lost",
        re.compile(r"Connection with node\s+(?P<node>\d+)\s+lost", re.I),
    ),
    (
        "node_restarted",
        re.compile(
            r"Node\s+(?P<node>\d+)@(?P<host>[a-z0-9.-]+\.host\.testing\.ydb\.yandex\.net)\s+was restarted",
            re.I,
        ),
    ),
    (
        "node_down",
        re.compile(
            r"Node\s+(?P<node>\d+)@(?P<host>[a-z0-9.-]+\.host\.testing\.ydb\.yandex\.net)\s+is down",
            re.I,
        ),
    ),
    (
        "cluster_unavailable",
        re.compile(
            r"Kikimr cluster or one of its subsystems was unavailable",
            re.I,
        ),
    ),
]

MAX_EVENTS = 40


def _nearest_ts(text: str, pos: int, *, window: int = 800) -> str | None:
    start = max(0, pos - window)
    # Prefer timestamp before the match (same log line / nearby)
    before = text[start:pos]
    m = None
    for m in TS_RE.finditer(before):
        pass
    if m:
        return m.group("ts")
    after = text[pos : min(len(text), pos + 120)]
    m2 = TS_RE.search(after)
    return m2.group("ts") if m2 else None


def extract_node_events(text: str, *, max_events: int = MAX_EVENTS) -> list[dict[str, Any]]:
    """Return ordered unique-ish node/cluster events with optional timestamps."""
    events: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for kind, pat in EVENT_PATTERNS:
        for m in pat.finditer(text):
            gd = m.groupdict()
            node = gd.get("node") or ""
            host = gd.get("host") or ""
            ts = _nearest_ts(text, m.start()) or ""
            key = (kind, node, host, ts)
            if key in seen:
                continue
            seen.add(key)
            events.append(
                {
                    "kind": kind,
                    "ts": ts or None,
                    "node": node or None,
                    "host": host or None,
                    "snippet": re.sub(r"\s+", " ", m.group(0))[:180],
                }
            )
    # Sort: timestamped first chronologically, then unknown ts
    def sort_key(e: dict[str, Any]) -> tuple[int, str]:
        ts = e.get("ts") or ""
        return (0 if ts else 1, ts)

    events.sort(key=sort_key)
    return events[:max_events]


def summarize_timeline(events: list[dict[str, Any]]) -> dict[str, Any]:
    def sort_key(e: dict[str, Any]) -> tuple[int, str]:
        ts = e.get("ts") or ""
        return (0 if ts else 1, ts)

    ordered = sorted(events, key=sort_key)
    stamped = [e for e in ordered if e.get("ts")]
    first = stamped[0] if stamped else (ordered[0] if ordered else None)
    last = stamped[-1] if stamped else (ordered[-1] if ordered else None)
    by_kind: dict[str, int] = {}
    for e in ordered:
        k = str(e.get("kind") or "?")
        by_kind[k] = by_kind.get(k, 0) + 1
    hosts = []
    nodes = []
    for e in ordered:
        h, n = e.get("host"), e.get("node")
        if h and h not in hosts:
            hosts.append(h)
        if n and n not in nodes:
            nodes.append(n)
    # Prefer timestamped events first in the card list
    display = stamped + [e for e in ordered if not e.get("ts")]
    return {
        "event_count": len(ordered),
        "by_kind": by_kind,
        "first_event_ts": (first or {}).get("ts"),
        "last_event_ts": (last or {}).get("ts"),
        "first_event": first,
        "last_event": last,
        "hosts": hosts[:12],
        "nodes": nodes[:12],
        "events": display[:20],
    }


def merge_case_timelines(cases: list[dict[str, Any]]) -> dict[str, Any]:
    all_events: list[dict[str, Any]] = []
    for c in cases:
        aa = c.get("attach_analysis") or {}
        for e in aa.get("events") or []:
            if isinstance(e, dict):
                all_events.append({**e, "test": c.get("name")})
    return summarize_timeline(all_events)
