"""Shared utilities for loading resource monitoring data (JSONL from monitor_resources.py)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_resources_jsonl(path: Path, encoding: str = "utf-8") -> list[dict[str, Any]]:
    """Load JSONL file from monitor_resources.py. Returns list of record dicts."""
    records: list[dict[str, Any]] = []
    text = path.read_text(encoding=encoding, errors="replace").strip()
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def is_host_record(record: dict[str, Any]) -> bool:
    """Host-wide sample. Test-cgroup ticks use kind=test."""
    kind = record.get("kind")
    return kind in (None, "", "host")


def measured_test_key(record: dict[str, Any]) -> str:
    cgroup = str(record.get("cgroup") or "").strip()
    if cgroup:
        return cgroup
    suite = str(record.get("suite") or "").strip()
    chunk = record.get("chunk")
    if chunk is None or chunk == "":
        return suite or "unknown"
    return f"{suite}#{chunk}"


def measured_test_label(record: dict[str, Any]) -> str:
    suite = str(record.get("suite") or record.get("cgroup") or "unknown")
    chunk = record.get("chunk")
    if chunk is None or chunk == "":
        return suite
    return f"{suite} [{chunk}]"
