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
