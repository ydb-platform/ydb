"""JSONL file buffer, send offset, and pending spans."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from .schema import default_metrics_file


def append_record(path: str, record: Dict[str, Any]) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


def offset_file(path: str) -> str:
    return f"{path}.offset"


def read_send_offset(path: str) -> int:
    marker = offset_file(path)
    try:
        with open(marker, encoding="utf-8") as handle:
            return max(int(handle.read().strip() or "0"), 0)
    except (FileNotFoundError, ValueError):
        return 0


def write_send_offset(path: str, offset: int) -> None:
    with open(offset_file(path), "w", encoding="utf-8") as handle:
        handle.write(str(offset))


def load_unsent_lines(path: str) -> tuple[List[str], int]:
    if not path or not os.path.exists(path):
        return [], 0
    size = os.path.getsize(path)
    offset = read_send_offset(path)
    if offset > size:
        offset = 0
    if offset >= size:
        return [], size
    with open(path, "rb") as handle:
        handle.seek(offset)
        chunk = handle.read()
        new_offset = handle.tell()
    return chunk.decode("utf-8").splitlines(), new_offset


def pending_file(metrics_path: Optional[str] = None) -> str:
    return f"{metrics_path or default_metrics_file()}.pending"


def read_pending_spans(metrics_path: Optional[str] = None) -> List[Dict[str, Any]]:
    path = pending_file(metrics_path)
    if not os.path.exists(path):
        return []
    spans: List[Dict[str, Any]] = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                item = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                spans.append(item)
    return spans


def write_pending_spans(spans: List[Dict[str, Any]], metrics_path: Optional[str] = None) -> None:
    path = pending_file(metrics_path)
    if not spans:
        if os.path.exists(path):
            os.remove(path)
        return
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        for span in spans:
            handle.write(json.dumps(span, ensure_ascii=False, separators=(",", ":")) + "\n")
    os.replace(tmp, path)
