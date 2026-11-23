"""JSONL file buffer, send offset, and pending starts."""

from __future__ import annotations

import contextlib
import json
import os
import sys
from typing import Any, Dict, Iterator, List, Optional

from .schema import default_metrics_file

try:
    import fcntl
except ImportError:  # pragma: no cover — Windows has no flock
    fcntl = None


def lock_file(path: str) -> str:
    return f"{path}.lock"


@contextlib.contextmanager
def buffer_lock(path: str) -> Iterator[None]:
    """Serialize writers of one buffer.

    `enrich` rewrites the unsent tail from an in-memory snapshot, so an append
    landing between its read and its os.replace would be lost.
    """
    if fcntl is None or not path:
        yield
        return
    marker = lock_file(path)
    parent = os.path.dirname(marker)
    if parent:
        os.makedirs(parent, exist_ok=True)
    handle = None
    try:
        handle = open(marker, "a+")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield
    except OSError:
        # A lock we cannot take must not stop telemetry from being written.
        yield
    finally:
        if handle is not None:
            with contextlib.suppress(OSError):
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()


def split_jsonl(text: str) -> List[str]:
    """Split on \\n only, dropping the empty tail of newline-terminated text.

    str.splitlines() also breaks on U+2028, U+2029 and U+0085, which
    json.dumps(ensure_ascii=False) emits raw inside label values, turning one
    record into two unparseable fragments.
    """
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return lines


def _ends_without_newline(path: str) -> bool:
    try:
        if os.path.getsize(path) == 0:
            return False
        with open(path, "rb") as handle:
            handle.seek(-1, os.SEEK_END)
            return handle.read(1) != b"\n"
    except OSError:
        return False


def append_record(path: str, record: Dict[str, Any]) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
    with buffer_lock(path):
        # Terminate an orphaned fragment left by an interrupted write, so this
        # record does not get appended onto it and corrupted as well.
        prefix = "\n" if _ends_without_newline(path) else ""
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(prefix + line)


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
    """Complete records after the send offset, plus the offset they end at.

    A trailing fragment with no newline is left behind: the writer may still be
    in the middle of that line, and moving the offset past it would make the
    next append concatenate onto it and corrupt a second record too.
    """
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
    end = chunk.rfind(b"\n")
    if end < 0:
        return [], offset
    complete = chunk[: end + 1]
    return split_jsonl(complete.decode("utf-8")), offset + len(complete)


def pending_file(metrics_path: Optional[str] = None) -> str:
    return f"{metrics_path or default_metrics_file()}.pending"


def read_pending_spans(metrics_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """Unclosed starts. A line that cannot be parsed is reported, not swallowed."""
    path = pending_file(metrics_path)
    if not os.path.exists(path):
        return []
    with open(path, "rb") as handle:
        raw = handle.read()
    spans: List[Dict[str, Any]] = []
    dropped = 0
    for line in split_jsonl(raw.decode("utf-8", errors="replace")):
        text = line.strip()
        if not text:
            continue
        try:
            item = json.loads(text)
        except json.JSONDecodeError:
            dropped += 1
            continue
        if isinstance(item, dict):
            spans.append(item)
        else:
            dropped += 1
    if dropped:
        print(f"Warning: {dropped} unreadable open start(s) in {path}", file=sys.stderr)
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
