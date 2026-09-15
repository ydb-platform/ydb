"""Helpers for persisting command output to per-step log files.

Logs are stored under ``{deploy_ctx.work_directory}/logs/``. File names are
derived from the step title (rich tags stripped, unsafe characters replaced).
A short step id can be appended to disambiguate concurrent steps with the
same title.
"""

import os
import re
from typing import Iterable, List, Optional, Tuple

from ydb.tools.mnc.lib import deploy_ctx


_RICH_TAG_RE = re.compile(r"\[[/a-zA-Z0-9 _#=.,:-]*\]")
_UNSAFE_CHARS_RE = re.compile(r"[^a-zA-Z0-9_.-]+")

DEFAULT_TAIL_LINES = 200


def get_logs_dir() -> str:
    return os.path.join(deploy_ctx.work_directory or ".", "logs")


def sanitize_title(title: str) -> str:
    if not title:
        return "step"
    plain = _RICH_TAG_RE.sub("", title)
    plain = plain.strip()
    plain = _UNSAFE_CHARS_RE.sub("_", plain)
    plain = plain.strip("_.")
    return plain or "step"


def log_path_for(step_title: str, step_id: Optional[str] = None) -> str:
    name = sanitize_title(step_title)
    if step_id:
        # Keep only last 8 chars of uuid for readability.
        suffix = step_id.replace("-", "")[-8:]
        name = f"{name}.{suffix}"
    return os.path.join(get_logs_dir(), f"{name}.log")


def ensure_logs_dir() -> str:
    path = get_logs_dir()
    try:
        os.makedirs(path, exist_ok=True)
    except OSError:
        pass
    return path


def open_log_file(step_title: str, step_id: Optional[str] = None):
    """Open a log file for writing. Returns ``(path, file_or_none)``.

    On any IO error, ``file`` will be ``None`` and the caller can skip
    persistence without failing the whole step.
    """
    ensure_logs_dir()
    path = log_path_for(step_title, step_id)
    try:
        return path, open(path, "w", encoding="utf-8", errors="replace")
    except OSError:
        return path, None


def tail_text(chunks: Iterable[str], max_lines: int = DEFAULT_TAIL_LINES) -> Tuple[str, int]:
    """Return ``(tail_text, total_lines)`` for an iterable of text chunks."""
    lines: List[str] = []
    total = 0
    for chunk in chunks:
        if not chunk:
            continue
        chunk_lines = chunk.splitlines()
        total += len(chunk_lines)
        lines.extend(chunk_lines)
        if len(lines) > max_lines * 4:
            # Avoid unbounded memory growth on extremely long runs.
            lines = lines[-max_lines:]
    tail = lines[-max_lines:] if max_lines > 0 else lines
    return "\n".join(tail), total
