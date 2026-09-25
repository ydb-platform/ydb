#!/usr/bin/env python3
"""JSONL buffer and YDB flush. Wrappers add their own columns on top.

    python3 -m collector start my_step --source my_job --label k=v
    python3 -m collector send --conclusion success
"""

from __future__ import annotations

import sys
from typing import Any

from .cli import parse_args, run_cli
from .flush import flush_file as _flush_file
from .flush import upsert_metrics as _upsert_metrics
from .spans import end, enrich, start, track
from .spans import send as _send
from .values import normalize_metric


def upsert_metrics(*args: Any, **kwargs: Any) -> int:
    return _upsert_metrics(*args, **kwargs)


def flush_file(*args: Any, **kwargs: Any) -> int:
    kwargs.setdefault("upsert", upsert_metrics)
    return _flush_file(*args, **kwargs)


def send(*args: Any, **kwargs: Any) -> int:
    kwargs.setdefault("flush", flush_file)
    return _send(*args, **kwargs)


def main(argv=None) -> int:
    try:
        return run_cli(
            parse_args(argv),
            start_fn=start,
            end_fn=end,
            track_fn=track,
            send_fn=send,
            flush_fn=flush_file,
            enrich_fn=enrich,
        )
    except Exception as exc:  # noqa: BLE001 — telemetry must not fail the caller
        print(f"Warning: analytics failed: {exc}", file=sys.stderr)
        return 0


__all__ = (
    "end",
    "enrich",
    "flush_file",
    "main",
    "normalize_metric",
    "run_cli",
    "send",
    "start",
    "track",
    "upsert_metrics",
)


if __name__ == "__main__":
    raise SystemExit(main())
