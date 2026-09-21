#!/usr/bin/env python3
"""Append one CI stage event to a JSONL file. Never fails the caller."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _parse_started_at(started_at: Optional[str], started_epoch: Optional[str]) -> str:
    if started_at:
        return started_at
    if started_epoch:
        ts = float(started_epoch)
        if ts > 1e12:
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def build_record(args: argparse.Namespace) -> Dict[str, Any]:
    started_at = _parse_started_at(args.started_at, args.started_epoch)
    finished_epoch = args.finished_epoch
    duration_ms = args.duration_ms
    if duration_ms is None and args.started_epoch and finished_epoch:
        start = float(args.started_epoch)
        end = float(finished_epoch)
        if start > 1e12:
            start /= 1000.0
        if end > 1e12:
            end /= 1000.0
        duration_ms = max(int((end - start) * 1000), 0)
    elif duration_ms is None and args.started_epoch:
        start = float(args.started_epoch)
        if start > 1e12:
            start /= 1000.0
        duration_ms = max(int((datetime.now(timezone.utc).timestamp() - start) * 1000), 0)

    extra = None
    if args.extra:
        try:
            extra = json.loads(args.extra)
        except json.JSONDecodeError:
            extra = {"raw": args.extra}

    record: Dict[str, Any] = {
        "stage_kind": args.stage_kind,
        "stage_name": args.stage_name,
        "started_at": started_at,
        "duration_ms": duration_ms,
        "conclusion": args.conclusion,
        "ya_attempt": args.ya_attempt,
        "cache_mode": args.cache_mode or os.environ.get("CI_CACHE_MODE") or None,
    }
    if extra is not None:
        record["extra"] = extra
    return {key: value for key, value in record.items() if value is not None and value != ""}


def append_record(path: str, record: Dict[str, Any]) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Append a CI stage timing record to JSONL")
    parser.add_argument("command", choices=["append"], help="append a stage record")
    parser.add_argument("--file", required=True, help="JSONL path")
    parser.add_argument("--stage-kind", default="ya_phase")
    parser.add_argument("--stage-name", required=True)
    parser.add_argument("--started-at", default=None, help="ISO-8601 timestamp")
    parser.add_argument("--started-epoch", default=None, help="epoch seconds (or ms)")
    parser.add_argument("--finished-epoch", default=None, help="epoch seconds (or ms)")
    parser.add_argument("--duration-ms", type=int, default=None)
    parser.add_argument("--conclusion", default="success")
    parser.add_argument("--ya-attempt", type=int, default=None)
    parser.add_argument("--cache-mode", default=None)
    parser.add_argument("--extra", default=None, help="JSON object string")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    try:
        args = parse_args(argv)
        record = build_record(args)
        append_record(args.file, record)
        return 0
    except Exception as exc:  # noqa: BLE001 — never fail CI because of telemetry
        print(f"Warning: failed to record CI stage: {exc}", file=sys.stderr)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
