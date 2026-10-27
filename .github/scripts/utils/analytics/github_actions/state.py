#!/usr/bin/env python3
"""Watermark / open_runs / failed_runs for the job/step exporter."""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_ANALYTICS_ROOT = Path(__file__).resolve().parents[1]
if str(_ANALYTICS_ROOT) not in sys.path:
    sys.path.insert(0, str(_ANALYTICS_ROOT))

from collector.flush import has_send_credentials
from collector.schema import _open_ydb_wrapper, resolve_table_path
from collector.values import parse_datetime

DEFAULT_STATE_TABLE_PATH = "analytics/ci_metrics_state"
STATE_TABLE_CONFIG_KEY = "ci_metrics_state"

WATERMARK_KEY = "export_watermark"
OPEN_RUNS_KEY = "open_runs"
FAILED_RUNS_KEY = "failed_runs"

# A completed run whose jobs could not be listed is retried this many times
# before it is dropped, so a deleted run cannot be retried forever.
MAX_RETRY_ATTEMPTS = 5

_KEY_RE = re.compile(r"^[a-z][a-z0-9_]*$")

RunRef = Tuple[int, int]


def resolve_state_table_path(ydb_wrapper=None) -> str:
    return resolve_table_path(
        ydb_wrapper,
        table_config_key=STATE_TABLE_CONFIG_KEY,
        default=DEFAULT_STATE_TABLE_PATH,
    )


def build_create_state_table_sql(table_path: str) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
            `name` Utf8 NOT NULL,
            `updated_at` Timestamp,
            `payload` Json,
            PRIMARY KEY (`name`)
        )
    """


def _check_key(name: str) -> str:
    if not _KEY_RE.match(name or ""):
        raise ValueError(f"invalid state key: {name!r}")
    return name


def read_state(name: str, table_path: Optional[str] = None) -> Tuple[Optional[Dict[str, Any]], bool]:
    """Return (payload, read_ok). No row is (None, True); a failed read is (None, False)."""
    _check_key(name)
    if not has_send_credentials():
        return None, False
    try:
        with _open_ydb_wrapper() as wrapper:
            if not wrapper.check_credentials():
                return None, False
            path = table_path or resolve_state_table_path(wrapper)
            rows = wrapper.execute_scan_query(
                f'SELECT payload FROM `{path}` WHERE name = "{name}"',
                query_name=f"ci_metrics_state_read_{name}",
            )
    except Exception as exc:  # noqa: BLE001
        print(f"Warning: state read for {name} failed: {exc}")
        return None, False
    for row in rows or []:
        payload = row.get("payload") if isinstance(row, dict) else None
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8", errors="replace")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                print(f"Warning: state payload for {name} is not JSON")
                return None, False
        if isinstance(payload, dict):
            return payload, True
        if payload is not None:
            print(f"Warning: state payload for {name} is not a JSON object")
            return None, False
    return None, True


def write_state(name: str, payload: Dict[str, Any], table_path: Optional[str] = None) -> bool:
    _check_key(name)
    if not has_send_credentials():
        print(f"Analytics YDB credentials are missing, state {name} not saved")
        return False
    try:
        with _open_ydb_wrapper() as wrapper:
            if not wrapper.check_credentials():
                return False
            path = table_path or resolve_state_table_path(wrapper)
            wrapper.execute_dml(
                f"""
                DECLARE $name AS Utf8;
                DECLARE $payload AS Utf8;

                UPSERT INTO `{path}` (`name`, `updated_at`, `payload`)
                VALUES ($name, CurrentUtcTimestamp(), CAST($payload AS Json));
                """,
                {"$name": name, "$payload": json.dumps(payload, separators=(",", ":"))},
                query_name=f"ci_metrics_state_write_{name}",
            )
    except Exception as exc:  # noqa: BLE001
        print(f"Warning: state write for {name} failed: {exc}")
        return False
    return True


def load_watermark(table_path: Optional[str] = None) -> Tuple[Optional[datetime], bool]:
    """Return (watermark, read_ok). read_ok is false when the read itself failed.

    Unlike a MAX(exported_at) scan over the metrics table, this has no lookback
    floor, so a watermark of any age is visible.
    """
    payload, ok = read_state(WATERMARK_KEY, table_path)
    if not ok:
        return None, False
    if payload is None:
        return None, True
    return parse_datetime(payload.get("exported_until")), True


def save_watermark(moment: datetime, table_path: Optional[str] = None) -> bool:
    return write_state(
        WATERMARK_KEY,
        {"exported_until": moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")},
        table_path,
    )


def _refs_from_payload(payload: Optional[Dict[str, Any]]) -> List[RunRef]:
    refs: List[RunRef] = []
    for item in (payload or {}).get("runs") or []:
        if not isinstance(item, dict):
            continue
        try:
            ref = (int(item["id"]), int(item.get("attempt") or 1))
        except (KeyError, TypeError, ValueError):
            continue
        if ref not in refs:
            refs.append(ref)
    return refs


def load_open_runs(table_path: Optional[str] = None) -> Tuple[List[RunRef], bool]:
    payload, ok = read_state(OPEN_RUNS_KEY, table_path)
    if not ok:
        return [], False
    return _refs_from_payload(payload), True


def save_open_runs(refs: List[RunRef], table_path: Optional[str] = None) -> bool:
    return write_state(
        OPEN_RUNS_KEY,
        {"runs": [{"id": run_id, "attempt": attempt} for run_id, attempt in refs]},
        table_path,
    )


def load_failed_runs(table_path: Optional[str] = None) -> Tuple[Dict[RunRef, int], bool]:
    payload, ok = read_state(FAILED_RUNS_KEY, table_path)
    if not ok:
        return {}, False
    failed: Dict[RunRef, int] = {}
    for item in (payload or {}).get("runs") or []:
        if not isinstance(item, dict):
            continue
        try:
            ref = (int(item["id"]), int(item.get("attempt") or 1))
        except (KeyError, TypeError, ValueError):
            continue
        try:
            tries = int(item.get("tries") or 1)
        except (TypeError, ValueError):
            tries = 1
        failed[ref] = max(tries, failed.get(ref, 0))
    return failed, True


def save_failed_runs(failed: Dict[RunRef, int], table_path: Optional[str] = None) -> bool:
    runs = [
        {"id": run_id, "attempt": attempt, "tries": tries}
        for (run_id, attempt), tries in sorted(failed.items())
    ]
    return write_state(FAILED_RUNS_KEY, {"runs": runs}, table_path)


def record_failure(failed: Dict[RunRef, int], ref: RunRef) -> bool:
    """Count one more failure for ref. False means the retry budget is spent."""
    tries = failed.get(ref, 0) + 1
    if tries >= MAX_RETRY_ATTEMPTS:
        failed.pop(ref, None)
        return False
    failed[ref] = tries
    return True
