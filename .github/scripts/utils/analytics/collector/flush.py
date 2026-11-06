"""Normalize JSONL batches and upsert them into YDB."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from .buffer import load_unsent_lines, write_send_offset
from .schema import (
    CREDENTIAL_ENVS,
    DEFAULT_TABLE_PATH,
    TABLE_CONFIG_KEY,
    _open_ydb_wrapper,
    build_column_types,
    build_create_table_sql,
    default_metrics_file,
    resolve_table_path,
)
from .values import default_env_defaults, merge_defaults, normalize_metric, normalize_skip_reason

_ENSURED_TABLES: set[str] = set()


def has_send_credentials(envs: Sequence[str] = CREDENTIAL_ENVS) -> bool:
    return any(os.environ.get(name) for name in envs)


def skipped_file(path: str) -> str:
    return f"{path}.skipped"


def append_skipped(path: str, skipped: List[Dict[str, Any]]) -> None:
    if not skipped:
        return
    dest = skipped_file(path)
    parent = os.path.dirname(dest)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(dest, "a", encoding="utf-8") as handle:
        for item in skipped:
            handle.write(json.dumps(item, ensure_ascii=False, default=str, separators=(",", ":")) + "\n")


def classify_jsonl_lines(
    lines: Iterable[str],
    defaults: Optional[Dict[str, Any]] = None,
    *,
    normalize: Optional[Callable[..., Optional[Dict[str, Any]]]] = None,
    skip_reason: Optional[Callable[[Dict[str, Any]], Optional[str]]] = None,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    now = datetime.now(timezone.utc)
    rows: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    normalize_fn = normalize or normalize_metric
    reason_fn = skip_reason or normalize_skip_reason
    for line in lines:
        text = line.strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            skipped.append({"reason": "invalid json", "record": text})
            continue
        if not isinstance(payload, dict):
            skipped.append({"reason": "not an object", "record": payload})
            continue
        if defaults:
            payload = merge_defaults(payload, defaults)
        row = normalize_fn(payload, now=now)
        if row is None:
            skipped.append({"reason": reason_fn(payload) or "invalid", "record": payload})
            continue
        rows.append(row)
    return rows, skipped


def rows_from_jsonl(
    lines: Iterable[str],
    defaults: Optional[Dict[str, Any]] = None,
    *,
    normalize: Optional[Callable[..., Optional[Dict[str, Any]]]] = None,
    skip_reason: Optional[Callable[[Dict[str, Any]], Optional[str]]] = None,
) -> List[Dict[str, Any]]:
    rows, skipped = classify_jsonl_lines(
        lines, defaults, normalize=normalize, skip_reason=skip_reason
    )
    if skipped:
        print(f"Skipped {len(skipped)} invalid analytics line(s)")
    return rows


def upsert_metrics(
    ydb_wrapper,
    rows: List[Dict[str, Any]],
    table_path: Optional[str] = None,
    batch_size: int = 200,
    *,
    columns: Optional[Sequence[Tuple[str, str, bool]]] = None,
    primary_keys: Optional[Sequence[str]] = None,
    table_config_key: str = TABLE_CONFIG_KEY,
    default_table: str = DEFAULT_TABLE_PATH,
    ensure_table: bool = True,
) -> int:
    if not rows:
        return 0
    path = table_path or resolve_table_path(ydb_wrapper, table_config_key=table_config_key, default=default_table)
    if ensure_table and path not in _ENSURED_TABLES:
        ydb_wrapper.create_table(path, build_create_table_sql(path, columns=columns, primary_keys=primary_keys))
        _ENSURED_TABLES.add(path)
    ydb_wrapper.bulk_upsert_batches(path, rows, build_column_types(columns), batch_size)
    return len(rows)


def flush_file(
    path: Optional[str] = None,
    table_path: Optional[str] = None,
    defaults: Optional[Dict[str, Any]] = None,
    *,
    normalize: Optional[Callable[..., Optional[Dict[str, Any]]]] = None,
    skip_reason: Optional[Callable[[Dict[str, Any]], Optional[str]]] = None,
    columns: Optional[Sequence[Tuple[str, str, bool]]] = None,
    primary_keys: Optional[Sequence[str]] = None,
    table_config_key: str = TABLE_CONFIG_KEY,
    default_table: str = DEFAULT_TABLE_PATH,
    ydb_wrapper_factory: Optional[Callable] = None,
    upsert: Optional[Callable[..., int]] = None,
    ensure_table: bool = True,
) -> int:
    """Export unacknowledged completed events. Safe to call repeatedly."""
    try:
        metrics_path = path or default_metrics_file()
        lines, new_offset = load_unsent_lines(metrics_path)
        if not lines:
            return 0
        rows, skipped = classify_jsonl_lines(
            lines,
            defaults=defaults if defaults is not None else default_env_defaults(),
            normalize=normalize,
            skip_reason=skip_reason,
        )
        if not rows:
            print(f"No valid metric rows in {metrics_path}, keeping local batch")
            return 0
        if not has_send_credentials():
            print("Analytics YDB credentials are missing, keeping local batch")
            return 0
        upsert_fn = upsert or upsert_metrics
        with _open_ydb_wrapper(ydb_wrapper_factory) as wrapper:
            if not wrapper.check_credentials():
                print("Analytics YDB credentials are missing, keeping local batch")
                return 0
            path_used = table_path or resolve_table_path(
                wrapper, table_config_key=table_config_key, default=default_table
            )
            uploaded = upsert_fn(
                wrapper,
                rows,
                table_path=path_used,
                columns=columns,
                primary_keys=primary_keys,
                table_config_key=table_config_key,
                default_table=default_table,
                ensure_table=ensure_table,
            )
        append_skipped(metrics_path, skipped)
        write_send_offset(metrics_path, new_offset)
        extra = f", skipped {len(skipped)}" if skipped else ""
        print(f"Uploaded {uploaded} analytics rows to {path_used}{extra}")
        return uploaded
    except Exception as exc:  # noqa: BLE001 — telemetry must not fail the caller
        print(f"Warning: analytics send failed: {exc}", file=sys.stderr)
        return 0
