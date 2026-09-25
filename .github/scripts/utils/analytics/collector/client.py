#!/usr/bin/env python3
"""JSONL buffer and YDB flush. Wrappers add their own columns on top.

    python3 -m collector start my_step --source my_job --label k=v
    python3 -m collector send --conclusion success
"""

from __future__ import annotations

import sys
from typing import Any

from .buffer import (
    append_record,
    load_unsent_lines,
    offset_file,
    pending_file,
    read_pending_spans,
    read_send_offset,
    write_pending_spans,
    write_send_offset,
)
from .cli import add_enrich_cli_args, add_track_cli_args, parse_args, run_cli
from .flush import has_send_credentials, rows_from_jsonl
from .flush import flush_file as _flush_file
from .flush import upsert_metrics as _upsert_metrics
from .schema import (
    COLUMNS_SCHEMA,
    CREDENTIAL_ENVS,
    DEFAULT_KIND,
    DEFAULT_TABLE_PATH,
    INTERNAL_FIELD_KEYS,
    KIND_UNITS,
    PRIMARY_KEYS,
    TABLE_CONFIG_KEY,
    TTL_MINUTES,
    AttachFn,
    EnrichFn,
    _open_ydb_wrapper,
    _ydb_wrapper_cls,
    build_column_types,
    build_create_table_sql,
    default_metrics_file,
    resolve_table_path,
)
from .spans import attach_context, close_span, end, enrich, start, timed, track
from .spans import send as _send
from .values import (
    _as_uint,
    build_track_record,
    default_env_defaults,
    duration_ms_between,
    merge_defaults,
    normalize_metric,
    parse_datetime,
    parse_labels,
)


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
    "AttachFn",
    "COLUMNS_SCHEMA",
    "CREDENTIAL_ENVS",
    "DEFAULT_KIND",
    "DEFAULT_TABLE_PATH",
    "EnrichFn",
    "INTERNAL_FIELD_KEYS",
    "KIND_UNITS",
    "PRIMARY_KEYS",
    "TABLE_CONFIG_KEY",
    "TTL_MINUTES",
    "add_enrich_cli_args",
    "add_track_cli_args",
    "append_record",
    "attach_context",
    "build_column_types",
    "build_create_table_sql",
    "build_track_record",
    "close_span",
    "default_env_defaults",
    "default_metrics_file",
    "duration_ms_between",
    "end",
    "enrich",
    "flush_file",
    "has_send_credentials",
    "load_unsent_lines",
    "main",
    "merge_defaults",
    "normalize_metric",
    "offset_file",
    "parse_args",
    "parse_datetime",
    "parse_labels",
    "pending_file",
    "read_pending_spans",
    "read_send_offset",
    "resolve_table_path",
    "rows_from_jsonl",
    "run_cli",
    "send",
    "start",
    "timed",
    "track",
    "upsert_metrics",
    "write_pending_spans",
    "write_send_offset",
)


if __name__ == "__main__":
    raise SystemExit(main())
