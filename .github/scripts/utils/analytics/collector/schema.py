"""Table schema, file path, and YDB type helpers."""

from __future__ import annotations

import os
import sys
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

try:
    import ydb
except ImportError:  # pragma: no cover
    ydb = None

AttachFn = Callable[[Dict[str, Any]], Dict[str, Any]]
EnrichFn = Callable[[Dict[str, Any]], Dict[str, Any]]

DEFAULT_TABLE_PATH = "analytics/events"
TABLE_CONFIG_KEY = "analytics_events"
TTL_MINUTES = 365 * 24 * 60
DEFAULT_KIND = "duration"
KIND_UNITS = {
    "duration": "ms",
    "gauge": "",
    "count": "count",
    "event": "",
    "info": "",
}
COLUMNS_SCHEMA = [
    ("date", "Date", False),
    ("event_ts", "Timestamp", False),
    ("run_id", "Uint64", False),
    ("name", "Utf8", False),
    ("kind", "Utf8", False),
    ("source", "Utf8", False),
    ("span_id", "Utf8", False),
    ("value", "Double", True),
    ("unit", "Utf8", True),
    ("conclusion", "Utf8", True),
    ("labels", "Json", True),
    ("exported_at", "Timestamp", True),
]
PRIMARY_KEYS = ("event_ts", "date", "run_id", "source", "name", "kind", "span_id")
CREDENTIAL_ENVS = (
    "ANALYTICS_YDB_CREDENTIALS",
    "CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS",
)
INTERNAL_FIELD_KEYS = ("file", "attach", "enrich", "flush", "table_path")


def default_metrics_file() -> str:
    return os.environ.get("CI_METRICS_FILE") or "ci_metrics.jsonl"


def resolve_table_path(ydb_wrapper=None, *, table_config_key: str = TABLE_CONFIG_KEY, default: str = DEFAULT_TABLE_PATH) -> str:
    if ydb_wrapper is not None:
        try:
            return ydb_wrapper.get_table_path(table_config_key)
        except KeyError:
            pass
    return default


def _ydb_wrapper_cls():
    try:
        from ydb_wrapper import YDBWrapper

        return YDBWrapper
    except ImportError:
        qa_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "analytics"))
        if qa_dir not in sys.path:
            sys.path.insert(0, qa_dir)
        from ydb_wrapper import YDBWrapper

        return YDBWrapper


def _open_ydb_wrapper(factory: Optional[Callable] = None):
    resolved = factory or _ydb_wrapper_cls
    value = resolved() if callable(resolved) else resolved
    if isinstance(value, type):
        value = value()
    return value


def _ydb_primitive(sql_type: str):
    if ydb is None:
        raise RuntimeError("ydb SDK is required to flush analytics")
    return getattr(ydb.PrimitiveType, sql_type)


def build_column_types(columns: Optional[Sequence[Tuple[str, str, bool]]] = None):
    if ydb is None:
        raise RuntimeError("ydb SDK is required to flush analytics")
    columns = columns or COLUMNS_SCHEMA
    result = ydb.BulkUpsertColumns()
    for name, sql_type, _nullable in columns:
        result.add_column(name, ydb.OptionalType(_ydb_primitive(sql_type)))
    return result


def build_create_table_sql(
    table_path: str,
    *,
    columns: Optional[Sequence[Tuple[str, str, bool]]] = None,
    primary_keys: Optional[Sequence[str]] = None,
    ttl_minutes: int = TTL_MINUTES,
) -> str:
    columns = columns or COLUMNS_SCHEMA
    primary_keys = primary_keys or PRIMARY_KEYS
    col_defs = []
    for name, sql_type, nullable in columns:
        null_str = "" if nullable else " NOT NULL"
        col_defs.append(f"            `{name}` {sql_type}{null_str}")
    columns_sql = ",\n".join(col_defs)
    pk_sql = ", ".join(f"`{key}`" for key in primary_keys)
    return f"""
        CREATE TABLE IF NOT EXISTS `{table_path}` (
{columns_sql},
            PRIMARY KEY ({pk_sql})
        )
        PARTITION BY HASH(`date`)
        WITH (
            STORE = COLUMN,
            AUTO_PARTITIONING_BY_SIZE = ENABLED,
            AUTO_PARTITIONING_PARTITION_SIZE_MB = 2048,
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4,
            TTL = Interval("PT{ttl_minutes}M") ON event_ts
        )
    """
