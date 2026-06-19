"""Shared window antijoin cleanup for analytics marts and upload scripts."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Sequence, Tuple

import ydb

from ydb_wrapper import YDBWrapper


def validate_identifier(value: str, arg_name: str) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value or ""):
        raise ValueError(
            f"Invalid {arg_name}: {value!r}. Allowed pattern: [A-Za-z_][A-Za-z0-9_]*"
        )


def validate_interval_expression(interval_expr: str) -> None:
    # Example: 365 * Interval("P1D")
    if not re.fullmatch(r'\d+\s*\*\s*Interval\("P[0-9A-Z]+"\)', interval_expr or ""):
        raise ValueError(
            f"Invalid cleanup window interval: {interval_expr!r}. "
            'Expected format like: 365 * Interval("P1D")'
        )


def normalize_pk_value(value: Any) -> Any:
    """Normalize YDB scan values for PK comparison (Utf8 often arrives as bytes)."""
    if isinstance(value, bytes):
        return value.decode('utf-8', errors='replace')
    return value


def pk_tuple(row: Dict[str, Any], primary_keys: Sequence[str]) -> Tuple[Any, ...]:
    return tuple(normalize_pk_value(row[key]) for key in primary_keys)


def to_typed_param(value: Any, type_name: str) -> Any:
    primitive = getattr(ydb.PrimitiveType, type_name, None)
    if primitive is None:
        return value
    return ydb.TypedValue(value=value, value_type=primitive)


def delete_stale_pk_rows(
    ydb_wrapper: YDBWrapper,
    table_path: str,
    primary_keys: Sequence[str],
    pk_type_map: Dict[str, str],
    stale_rows: Sequence[Dict[str, Any]],
    query_name_prefix: str | None = None,
) -> None:
    if not stale_rows:
        print("Cleanup mode=window_antijoin: no stale rows to delete")
        return

    prefix = query_name_prefix or table_path.split('/')[-1]
    print(f"Cleanup mode=window_antijoin: deleting stale rows: {len(stale_rows)}")
    preview_limit = 10
    chunk_size = 100
    total = len(stale_rows)
    for idx, row in enumerate(stale_rows, 1):
        if idx <= preview_limit:
            preview = ", ".join([f"{key}={row[key]!r}" for key in primary_keys])
            print(f"Stale row {idx}/{total}: {preview}")
        if idx == preview_limit + 1:
            print("Stale row preview limit reached; continuing without per-row logging")

    for start_idx in range(0, total, chunk_size):
        chunk = stale_rows[start_idx:start_idx + chunk_size]
        chunk_no = start_idx // chunk_size + 1
        declare_lines: List[str] = []
        predicates: List[str] = []
        params: Dict[str, Any] = {}

        for row_idx, row in enumerate(chunk):
            row_param_names: List[str] = []
            for key in primary_keys:
                param_name = f"${key}_{row_idx}"
                declare_lines.append(f"DECLARE {param_name} AS {pk_type_map[key]};")
                params[param_name] = to_typed_param(normalize_pk_value(row[key]), pk_type_map[key])
                row_param_names.append(param_name)
            predicates.append(
                "(" + " AND ".join(
                    [f"`{key}` = {row_param_names[key_idx]}" for key_idx, key in enumerate(primary_keys)]
                ) + ")"
            )

        declare_block = "\n".join(declare_lines)
        delete_query = f"""
            {declare_block}

            DELETE FROM `{table_path}`
            WHERE {' OR '.join(predicates)};
        """

        ydb_wrapper.execute_dml(
            delete_query,
            parameters=params,
            query_name=f"{prefix}_cleanup_stale_pk_chunk_{chunk_no}",
        )

        deleted = min(start_idx + chunk_size, total)
        print(f"Deleted stale rows: {deleted}/{total} (chunks: {chunk_no})")


def cleanup_window_antijoin(
    ydb_wrapper: YDBWrapper,
    table_path: str,
    source_rows: Sequence[Dict[str, Any]],
    primary_keys: Sequence[str],
    pk_type_map: Dict[str, str],
    window_predicate: str,
    query_name: str,
) -> None:
    """Delete table rows in window that are absent from source_rows (by primary key)."""
    for key in primary_keys:
        validate_identifier(key, "primary_keys")

    target_pk_projection = ", ".join([f"t.`{key}` AS `{key}`" for key in primary_keys])
    target_pk_query = f"""
        SELECT {target_pk_projection}
        FROM `{table_path}` AS t
        WHERE {window_predicate};
    """

    target_pk_rows = ydb_wrapper.execute_scan_query(
        target_pk_query, f"{query_name}_cleanup_target_pks"
    )

    source_pk_set = {pk_tuple(row, primary_keys) for row in source_rows}
    stale_rows = [
        row for row in target_pk_rows
        if pk_tuple(row, primary_keys) not in source_pk_set
    ]
    delete_stale_pk_rows(
        ydb_wrapper, table_path, primary_keys, pk_type_map, stale_rows, query_name_prefix=query_name
    )


def cleanup_window_antijoin_by_date_key(
    ydb_wrapper: YDBWrapper,
    table_path: str,
    source_rows: Sequence[Dict[str, Any]],
    primary_keys: Sequence[str],
    pk_type_map: Dict[str, str],
    window_key: str,
    window_interval: str,
    query_name: str,
) -> None:
    """Date-key variant used by data_mart_executor (CurrentUtcDate() window)."""
    validate_identifier(window_key, "window_key")
    validate_interval_expression(window_interval)
    window_predicate = f"t.`{window_key}` >= CurrentUtcDate() - {window_interval}"
    cleanup_window_antijoin(
        ydb_wrapper,
        table_path,
        source_rows,
        primary_keys,
        pk_type_map,
        window_predicate,
        query_name,
    )
