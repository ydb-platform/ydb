#!/usr/bin/env python3

#--query_path .github/scripts/analytics/data_mart_queries/perfomance_olap_mart.sql --table_path perfomance/olap/fast_results --store_type column --partition_keys Run_start_timestamp --primary_keys Db Suite Test Branch Run_start_timestamp --ttl_min 43200 --ttl_key Run_start_timestamp
import argparse
import json
import ydb
import os
import re
import time
from ydb_wrapper import YDBWrapper

# Get repository path
dir = os.path.dirname(__file__)
repo_path = os.path.abspath(f"{dir}/../../../")
MUTE_CONFIG_PATH = os.path.join(
    repo_path, ".github", "config", "mute_config.json"
)


def load_query_variables():
    variables = {}
    with open(MUTE_CONFIG_PATH, "r", encoding="utf-8") as config_file:
        config = json.load(config_file)
    observation_window_days = int(config["observation_window_days"])
    if observation_window_days <= 0:
        raise ValueError("observation_window_days must be a positive integer")
    variables["observation_window_days"] = observation_window_days
    return variables


def render_query_template(sql_query: str, variables: dict) -> str:
    rendered = sql_query
    for variable_name, variable_value in variables.items():
        rendered = rendered.replace(f"{{{variable_name}}}", str(variable_value))
    return rendered

def ydb_type_to_str(ydb_type, store_type = 'ROW'):
    # Converts YDB type to string representation for table creation
    is_optional = False
    if ydb_type.HasField('optional_type'):
        is_optional = True
        base_type = ydb_type.optional_type.item
    else:
        base_type = ydb_type

    for type in ydb.PrimitiveType:
        if type.proto.type_id == base_type.type_id:
            break
    if is_optional:
        result_type = ydb.OptionalType(type)
        name = result_type._repr
    else:
        result_type = type
        name = result_type.name

    if name.upper() == 'BOOL' and store_type.upper() == 'COLUMN':
        if is_optional:
            result_type = ydb.OptionalType(ydb.PrimitiveType.Uint8)
        else:
            result_type = ydb.PrimitiveType.Uint8
        name = 'Uint8'
    return result_type, name

def create_table(ydb_wrapper, table_path, column_types, store_type, partition_keys, primary_keys, ttl_min, ttl_key):
    """Create table using ydb_wrapper based on the structure of the provided column types."""
    if not column_types:
        raise ValueError("No column types to create table from.")

    columns_sql = []
    for column_name, column_ydb_type in column_types:
        column_type_obj, column_type_str = ydb_type_to_str(column_ydb_type, store_type.upper())
        if column_name in primary_keys:
            columns_sql.append(f"`{column_name}` {column_type_str.replace('?','')} NOT NULL")
        else:
            columns_sql.append(f"`{column_name}` {column_type_str.replace('?','')}")

    partition_keys_sql = ", ".join([f"`{key}`" for key in partition_keys])
    primary_keys_sql = ", ".join([f"`{key}`" for key in primary_keys])
    
    # Добавляем TTL только если оба аргумента заданы
    ttl_clause = ""
    if ttl_min and ttl_key:
        ttl_clause = f' TTL = Interval("PT{ttl_min}M") ON {ttl_key}'

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_path}` (
        {', '.join(columns_sql)},
        PRIMARY KEY ({primary_keys_sql})
    )
    PARTITION BY HASH({partition_keys_sql})
    WITH (
       {"STORE = COLUMN" if store_type.upper() == 'COLUMN' else ''}
        {',' if store_type and  ttl_clause else ''}
        {ttl_clause}
    )
    """

    ydb_wrapper.create_table(table_path, create_table_sql)
def create_table_if_not_exists(ydb_wrapper, table_path, column_types, store_type, partition_keys, primary_keys, ttl_min, ttl_key):
    """Create table if it does not already exist, using ydb_wrapper."""
    # For now, we'll always try to create the table
    # In a more sophisticated implementation, we could check if table exists first
    create_table(ydb_wrapper, table_path, column_types, store_type, partition_keys, primary_keys, ttl_min, ttl_key)


def _validate_identifier(value: str, arg_name: str) -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value or ""):
        raise ValueError(
            f"Invalid {arg_name}: {value!r}. Allowed pattern: [A-Za-z_][A-Za-z0-9_]*"
        )


def _validate_interval_expression(interval_expr: str) -> None:
    # Example: 365 * Interval("P1D")
    if not re.fullmatch(r'\d+\s*\*\s*Interval\("P[0-9A-Z]+"\)', interval_expr or ""):
        raise ValueError(
            f"Invalid --cleanup_window_interval: {interval_expr!r}. "
            'Expected format like: 365 * Interval("P1D")'
        )


def _type_name_for_declare(column_type):
    _, type_name = ydb_type_to_str(column_type, "ROW")
    return type_name.replace("?", "")


def _pk_tuple(row, primary_keys):
    return tuple(row[key] for key in primary_keys)


def _collect_pk_type_map(primary_keys, column_types):
    column_types_map = {name: ctype for name, ctype in column_types}
    missing = [key for key in primary_keys if key not in column_types_map]
    if missing:
        raise ValueError(f"Primary keys not found in query result columns: {missing}")
    return {key: _type_name_for_declare(column_types_map[key]) for key in primary_keys}


def _to_typed_param(value, type_name):
    primitive = getattr(ydb.PrimitiveType, type_name, None)
    if primitive is None:
        # Fallback: let SDK infer type when primitive mapping is unavailable.
        return value
    return ydb.TypedValue(value=value, value_type=primitive)


def _delete_stale_pk_rows(ydb_wrapper, table_path, primary_keys, pk_type_map, stale_rows):
    if not stale_rows:
        print("Cleanup mode=window_antijoin: no stale rows to delete")
        return

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
        declare_lines = []
        predicates = []
        params = {}

        for row_idx, row in enumerate(chunk):
            row_param_names = []
            for key in primary_keys:
                param_name = f"${key}_{row_idx}"
                declare_lines.append(f"DECLARE {param_name} AS {pk_type_map[key]};")
                params[param_name] = _to_typed_param(row[key], pk_type_map[key])
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
            query_name=f"{table_path.split('/')[-1]}_cleanup_stale_pk_chunk_{chunk_no}",
        )

        deleted = min(start_idx + chunk_size, total)
        print(f"Deleted stale rows: {deleted}/{total} (chunks: {chunk_no})")


def _cleanup_window_antijoin(
    ydb_wrapper,
    table_path,
    results,
    primary_keys,
    column_types,
    window_key,
    window_interval,
    query_name,
):
    target_pk_projection = ", ".join([f"t.`{key}` AS `{key}`" for key in primary_keys])

    target_pk_query = f"""
        SELECT {target_pk_projection}
        FROM `{table_path}` AS t
        WHERE t.`{window_key}` >= CurrentUtcDate() - {window_interval};
    """

    target_pk_rows, _ = ydb_wrapper.execute_scan_query_with_metadata(
        target_pk_query, f"{query_name}_cleanup_target_pks"
    )

    source_pk_set = {_pk_tuple(row, primary_keys) for row in results}
    stale_rows = [
        row for row in target_pk_rows
        if _pk_tuple(row, primary_keys) not in source_pk_set
    ]
    pk_type_map = _collect_pk_type_map(primary_keys, column_types)
    _delete_stale_pk_rows(ydb_wrapper, table_path, primary_keys, pk_type_map, stale_rows)


def cleanup_after_upsert(
    ydb_wrapper,
    table_path,
    results,
    primary_keys,
    column_types,
    window_key,
    window_interval,
    query_name,
):
    _validate_identifier(window_key, "--cleanup_window_key")
    _validate_interval_expression(window_interval)
    for key in primary_keys:
        _validate_identifier(key, "--primary_keys")
    _cleanup_window_antijoin(
        ydb_wrapper,
        table_path,
        results,
        primary_keys,
        column_types,
        window_key,
        window_interval,
        query_name,
    )

def parse_args():
    parser = argparse.ArgumentParser(description="YDB Table Manager")
    parser.add_argument("--table_path", required=True, help="Table path and name")
    parser.add_argument("--query_path", required=True, help="Path to the SQL query file")
    parser.add_argument("--store_type", choices=["column", "row"], required=True, help="Table store type (column or row)")
    parser.add_argument("--partition_keys", nargs="+", required=True, help="List of partition keys")
    parser.add_argument("--primary_keys", nargs="+", required=True, help="List of primary keys")
    parser.add_argument("--ttl_min", type=int, help="TTL in minutes")
    parser.add_argument("--ttl_key", help="TTL key column name")
    parser.add_argument(
        "--cleanup_window_key",
        help="Optional: Date key column used for cleanup before upsert.",
    )
    parser.add_argument(
        "--cleanup_window_interval",
        help='Optional: interval expression for cleanup window, e.g. 365 * Interval("P1D").',
    )
    return parser.parse_args()

def main():
    args = parse_args()

    table_path = args.table_path
    batch_size = 1000
    
    # Extract query_name from table_path (last part after /)
    query_name = table_path.split('/')[-1]
    
    # Read SQL query from file
    sql_query_path = os.path.join(repo_path, args.query_path)
    print(f'Query found: {sql_query_path}')

    with YDBWrapper() as ydb_wrapper:
        # Check credentials
        if not ydb_wrapper.check_credentials():
            return 1

        # Read SQL query from file
        with open(sql_query_path, 'r') as file:
            sql_query = file.read()
        query_variables = load_query_variables()
        sql_query = render_query_template(sql_query, query_variables)

        # Run query to get sample data and column types
        results, column_types = ydb_wrapper.execute_scan_query_with_metadata(sql_query, query_name)
        if not results:
            print("No data to create table from.")
            return

        create_table_if_not_exists(
            ydb_wrapper, table_path, column_types, args.store_type,
            args.partition_keys, args.primary_keys, args.ttl_min, args.ttl_key
        )

        print(f'Preparing to upsert: {len(results)} rows')
        
        # Подготавливаем column_types_map один раз
        column_types_map = ydb.BulkUpsertColumns()
        for column_name, column_ydb_type in column_types:
            column_type_obj, column_type_str = ydb_type_to_str(column_ydb_type, args.store_type.upper())
            column_types_map.add_column(column_name, column_type_obj)

        cleanup_args_set = any([args.cleanup_window_key, args.cleanup_window_interval])
        if cleanup_args_set:
            if not args.cleanup_window_key or not args.cleanup_window_interval:
                raise ValueError(
                    "Cleanup mode requires both --cleanup_window_key and --cleanup_window_interval"
                )
        ydb_wrapper.bulk_upsert_batches(table_path, results, column_types_map, batch_size, query_name)

        if cleanup_args_set:
            cleanup_after_upsert(
                ydb_wrapper=ydb_wrapper,
                table_path=table_path,
                results=results,
                primary_keys=args.primary_keys,
                column_types=column_types,
                window_key=args.cleanup_window_key,
                window_interval=args.cleanup_window_interval,
                query_name=query_name,
            )
        
        print('Data uploaded')


if __name__ == "__main__":
    main()

