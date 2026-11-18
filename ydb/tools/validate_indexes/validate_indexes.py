#!/usr/bin/env python3
"""Check a table against its ordinary secondary index implementation tables."""

import argparse
import decimal
import json
import math
import os
import posixpath
import struct
import sys
import tempfile
import time

import ydb
from ydb import _apis, issues


INDEX_TYPES = {"global_index", "global_async_index", "global_unique_index"}
FLOAT_FORMATS = {_apis.ydb_value.Type.FLOAT: "<f", _apis.ydb_value.Type.DOUBLE: "<d"}
DDL_WARNING = (
    "Do not run DDL on the table, its ancestors or indexes during validation. "
    "Wait for ongoing DDL to finish before starting."
)


def _quote(name):
    return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`"


def _describe(driver, session, path):
    # Preserve index types and covering columns omitted by the SDK.
    def unwrap(_, response):
        issues._process_response(response.operation)
        result = _apis.ydb_table.DescribeTableResult()
        response.operation.result.Unpack(result)
        return result

    return driver(
        _apis.ydb_table.DescribeTableRequest(session_id=session.session_id, path=path),
        _apis.TableService.Stub,
        _apis.TableService.DescribeTable,
        unwrap,
    )


class Table:
    def __init__(self, driver, session, path):
        self.path = path
        self.description = _describe(driver, session, path)
        self.primary_key = list(self.description.primary_key)
        self.columns = [column.name for column in self.description.columns]
        self.types = {column.name: column.type for column in self.description.columns}
        self.schema = self._schema(self.description)

    @staticmethod
    def _schema(description):
        created = description.self.created_at
        if not created.plan_step or not created.tx_id:
            raise ValueError("Table incarnation is unavailable in DescribeTable")
        schema = _apis.ydb_table.DescribeTableResult()
        # Exclude runtime metadata from schema comparison.
        schema.columns.extend(description.columns)
        schema.primary_key.extend(description.primary_key)
        schema.indexes.extend(description.indexes)
        schema.store_type = description.store_type
        for index in schema.indexes:
            index.ClearField("size_bytes")
        schema.columns.sort(key=lambda column: column.name)
        schema.indexes.sort(key=lambda index: index.name)
        return created.plan_step, created.tx_id, schema.SerializeToString(deterministic=True)

    def check_schema(self, driver, session):
        if self.schema != self._schema(_describe(driver, session, self.path)):
            raise ValueError(f"Schema changed for {self.path}; validation result discarded. Keep DDL excluded")

    def column_type(self, name):
        kind = self.types[name]
        return kind.optional_type.item if kind.HasField("optional_type") else kind

    def key(self, row):
        return {name: row[name] for name in self.primary_key}


def _query(table, indexes, columns):
    branches = []
    for position, (_, index, _) in enumerate(indexes):
        projection = []
        for number, name in enumerate(columns):
            expression = "NULL"
            if name in index.columns:
                expression = _quote(name)
                kind = table.column_type(name)
                if kind.HasField("pg_type"):
                    if kind.pg_type.type_name in ("pgbpchar", "bpchar"):
                        # Text casts trim bpchar trailing spaces.
                        expression = f"FromPg(Pg::bpcharout({expression}))"
                    else:
                        expression = f"FromPg(PgCast({expression}, PgText))"
                elif kind.type_id in FLOAT_FORMATS or kind.type_id in (
                    _apis.ydb_value.Type.JSON, _apis.ydb_value.Type.JSON_DOCUMENT, _apis.ydb_value.Type.YSON,
                ):
                    # Preserve signed zeros and NaN payloads.
                    expression = f"ToBytes({expression})"
            projection.append(f"{expression} AS c{number}")
        for source, delta in ((table, 1), (index, -1)):
            branches.append(
                f"SELECT {position}u AS index_id, {', '.join(projection)}, {delta} AS delta "
                f"FROM {_quote(source.path)}"
            )
    keys = "index_id, " + ", ".join(f"c{number}" for number in range(len(columns)))
    return (
        f"SELECT {keys}, SUM(delta) AS balance FROM (\n" + "\nUNION ALL\n".join(branches) +
        f"\n) AS entries GROUP BY {keys} HAVING SUM(delta) != 0;"
    )


def _read_differences(driver, table, indexes, columns):
    # Avoid Python datetime range limits.
    settings = (ydb.QueryClientSettings()
                .with_native_date_in_result_sets(False)
                .with_native_datetime_in_result_sets(False)
                .with_native_timestamp_in_result_sets(False)
                .with_native_interval_in_result_sets(False))
    with ydb.QuerySessionPool(driver, query_client_settings=settings) as pool:
        with pool.checkout() as session:
            # Async index tables require StaleRO.
            for asynchronous, mode in ((False, ydb.QuerySnapshotReadOnly()), (True, ydb.QueryStaleReadOnly())):
                positions = [position for position, (_, _, is_async) in enumerate(indexes) if is_async == asynchronous]
                if not positions:
                    continue
                selected = [indexes[position] for position in positions]
                stream = session.transaction(mode).execute(_query(table, selected, columns), commit_tx=True)
                try:
                    for result_set in stream:
                        if result_set.truncated:
                            raise ydb.TruncatedResponseError("Validation result was truncated by the server")
                        for row in result_set.rows:
                            yield positions[row["index_id"]], row
                finally:
                    stream.cancel()


def _difference(table, indexes, columns, position, result):
    name, index, asynchronous = indexes[position]
    row, comparison_bytes = {}, {}
    for number, column in enumerate(columns):
        value = result[f"c{number}"]
        fmt = FLOAT_FORMATS.get(table.column_type(column).type_id)
        if fmt and value is not None:
            comparison_bytes[column] = value.hex()
            value = struct.unpack(fmt, value)[0]
            if not math.isfinite(value):
                value = repr(value)
        row[column] = value
    details = {
        "index": name,
        "pk": table.key(row),
        "index_key": index.key(row),
        "reason": "missing index row" if result["balance"] > 0 else "extra index row",
        "row": {column: row[column] for column in index.columns},
    }
    if asynchronous:
        details["status"] = "inconclusive"
    if comparison_bytes:
        details["comparison_bytes"] = comparison_bytes
    return json.dumps(details, ensure_ascii=True, default=repr)


def _check_schema(driver, session, described):
    for source in described:
        source.check_schema(driver, session)


def _check_async(driver, session, table, indexes, columns, described, retries, delay):
    # Stale main-table reads can hide index corruption.
    for source in [table] + [index for _, index, _ in indexes]:
        if any(value for _, value in source.description.read_replicas_settings.ListFields()):
            print(f"Skipping async validation: read replicas on {source.path} prevent freshness confirmation")
            return False, [None] * len(indexes)
    clean_passes = 0
    with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as diagnostics:
        for attempt in range(1, retries + 2):
            _check_schema(driver, session, described)
            diagnostics.seek(0)
            diagnostics.truncate()
            counts = [0] * len(indexes)
            for position, result in _read_differences(driver, table, indexes, columns):
                counts[position] += 1
                diagnostics.write(_difference(table, indexes, columns, position, result) + "\n")
            _check_schema(driver, session, described)
            total = sum(counts)
            clean_passes = 0 if total else clean_passes + 1
            print(f"Async attempt {attempt}/{retries + 1}: mismatches: {total}; "
                  f"consecutive clean passes: {clean_passes}/2", flush=True)
            if clean_passes == 2:
                return True, counts
            if attempt <= retries:
                print(f"Retrying async validation in {delay:g}s", flush=True)
                time.sleep(delay)
        diagnostics.seek(0)
        for line in diagnostics:
            print(line, end="")
    print("Async retry budget exhausted; consistency could not be confirmed")
    return False, counts


def _validate(driver, session, path, writes_stopped, async_retries, async_retry_delay):
    table = Table(driver, session, path)
    indexes = []
    checked = []
    for description in table.description.indexes:
        kind = description.WhichOneof("type")
        if kind not in INDEX_TYPES:
            print(f"Skipping {description.name} ({kind or 'unknown index type'})")
            continue
        if description.status != _apis.ydb_table.TableIndexDescription.STATUS_READY:
            raise ValueError(f"Index {description.name} is not ready")
        indexes.append((description.name, Table(driver, session, f"{path}/{description.name}/indexImplTable"),
                        kind == "global_async_index"))
        if kind == "global_async_index" and not writes_stopped:
            print(f"Skipping {description.name} ({kind}): use --writes-stopped to check eventual consistency")
        else:
            checked.append(f"{description.name} ({kind})")
    print("Checked index set: " + (", ".join(checked) or "(none)"))
    described = [table] + [index for _, index, _ in indexes]
    _check_schema(driver, session, described)
    if not indexes:
        print("No supported indexes to validate")
        return True
    columns = sorted({name for _, index, _ in indexes for name in index.columns})
    synchronous = [index for index in indexes if not index[2]]
    asynchronous = [index for index in indexes if index[2]]
    mismatches = {name: None for name, _, _ in indexes}
    for name, _, _ in synchronous:
        mismatches[name] = 0
    if synchronous:
        for position, result in _read_differences(driver, table, synchronous, columns):
            name = synchronous[position][0]
            print(_difference(table, synchronous, columns, position, result))
            mismatches[name] += 1
    async_confirmed = False
    if asynchronous and writes_stopped:
        print("Writes stopped: asserted by the caller; checking async convergence", flush=True)
        async_confirmed, counts = _check_async(driver, session, table, asynchronous, columns, described,
                                              async_retries, async_retry_delay)
        mismatches.update((name, count) for (name, _, _), count in zip(asynchronous, counts))
    _check_schema(driver, session, described)
    consistent = True
    inconclusive = False
    for name, _, is_async in indexes:
        count = mismatches[name]
        if is_async and not async_confirmed:
            inconclusive = True
            status = "inconclusive"
        elif count:
            consistent = False
            status = "inconsistent"
        else:
            status = "consistent"
        print(f"{name}: {status} (mismatches: {count if count is not None else 'not checked'})")
    status = "inconsistent" if not consistent else ("inconclusive" if inconclusive else "consistent")
    print(f"{path}: {status}")
    return False if not consistent else (None if inconclusive else True)


def validate(driver, session, path, *, writes_stopped=False, async_retries=20, async_retry_delay=30):
    if not isinstance(async_retries, int) or async_retries < 1:
        raise ValueError("async_retries must be an integer of at least 1")
    if not math.isfinite(async_retry_delay) or async_retry_delay <= 0:
        raise ValueError("async_retry_delay must be a finite positive number")
    print(f"WARNING: {DDL_WARNING}", file=sys.stderr)
    # YDB Decimal precision exceeds Python's default.
    with decimal.localcontext() as context:
        context.prec = max(context.prec, 35)
        return _validate(driver, session, path, writes_stopped, async_retries, async_retry_delay)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, epilog=f"WARNING: {DDL_WARNING}")
    parser.add_argument("-e", "--endpoint", required=True, help="YDB gRPC endpoint")
    parser.add_argument("-d", "--database", required=True, help="Database path")
    parser.add_argument("-t", "--table", required=True, help="Absolute table path or path relative to the database")
    parser.add_argument("--writes-stopped", action="store_true",
                        help="Assert all writes have stopped until exit; wait for async index convergence")
    parser.add_argument("--async-retries", type=int, default=20, help="Async retries after the first pass (default: 20)")
    parser.add_argument("--async-retry-delay", type=float, default=30,
                        help="Seconds between complete async passes (default: 30)")
    args = parser.parse_args(argv)
    if args.async_retries < 1 or not math.isfinite(args.async_retry_delay) or args.async_retry_delay <= 0:
        parser.error("async retries must be at least 1 and async retry delay must be finite and positive")
    path = posixpath.normpath(posixpath.join(args.database, args.table))
    try:
        credential_vars = (
            "YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS", "YDB_USER", "YDB_ANONYMOUS_CREDENTIALS",
            "YDB_METADATA_CREDENTIALS", "YDB_ACCESS_TOKEN_CREDENTIALS", "YDB_OAUTH2_KEY_FILE",
        )
        credentials = ydb.credentials_from_env_variables() if any(name in os.environ for name in credential_vars) else None
        with ydb.Driver(endpoint=args.endpoint, database=args.database, credentials=credentials) as driver:
            driver.wait(timeout=30, fail_fast=True)
            with ydb.SessionPool(driver) as pool:
                with pool.checkout() as session:
                    result = validate(driver, session, path, writes_stopped=args.writes_stopped,
                                      async_retries=args.async_retries, async_retry_delay=args.async_retry_delay)
                    return 3 if result is None else (0 if result else 1)
    except (ydb.Error, ValueError, RuntimeError, OSError) as error:
        print(f"Validation failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
