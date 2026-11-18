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

import ydb
from ydb import _apis, issues


INDEX_TYPES = {"global_index", "global_async_index", "global_unique_index"}
FLOAT_FORMATS = {_apis.ydb_value.Type.FLOAT: "<f", _apis.ydb_value.Type.DOUBLE: "<d"}


def _quote(name):
    return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`"


def _describe(driver, session, path):
    # The SDK's TableSchemeEntry drops index types and covering columns.
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
        # Keep comparison-relevant definitions, excluding live statistics,
        # partitioning and changefeed scan progress/state.
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
                    # The Python SDK cannot decode PostgreSQL result columns.
                    if kind.pg_type.type_name in ("pgbpchar", "bpchar"):
                        # Casting char(n) to text would trim stored trailing spaces.
                        expression = f"FromPg(Pg::bpcharout({expression}))"
                    else:
                        expression = f"FromPg(PgCast({expression}, PgText))"
                elif kind.type_id in FLOAT_FORMATS or kind.type_id in (
                    _apis.ydb_value.Type.JSON, _apis.ydb_value.Type.JSON_DOCUMENT, _apis.ydb_value.Type.YSON,
                ):
                    # Float grouping merges signed zeros and NaNs. Compare their
                    # exact bits; non-comparable payload types also need bytes.
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
    # Python datetime cannot represent the full range of YDB temporal types.
    settings = (ydb.QueryClientSettings()
                .with_native_date_in_result_sets(False)
                .with_native_datetime_in_result_sets(False)
                .with_native_timestamp_in_result_sets(False)
                .with_native_interval_in_result_sets(False))
    with ydb.QuerySessionPool(driver, query_client_settings=settings) as pool:
        with pool.checkout() as session:
            # Async implementation tables only support StaleRO. Keep them out
            # of the one shared snapshot used for every synchronous index.
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
                    # Never resume an interrupted query on a different snapshot.
                    stream.cancel()


def _validate(driver, session, path):
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
        checked.append(f"{description.name} ({kind})")
    print("Checked index set: " + (", ".join(checked) or "(none)"))
    print("DDL exclusion: supplied externally by the caller")
    described = [table] + [index for _, index, _ in indexes]
    for source in described:
        source.check_schema(driver, session)
    if not indexes:
        print("No supported indexes to validate")
        return True
    columns = sorted({name for _, index, _ in indexes for name in index.columns})
    mismatches = [0] * len(indexes)
    for position, result in _read_differences(driver, table, indexes, columns):
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
        print(json.dumps(details, ensure_ascii=True, default=repr))
        mismatches[position] += 1
    # Describes cannot prove the absence of transient DDL. External exclusion
    # is mandatory; these checks additionally reject observed schema changes.
    for source in described:
        source.check_schema(driver, session)
    consistent = True
    inconclusive = False
    for (name, _, asynchronous), count in zip(indexes, mismatches):
        if asynchronous:
            inconclusive = True
            print(f"{name}: async replication may lag; consistency could not be confirmed")
        elif count:
            consistent = False
        status = "inconclusive" if asynchronous else ("inconsistent" if count else "consistent")
        print(f"{name}: {status} (mismatches: {count})")
    status = "inconsistent" if not consistent else ("inconclusive" if inconclusive else "consistent")
    print(f"{path}: {status}")
    return False if not consistent else (None if inconclusive else True)


def validate(driver, session, path, *, ddl_excluded=False):
    if not ddl_excluded:
        raise ValueError("DDL exclusion is required from before discovery until validation completes")
    # SDK Decimal decoding divides by the scale using Python's current precision.
    # YDB supports up to 35 digits, exceeding Python's default precision of 28.
    with decimal.localcontext() as context:
        context.prec = max(context.prec, 35)
        return _validate(driver, session, path)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-e", "--endpoint", required=True, help="YDB gRPC endpoint")
    parser.add_argument("-d", "--database", required=True, help="Database path")
    parser.add_argument("-t", "--table", required=True, help="Absolute table path or path relative to the database")
    parser.add_argument("--ddl-excluded", action="store_true", help="Assert an external DDL exclusion mechanism is held")
    args = parser.parse_args(argv)
    if not args.ddl_excluded:
        parser.error("DDL exclusion is required; use --ddl-excluded only while an external DDL exclusion mechanism is held")
    path = posixpath.normpath(posixpath.join(args.database, args.table))
    try:
        # Use anonymous auth locally; otherwise follow the SDK credential environment.
        credential_vars = (
            "YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS", "YDB_USER", "YDB_ANONYMOUS_CREDENTIALS",
            "YDB_METADATA_CREDENTIALS", "YDB_ACCESS_TOKEN_CREDENTIALS", "YDB_OAUTH2_KEY_FILE",
        )
        credentials = ydb.credentials_from_env_variables() if any(name in os.environ for name in credential_vars) else None
        with ydb.Driver(endpoint=args.endpoint, database=args.database, credentials=credentials) as driver:
            driver.wait(timeout=30, fail_fast=True)
            with ydb.SessionPool(driver) as pool:
                with pool.checkout() as session:
                    result = validate(driver, session, path, ddl_excluded=args.ddl_excluded)
                    return 3 if result is None else (0 if result else 1)
    except (ydb.Error, ValueError, RuntimeError) as error:
        print(f"Validation failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
