#!/usr/bin/env python3
"""Check a table against its ordinary secondary index implementation tables."""

import argparse
import decimal
import json
import os
import posixpath
import sys

import ydb
from ydb import _apis, issues


INDEX_TYPES = {"global_index", "global_async_index", "global_unique_index"}


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
                kind = table.types[name]
                if kind.HasField("optional_type"):
                    kind = kind.optional_type.item
                if kind.HasField("pg_type"):
                    # The Python SDK cannot decode PostgreSQL result columns.
                    if kind.pg_type.type_name in ("pgbpchar", "bpchar"):
                        # Casting char(n) to text would trim stored trailing spaces.
                        expression = f"FromPg(Pg::bpcharout({expression}))"
                    else:
                        expression = f"FromPg(PgCast({expression}, PgText))"
                elif kind.type_id in (_apis.ydb_value.Type.JSON, _apis.ydb_value.Type.JSON_DOCUMENT,
                                      _apis.ydb_value.Type.YSON):
                    # Payload types cannot be GROUP BY keys; compare their stored bytes.
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


def _validate(driver, session, path):
    table = Table(driver, session, path)
    indexes = []
    for description in table.description.indexes:
        kind = description.WhichOneof("type")
        if kind not in INDEX_TYPES:
            print(f"Skipping {description.name} ({kind or 'unknown index type'})")
            continue
        if description.status != _apis.ydb_table.TableIndexDescription.STATUS_READY:
            raise ValueError(f"Index {description.name} is not ready")
        indexes.append((description.name, Table(driver, session, f"{path}/{description.name}/indexImplTable"),
                        kind == "global_async_index"))
    if not indexes:
        print("No supported indexes to validate")
        return True
    columns = sorted({name for _, index, _ in indexes for name in index.columns})
    mismatches = [0] * len(indexes)
    # One scan snapshot covers every sync comparison. Async updates can arrive
    # with older MVCC versions, so even matching async observations are inconclusive.
    # Consume the entire stream before declaring success; do not retry partial scans.
    stream = driver.table_client.scan_query(_query(table, indexes, columns))
    try:
        for part in stream:
            if part.result_set.truncated:
                raise ydb.TruncatedResponseError("Validation result was truncated by the server")
            for result in part.result_set.rows:
                position = result["index_id"]
                name, index, asynchronous = indexes[position]
                row = {column: result[f"c{number}"] for number, column in enumerate(columns)}
                details = {
                    "index": name,
                    "pk": table.key(row),
                    "index_key": index.key(row),
                    "reason": "missing index row" if result["balance"] > 0 else "extra index row",
                    "row": {column: row[column] for column in index.columns},
                }
                if asynchronous:
                    details["status"] = "inconclusive"
                print(json.dumps(details, ensure_ascii=True, default=repr))
                mismatches[position] += 1
    finally:
        stream.cancel()
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


def validate(driver, session, path):
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
    args = parser.parse_args(argv)
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
                    result = validate(driver, session, path)
                    return 3 if result is None else (0 if result else 1)
    except (ydb.Error, ValueError, RuntimeError) as error:
        print(f"Validation failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
