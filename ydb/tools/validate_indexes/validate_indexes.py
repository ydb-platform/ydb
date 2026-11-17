#!/usr/bin/env python3
"""Check a table against its ordinary secondary index implementation tables."""

import argparse
import decimal
import json
import math
import os
import posixpath
import sys

import ydb
from ydb import _apis, issues


INDEX_TYPES = {"global_index", "global_async_index", "global_unique_index"}


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
        types = {column.name: column.type for column in self.description.columns}
        self.key_type = _apis.ydb_value.Type()
        for name in self.primary_key:
            self.key_type.tuple_type.elements.add().CopyFrom(types[name])

    def key(self, row):
        return {name: row[name] for name in self.primary_key}

    def rows(self, session, **kwargs):
        stream = session.read_table(self.path, use_snapshot=True, **kwargs)
        try:
            for part in stream:
                yield from part.rows
        finally:
            stream.cancel()

    def point(self, session, row, columns):
        bound = ydb.KeyBound.inclusive([row[name] for name in self.primary_key], self.key_type)
        rows = list(self.rows(session, key_range=ydb.KeyRange(bound, bound), row_limit=1, columns=columns))
        return rows[0] if rows else None


def _equal(left, right):
    if isinstance(left, decimal.Decimal) and isinstance(right, decimal.Decimal) and left.is_nan() and right.is_nan():
        return True
    if isinstance(left, float) and isinstance(right, float) and math.isnan(left) and math.isnan(right):
        return True
    return left == right


def _check_index(session, table, index, name):
    mismatches = 0
    counts = []
    for source, target in ((table, index), (index, table)):
        count = 0
        for key in source.rows(session, columns=source.primary_key):
            count += 1
            row = source.point(session, key, index.columns)
            if row is None:
                raise ValueError(f"{source.path}: row disappeared during validation; stop writes and retry")
            other = target.point(session, row, index.columns)
            details = {
                "index": name,
                "pk": table.key(row),
                "index_key": index.key(row),
            }
            if other is None:
                details["reason"] = "missing index row" if source is table else "extra index row"
            else:
                table_row, index_row = (row, other) if source is table else (other, row)
                differences = {
                    column: {"table": table_row[column], "index": index_row[column]}
                    for column in index.columns
                    if not _equal(row[column], other[column])
                }
                if not differences:
                    continue
                details["reason"] = "values differ"
                details["columns"] = differences
            mismatches += 1
            print(json.dumps(details, ensure_ascii=True, default=repr))
        counts.append(count)
    status = "inconsistent" if mismatches else "consistent"
    print(f"{name}: {status} (table rows: {counts[0]}, index rows: {counts[1]}, mismatches: {mismatches})")
    return mismatches == 0


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
        indexes.append((description.name, Table(driver, session, f"{path}/{description.name}/indexImplTable")))
    if not indexes:
        print("No supported indexes to validate")
        return True
    consistent = True
    for name, index in indexes:
        if not _check_index(session, table, index, name):
            consistent = False
    print(f"{path}: {'consistent' if consistent else 'inconsistent'}")
    return consistent


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
                    return 0 if validate(driver, session, path) else 1
    except (ydb.Error, ValueError, RuntimeError) as error:
        print(f"Validation failed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
