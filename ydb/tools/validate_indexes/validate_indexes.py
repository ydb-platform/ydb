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
from ydb import _apis, convert, issues


INDEX_TYPES = {"global_index", "global_async_index", "global_unique_index"}
PAGE_SIZE = 64


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
        self.key_type = _apis.ydb_value.Type()
        for name in self.primary_key:
            self.key_type.tuple_type.elements.add().CopyFrom(self.types[name])

    def key(self, row):
        return {name: row[name] for name in self.primary_key}

    def _select(self, tx, columns, key=None, after=False, limit=PAGE_SIZE):
        predicates, equal_prefix, parameters, parameter_types = [], [], {}, {}
        if key is not None:
            for position, name in enumerate(self.primary_key):
                column = _quote(name)
                value = key[name]
                if value is None:
                    equal, greater = f"{column} IS NULL", f"{column} IS NOT NULL"
                elif isinstance(value, decimal.Decimal) and value.is_nan():
                    # Storage orders Decimal NaN last, but SQL = and > never match it.
                    equal, greater = f"{column} != {column}", "FALSE"
                else:
                    parameter = f"$key{position}"
                    parameters[parameter] = value
                    parameter_types[parameter] = self.types[name]
                    equal, greater = f"{column} = {parameter}", f"{column} > {parameter}"
                    kind = self.types[name]
                    if kind.HasField("optional_type"):
                        kind = kind.optional_type.item
                    if kind.HasField("decimal_type"):
                        greater = f"({greater} OR {column} != {column})"
                if after:
                    predicates.append("(" + " AND ".join(equal_prefix + [greater]) + ")")
                equal_prefix.append(equal)
        predicate = " OR ".join(predicates) if after else " AND ".join(equal_prefix)
        declarations = "\n".join(
            f"DECLARE {name} AS {convert.type_to_native(kind)};" for name, kind in parameter_types.items()
        )
        query = f"{declarations}\nSELECT {', '.join(map(_quote, columns))} FROM {_quote(self.path)}"
        if predicate:
            query += " WHERE " + predicate
        query += f" ORDER BY {', '.join(map(_quote, self.primary_key))} LIMIT {limit};"
        previous_id = tx.tx_id
        result = tx.execute(ydb.DataQuery(query, parameter_types), parameters)
        if not tx.tx_id or (previous_id and previous_id != tx.tx_id):
            raise RuntimeError("Validation snapshot was lost")
        return result[0].rows

    def rows(self, session, tx=None, **kwargs):
        if tx is not None:
            columns = kwargs.get("columns", self.columns)
            last = None
            while True:
                page = self._select(tx, columns, last, after=last is not None)
                if not page:
                    return
                yield from page
                last = self.key(page[-1])
        stream = session.read_table(self.path, use_snapshot=True, **kwargs)
        try:
            for part in stream:
                yield from part.rows
        finally:
            stream.cancel()

    def point(self, session, row, columns, tx=None):
        if tx is not None:
            rows = self._select(tx, columns, row, limit=1)
        else:
            bound = ydb.KeyBound.inclusive([row[name] for name in self.primary_key], self.key_type)
            rows = list(self.rows(session, key_range=ydb.KeyRange(bound, bound), row_limit=1, columns=columns))
        return rows[0] if rows else None


def _equal(left, right):
    if isinstance(left, decimal.Decimal) and isinstance(right, decimal.Decimal) and left.is_nan() and right.is_nan():
        return True
    if isinstance(left, float) and isinstance(right, float) and math.isnan(left) and math.isnan(right):
        return True
    return left == right


def _check_index(session, table, index, name, tx=None):
    mismatches = 0
    counts = []
    for source, target in ((table, index), (index, table)):
        count = 0
        for key in source.rows(session, tx=tx, columns=source.primary_key):
            count += 1
            row = source.point(session, key, index.columns, tx=tx)
            if row is None:
                if tx is not None:
                    raise RuntimeError(f"{source.path}: row disappeared from validation snapshot")
                # Async observations have independent snapshots; deletion is expected under writes.
                continue
            other = target.point(session, row, index.columns, tx=tx)
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
    status = "inconclusive" if tx is None else ("inconsistent" if mismatches else "consistent")
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
        indexes.append((description.name, Table(driver, session, f"{path}/{description.name}/indexImplTable"),
                        kind == "global_async_index"))
    if not indexes:
        print("No supported indexes to validate")
        return True
    consistent = True
    # All sync indexes, key pages and point reads share this transaction. Never
    # retry an individual statement in a new transaction after snapshot loss.
    with session.transaction(ydb.SnapshotReadOnly(), allow_split_transactions=False) as tx:
        try:
            for name, index, asynchronous in indexes:
                if not asynchronous and not _check_index(session, table, index, name, tx=tx):
                    consistent = False
        finally:
            if tx.tx_id:
                tx.rollback()
    inconclusive = False
    for name, index, asynchronous in indexes:
        if asynchronous:
            inconclusive = True
            print(f"{name}: async observations cannot confirm atomic consistency; replication may lag")
            _check_index(session, table, index, name)
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
