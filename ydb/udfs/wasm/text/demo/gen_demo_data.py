#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Load demo tables of ASCII text for the Text WASM UDF.

  text_200kb / text_1mb / text_2mb
      (id Uint64, size_bytes Uint64, txt String)  — 1000 rows each

The measured queries scan `txt` in the same stage as Text::* so PreferWasm
marks the column. One tiled ASCII buffer is built once; each row is a
byte-shifted slice so the counters differ without generating 3 GB of unique
payload in RAM.
"""

from __future__ import annotations

import argparse
import os
import random
import sys
import time

KIB = 1024
MIB = 1024 * 1024

TABLES = (
    ("text_200kb", 200 * KIB),
    ("text_1mb", 1 * MIB),
    ("text_2mb", 2 * MIB),
)

UPPER = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
LOWER = b"abcdefghijklmnopqrstuvwxyz"
DIGITS = b"0123456789"
OTHER = b" \t.,!?;:-_/#@$%&*()[]{}+=<>|'\""


def count_letters(data: bytes) -> int:
    return sum(1 for c in data if (65 <= c <= 90) or (97 <= c <= 122))


def count_digits(data: bytes) -> int:
    return sum(1 for c in data if 48 <= c <= 57)


def count_upper(data: bytes) -> int:
    return sum(1 for c in data if 65 <= c <= 90)


def make_source(n_bytes: int, seed: int = 1) -> bytes:
    rng = random.Random(seed)
    out = bytearray(n_bytes)
    for i in range(n_bytes):
        r = rng.randrange(100)
        if r < 40:
            out[i] = LOWER[rng.randrange(26)]
        elif r < 50:
            out[i] = UPPER[rng.randrange(26)]
        elif r < 70:
            out[i] = DIGITS[rng.randrange(10)]
        else:
            out[i] = OTHER[rng.randrange(len(OTHER))]
    return bytes(out)


def row_slice(source: bytes, row_id: int, size: int) -> bytes:
    offset = (row_id - 1) % (len(source) - size + 1)
    return source[offset:offset + size]


def _ydb():
    try:
        import ydb  # type: ignore
    except ImportError as exc:
        raise SystemExit(
            "the ydb Python package is required (pip install ydb). Original: %s" % exc
        ) from exc
    return ydb


def upload_table(driver, ydb, database: str, table: str, source: bytes,
                 n_rows: int, size: int, batch_bytes: int) -> None:
    path = database.rstrip("/") + "/" + table
    with ydb.QuerySessionPool(driver, size=1) as pool:
        pool.execute_with_retries("DROP TABLE IF EXISTS `%s`;" % table)
        pool.execute_with_retries(
            "CREATE TABLE `%s` ("
            "    id Uint64 NOT NULL,"
            "    size_bytes Uint64,"
            "    txt String,"
            "    PRIMARY KEY (id)"
            ");" % table
        )
    cols = ydb.BulkUpsertColumns()
    cols.add_column("id", ydb.PrimitiveType.Uint64)
    cols.add_column("size_bytes", ydb.PrimitiveType.Uint64)
    cols.add_column("txt", ydb.PrimitiveType.String)
    batch_rows = max(1, batch_bytes // size)
    for start in range(1, n_rows + 1, batch_rows):
        end = min(start + batch_rows - 1, n_rows)
        chunk = []
        for row_id in range(start, end + 1):
            chunk.append({
                "id": row_id,
                "size_bytes": size,
                "txt": row_slice(source, row_id, size),
            })
        driver.table_client.bulk_upsert(path, chunk, cols)
        print("  %s rows %s..%s" % (table, start, end), flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Load Text UDF demo tables")
    parser.add_argument("--endpoint", default=os.environ.get("ENDPOINT", "grpc://localhost:2146"))
    parser.add_argument("--database", default=os.environ.get("DB", "/Root/test"))
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--batch-bytes", type=int, default=8 * MIB)
    parser.add_argument("--only", default="", help="comma-separated table names to load")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    wanted = {name.strip() for name in args.only.split(",") if name.strip()}
    tables = [(name, size) for name, size in TABLES if not wanted or name in wanted]
    if not tables:
        raise SystemExit("no tables selected")

    max_size = max(size for _, size in tables)
    print("building ASCII source of %s bytes" % (max_size + args.rows), flush=True)
    t0 = time.time()
    source = make_source(max_size + args.rows)
    sample = row_slice(source, 1, min(len(source), 7))
    # "Abc 123" is the smoke fixture; the generated sample is random ASCII.
    fixture = b"Abc 123"
    print("  fixture Abc 123: letters=%s digits=%s upper=%s len=%s"
          % (count_letters(fixture), count_digits(fixture), count_upper(fixture), len(fixture)),
          flush=True)
    first = row_slice(source, 1, tables[0][1])
    print("  row 1 of %s: letters=%s digits=%s upper=%s len=%s (built in %.2fs)"
          % (tables[0][0], count_letters(first), count_digits(first), count_upper(first),
             len(first), time.time() - t0),
          flush=True)
    print("  sample[:7]=%r" % sample, flush=True)

    if args.dry_run:
        return 0

    ydb = _ydb()
    config = ydb.DriverConfig(endpoint=args.endpoint, database=args.database)
    with ydb.Driver(config) as driver:
        driver.wait(timeout=30, fail_fast=True)
        for name, size in tables:
            print("loading %s: %s rows x %s bytes" % (name, args.rows, size), flush=True)
            upload_table(driver, ydb, args.database, name, source, args.rows, size, args.batch_bytes)
    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
