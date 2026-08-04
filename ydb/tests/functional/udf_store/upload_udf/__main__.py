#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standalone helper: upload a UDF binary to the YDB KV-body store and register
it in the UDF metadata table (md5, size, name, type).

Usage:
    upload_udf \\
        --endpoint grpc://localhost:2136 \\
        --database /Root/mydb \\
        --udf-file /path/to/libdicts_udf.so

The kv_volume_tool binary is located via the YDB_KV_VOLUME_TOOL_PATH
environment variable (set automatically when running under `ya test`).

On success the program prints the md5 hex-digest of the uploaded binary to
stdout and exits with code 0.  On any error it prints a message to stderr and
exits with a non-zero code.
"""

import argparse
import hashlib
import os
import subprocess
import sys

import ydb

from ydb.tests.functional.udf_store.lib.constants import UDF_KV_BINARIES_PATH, UDF_TABLE_META_PATH
_CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB


def _kv_tool() -> str:
    path = os.environ.get("YDB_KV_VOLUME_TOOL_PATH")
    if not path:
        raise RuntimeError("YDB_KV_VOLUME_TOOL_PATH environment variable is not set")
    return path


def _compute_md5(path: str) -> tuple:
    """Return (hex_md5, file_size_bytes) computed by streaming the file."""
    ctx = hashlib.md5()
    size = 0
    with open(path, "rb") as f:
        while True:
            chunk = f.read(_CHUNK_SIZE)
            if not chunk:
                break
            ctx.update(chunk)
            size += len(chunk)
    return ctx.hexdigest(), size


def _upload_to_kv(endpoint: str, database: str, udf_file: str, md5: str) -> None:
    """Upload the UDF binary to the KV body store with key=md5."""
    full_volume_path = "{}/{}".format(database, UDF_KV_BINARIES_PATH)
    cmd = [
        _kv_tool(), "upload",
        "-e", endpoint,
        "-d", database,
        "-p", full_volume_path,
        "-v",
        "--partition-id", "0",
        "--key", md5,
        "--file", udf_file,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(
            "kv_volume_tool upload failed (rc={}): stdout={!r}, stderr={!r}".format(
                result.returncode, result.stdout, result.stderr)
        )


def _upsert_udf_row(endpoint: str, database: str, md5: str, name: str, size: int) -> None:
    """Insert/update a row in the UDF metadata table via the YDB Python SDK."""
    full_table = "{}/{}".format(database, UDF_TABLE_META_PATH)
    query = (
        'UPSERT INTO `{}` (md5, size, name, type) '
        'VALUES ("{}", {}, "{}", "NATIVE_UNSAFE")'
    ).format(full_table, md5, size, name)

    with ydb.Driver(ydb.DriverConfig(endpoint=endpoint, database=database)) as driver:
        driver.wait(timeout=30, fail_fast=True)
        with ydb.QuerySessionPool(driver, size=1) as pool:
            pool.execute_with_retries(query)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Upload a UDF binary to the YDB KV store and register it in the metadata table."
    )
    parser.add_argument("--endpoint", required=True,
                        help="YDB gRPC endpoint, e.g. grpc://localhost:2136")
    parser.add_argument("--database", required=True,
                        help="YDB database path, e.g. /Root/mydb")
    parser.add_argument("--udf-file", required=True,
                        help="Path to the UDF shared library (.so) to upload")
    args = parser.parse_args()

    # Derive the UDF name from the filename, stripping known suffixes.
    udf_basename = os.path.basename(args.udf_file)
    udf_name = udf_basename.removesuffix(".so")

    try:
        md5, size = _compute_md5(args.udf_file)
        print("[upload_udf] file={} size={} md5={}".format(args.udf_file, size, md5), file=sys.stderr)

        _upload_to_kv(args.endpoint, args.database, args.udf_file, md5)
        print("[upload_udf] binary uploaded to KV volume with key={}".format(md5), file=sys.stderr)

        _upsert_udf_row(args.endpoint, args.database, md5, udf_name, size)
        print("[upload_udf] metadata row inserted: name={} md5={} size={}".format(udf_name, md5, size), file=sys.stderr)

    except Exception as exc:
        print("[upload_udf] ERROR: {}".format(exc), file=sys.stderr)
        return 1

    # Print md5 to stdout so callers can capture it.
    print(md5)
    return 0


if __name__ == "__main__":
    sys.exit(main())
