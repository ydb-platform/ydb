#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standalone helper: upload a UDF to YDB tables.

For NATIVE_UNSAFE: KV volume + meta table (unchanged).
For WASM: wasm_source + meta tables (no KV, no disk).
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys

import ydb

from ydb.tests.functional.udf_store.lib.constants import (
    UDF_KV_BINARIES_PATH,
    UDF_TABLE_LIBRARY_SOURCE_PATH,
    UDF_TABLE_META_PATH,
    UDF_TABLE_WASM_SOURCE_PATH,
)

_CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB


def _uint64(value: int):
    return (value, ydb.PrimitiveType.Uint64.proto)


def _kv_tool() -> str:
    path = os.environ.get("YDB_KV_VOLUME_TOOL_PATH")
    if not path:
        raise RuntimeError("YDB_KV_VOLUME_TOOL_PATH environment variable is not set")
    return path


def _compute_md5(path: str) -> tuple:
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


def _read_file(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def _upload_to_kv(endpoint: str, database: str, udf_file: str, md5: str) -> None:
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


def _upsert_wasm_source(
    pool,
    database: str,
    md5: str,
    version: int,
    body: bytes,
) -> None:
    full_table = "{}/{}".format(database, UDF_TABLE_WASM_SOURCE_PATH)
    query = (
        "DECLARE $md5 AS Utf8; "
        "DECLARE $version AS Uint64; "
        "DECLARE $body AS String; "
        "UPSERT INTO `{}` (md5, version, body) VALUES ($md5, $version, $body);"
    ).format(full_table)
    pool.execute_with_retries(
        query,
        {"$md5": md5, "$version": _uint64(version), "$body": body},
    )


def _upsert_library_source(
    pool,
    database: str,
    name: str,
    md5: str,
    version: int,
    body: bytes,
    compile_status: str = "pending",
) -> None:
    full_table = "{}/{}".format(database, UDF_TABLE_LIBRARY_SOURCE_PATH)
    query = (
        "DECLARE $name AS Utf8; "
        "DECLARE $md5 AS Utf8; "
        "DECLARE $version AS Uint64; "
        "DECLARE $body AS String; "
        "DECLARE $compile_status AS Utf8; "
        "UPSERT INTO `{}` (name, md5, version, body, compile_status) "
        "VALUES ($name, $md5, $version, $body, $compile_status);"
    ).format(full_table)
    pool.execute_with_retries(
        query,
        {
            "$name": name,
            "$md5": md5,
            "$version": _uint64(version),
            "$body": body,
            "$compile_status": compile_status,
        },
    )


def _upsert_udf_row(
    pool,
    database: str,
    md5: str,
    name: str,
    size: int,
    udf_type: str,
    manifest: str = "",
    version: int = 1,
    compile_status: str = "",
) -> None:
    full_table = "{}/{}".format(database, UDF_TABLE_META_PATH)
    params = {
        "$md5": md5,
        "$size": _uint64(size),
        "$name": name,
        "$type": udf_type,
        "$version": _uint64(version),
    }
    if udf_type == "WASM":
        params["$manifest"] = manifest
        params["$compile_status"] = compile_status or "pending"
        query = (
            "DECLARE $md5 AS Utf8; "
            "DECLARE $size AS Uint64; "
            "DECLARE $name AS Utf8; "
            "DECLARE $type AS Utf8; "
            "DECLARE $manifest AS Json; "
            "DECLARE $version AS Uint64; "
            "DECLARE $compile_status AS Utf8; "
            "UPSERT INTO `{}` (md5, size, name, type, manifest, version, compile_status) "
            "VALUES ($md5, $size, $name, $type, $manifest, $version, $compile_status);"
        ).format(full_table)
    else:
        query = (
            "DECLARE $md5 AS Utf8; "
            "DECLARE $size AS Uint64; "
            "DECLARE $name AS Utf8; "
            "DECLARE $type AS Utf8; "
            "UPSERT INTO `{}` (md5, size, name, type) "
            "VALUES ($md5, $size, $name, $type);"
        ).format(full_table)
    pool.execute_with_retries(query, params)


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload a UDF to the YDB UDF store.")
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--udf-file", required=True)
    parser.add_argument("--type", default="NATIVE_UNSAFE", choices=["NATIVE_UNSAFE", "WASM"])
    parser.add_argument("--manifest", default="")
    parser.add_argument("--kind", default="udf", choices=["udf", "library"])
    parser.add_argument("--library-name", default="")
    parser.add_argument("--version", type=int, default=1)
    args = parser.parse_args()

    udf_basename = os.path.basename(args.udf_file)
    udf_name = udf_basename.rsplit(".", 1)[0]

    manifest_text = ""
    if args.type == "WASM" and args.kind == "udf":
        if not args.manifest:
            print("[upload_udf] ERROR: --manifest is required for WASM uploads", file=sys.stderr)
            return 1
        with open(args.manifest, "r", encoding="utf-8") as manifest_file:
            manifest_text = manifest_file.read().strip()
        try:
            json.loads(manifest_text)
        except json.JSONDecodeError as exc:
            print("[upload_udf] ERROR: manifest is not valid JSON: {}".format(exc), file=sys.stderr)
            return 1

    if args.kind == "library" and not args.library_name:
        print("[upload_udf] ERROR: --library-name is required for library uploads", file=sys.stderr)
        return 1

    try:
        md5, size = _compute_md5(args.udf_file)
        body = _read_file(args.udf_file)
        print("[upload_udf] file={} size={} md5={} type={} kind={}".format(
            args.udf_file, size, md5, args.type, args.kind), file=sys.stderr)

        with ydb.Driver(ydb.DriverConfig(endpoint=args.endpoint, database=args.database)) as driver:
            driver.wait(timeout=30, fail_fast=True)
            with ydb.QuerySessionPool(driver, size=1) as pool:
                if args.kind == "library":
                    _upsert_library_source(pool, args.database, args.library_name, md5, args.version, body)
                    print("[upload_udf] library uploaded: name={} md5={}".format(
                        args.library_name, md5), file=sys.stderr)
                elif args.type == "WASM":
                    _upsert_wasm_source(pool, args.database, md5, args.version, body)
                    _upsert_udf_row(
                        pool, args.database, md5, udf_name, size, "WASM",
                        manifest=manifest_text, version=args.version, compile_status="pending",
                    )
                    print("[upload_udf] WASM source + metadata inserted: md5={}".format(md5), file=sys.stderr)
                else:
                    _upload_to_kv(args.endpoint, args.database, args.udf_file, md5)
                    _upsert_udf_row(pool, args.database, md5, udf_name, size, "NATIVE_UNSAFE")
                    print("[upload_udf] native binary uploaded to KV, metadata inserted", file=sys.stderr)

    except Exception as exc:
        print("[upload_udf] ERROR: {}".format(exc), file=sys.stderr)
        return 1

    print(md5)
    return 0


if __name__ == "__main__":
    sys.exit(main())
