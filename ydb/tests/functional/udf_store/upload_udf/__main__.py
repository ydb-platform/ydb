#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standalone helper: upload or delete a UDF / WASM library in YDB UDF store tables.

Upload:
  NATIVE_UNSAFE: KV volume + meta
  WASM udf: wasm_source(+chunks) + meta
  library: library_source(+chunks)

Delete:
  WASM udf: meta + wasm_source(+chunks) + best-effort AOT artifacts (kind=module)
  library: library_source(+chunks) + best-effort AOT artifacts (kind=library)
  NATIVE_UNSAFE: meta (KV orphan left; service removes on-disk copy on snapshot)
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys

import ydb

from ydb.tests.functional.udf_store.lib.constants import (
    UDF_ARTIFACTS_DIR_PATH,
    UDF_KV_BINARIES_PATH,
    UDF_TABLE_LIBRARY_SOURCE_CHUNKS_PATH,
    UDF_TABLE_LIBRARY_SOURCE_PATH,
    UDF_TABLE_META_PATH,
    UDF_TABLE_WASM_SOURCE_CHUNKS_PATH,
    UDF_TABLE_WASM_SOURCE_PATH,
    WASM_BLOB_CHUNK_SIZE,
)

_HASH_CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB


def _uint64(value: int):
    return (value, ydb.PrimitiveType.Uint64.proto)


def _json(value: str):
    return (value, ydb.PrimitiveType.Json.proto)


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
            chunk = f.read(_HASH_CHUNK_SIZE)
            if not chunk:
                break
            ctx.update(chunk)
            size += len(chunk)
    return ctx.hexdigest(), size


def _read_file(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()


def _split_blob(data: bytes, chunk_size: int = WASM_BLOB_CHUNK_SIZE) -> list:
    if not data:
        return []
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]


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


def _upsert_source_chunks(pool, database: str, chunks_table: str, owner_key: str, chunks: list) -> None:
    full_table = "{}/{}".format(database, chunks_table)
    query = (
        "DECLARE $owner_key AS Utf8; "
        "DECLARE $chunk_idx AS Uint64; "
        "DECLARE $data AS String; "
        "UPSERT INTO `{}` (owner_key, chunk_idx, data) "
        "VALUES ($owner_key, $chunk_idx, $data);"
    ).format(full_table)
    for idx, chunk in enumerate(chunks):
        pool.execute_with_retries(
            query,
            {
                "$owner_key": owner_key,
                "$chunk_idx": _uint64(idx),
                "$data": chunk,
            },
        )


def _upsert_wasm_source(
    pool,
    database: str,
    md5: str,
    version: int,
    body: bytes,
) -> None:
    chunks = _split_blob(body)
    _upsert_source_chunks(pool, database, UDF_TABLE_WASM_SOURCE_CHUNKS_PATH, md5, chunks)

    full_table = "{}/{}".format(database, UDF_TABLE_WASM_SOURCE_PATH)
    query = (
        "DECLARE $md5 AS Utf8; "
        "DECLARE $version AS Uint64; "
        "DECLARE $size AS Uint64; "
        "DECLARE $chunk_count AS Uint64; "
        "UPSERT INTO `{}` (md5, version, size, chunk_count) "
        "VALUES ($md5, $version, $size, $chunk_count);"
    ).format(full_table)
    pool.execute_with_retries(
        query,
        {
            "$md5": md5,
            "$version": _uint64(version),
            "$size": _uint64(len(body)),
            "$chunk_count": _uint64(len(chunks)),
        },
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
    chunks = _split_blob(body)
    _upsert_source_chunks(pool, database, UDF_TABLE_LIBRARY_SOURCE_CHUNKS_PATH, name, chunks)

    full_table = "{}/{}".format(database, UDF_TABLE_LIBRARY_SOURCE_PATH)
    query = (
        "DECLARE $name AS Utf8; "
        "DECLARE $md5 AS Utf8; "
        "DECLARE $version AS Uint64; "
        "DECLARE $size AS Uint64; "
        "DECLARE $chunk_count AS Uint64; "
        "DECLARE $compile_status AS Utf8; "
        "UPSERT INTO `{}` (name, md5, version, size, chunk_count, compile_status) "
        "VALUES ($name, $md5, $version, $size, $chunk_count, $compile_status);"
    ).format(full_table)
    pool.execute_with_retries(
        query,
        {
            "$name": name,
            "$md5": md5,
            "$version": _uint64(version),
            "$size": _uint64(len(body)),
            "$chunk_count": _uint64(len(chunks)),
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
        params["$manifest"] = _json(manifest)
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


def _delete_by_key(pool, database: str, table: str, column: str, value: str) -> None:
    full_table = "{}/{}".format(database, table)
    query = (
        "DECLARE $key AS Utf8; "
        "DELETE FROM `{}` WHERE {} = $key;"
    ).format(full_table, column)
    pool.execute_with_retries(query, {"$key": value})


def _list_artifact_table_paths(driver, database: str) -> list:
    """Return relative paths of artifact tables (not *_chunks) under udf_store/artifacts."""
    artifacts_path = "{}/{}".format(database, UDF_ARTIFACTS_DIR_PATH)
    try:
        listing = driver.scheme_client.list_directory(artifacts_path)
    except Exception as exc:
        print("[upload_udf] artifacts dir not listed ({}): {}".format(artifacts_path, exc), file=sys.stderr)
        return []

    paths = []
    for child in listing.children:
        name = getattr(child, "name", "")
        if not name or name.endswith("_chunks"):
            continue
        paths.append("{}/{}".format(UDF_ARTIFACTS_DIR_PATH, name))
    return paths


def _delete_artifacts(driver, pool, database: str, artifact_id: str, kind: str) -> None:
    for table_rel in _list_artifact_table_paths(driver, database):
        chunks_rel = table_rel + "_chunks"
        for rel, extra in (
            (chunks_rel, " AND kind = $kind"),
            (table_rel, " AND kind = $kind"),
        ):
            full_table = "{}/{}".format(database, rel)
            query = (
                "DECLARE $id AS Utf8; "
                "DECLARE $kind AS Utf8; "
                "DELETE FROM `{}` WHERE id = $id{};"
            ).format(full_table, extra)
            try:
                pool.execute_with_retries(query, {"$id": artifact_id, "$kind": kind})
            except Exception as exc:
                print(
                    "[upload_udf] best-effort artifact delete skipped for {}: {}".format(rel, exc),
                    file=sys.stderr,
                )


def _delete_udf(driver, pool, database: str, md5: str, udf_type: str) -> None:
    _delete_by_key(pool, database, UDF_TABLE_META_PATH, "md5", md5)
    if udf_type == "WASM":
        _delete_by_key(pool, database, UDF_TABLE_WASM_SOURCE_PATH, "md5", md5)
        _delete_by_key(pool, database, UDF_TABLE_WASM_SOURCE_CHUNKS_PATH, "owner_key", md5)
        _delete_artifacts(driver, pool, database, md5, "module")
    print("[upload_udf] deleted udf: md5={} type={}".format(md5, udf_type), file=sys.stderr)


def _delete_library(driver, pool, database: str, name: str) -> None:
    _delete_by_key(pool, database, UDF_TABLE_LIBRARY_SOURCE_PATH, "name", name)
    _delete_by_key(pool, database, UDF_TABLE_LIBRARY_SOURCE_CHUNKS_PATH, "owner_key", name)
    _delete_artifacts(driver, pool, database, name, "library")
    print("[upload_udf] deleted library: name={}".format(name), file=sys.stderr)


def _do_upload(args) -> str:
    udf_basename = os.path.basename(args.udf_file)
    udf_name = udf_basename.rsplit(".", 1)[0]

    manifest_text = ""
    if args.type == "WASM" and args.kind == "udf":
        if not args.manifest:
            raise RuntimeError("--manifest is required for WASM uploads")
        with open(args.manifest, "r", encoding="utf-8") as manifest_file:
            manifest_text = manifest_file.read().strip()
        try:
            json.loads(manifest_text)
        except json.JSONDecodeError as exc:
            raise RuntimeError("manifest is not valid JSON: {}".format(exc)) from exc

    if args.kind == "library" and not args.library_name:
        raise RuntimeError("--library-name is required for library uploads")

    md5, size = _compute_md5(args.udf_file)
    body = _read_file(args.udf_file)
    print("[upload_udf] file={} size={} md5={} type={} kind={} chunks={}".format(
        args.udf_file, size, md5, args.type, args.kind, len(_split_blob(body))), file=sys.stderr)

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
    return md5


def _do_delete(args) -> str:
    if args.kind == "library":
        if not args.library_name:
            raise RuntimeError("--library-name is required for library delete")
        with ydb.Driver(ydb.DriverConfig(endpoint=args.endpoint, database=args.database)) as driver:
            driver.wait(timeout=30, fail_fast=True)
            with ydb.QuerySessionPool(driver, size=1) as pool:
                _delete_library(driver, pool, args.database, args.library_name)
        return args.library_name

    md5 = args.md5
    if not md5:
        if not args.udf_file:
            raise RuntimeError("delete udf requires --md5 or --udf-file")
        md5, _ = _compute_md5(args.udf_file)

    with ydb.Driver(ydb.DriverConfig(endpoint=args.endpoint, database=args.database)) as driver:
        driver.wait(timeout=30, fail_fast=True)
        with ydb.QuerySessionPool(driver, size=1) as pool:
            _delete_udf(driver, pool, args.database, md5, args.type)
    return md5


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload or delete a UDF / library in the YDB UDF store.")
    parser.add_argument("--action", default="upload", choices=["upload", "delete"])
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--udf-file", default="")
    parser.add_argument("--md5", default="", help="UDF md5 for --action delete (optional if --udf-file given)")
    parser.add_argument("--type", default="NATIVE_UNSAFE", choices=["NATIVE_UNSAFE", "WASM"])
    parser.add_argument("--manifest", default="")
    parser.add_argument("--kind", default="udf", choices=["udf", "library"])
    parser.add_argument("--library-name", default="")
    parser.add_argument("--version", type=int, default=1)
    args = parser.parse_args()

    try:
        if args.action == "upload":
            if not args.udf_file:
                print("[upload_udf] ERROR: --udf-file is required for upload", file=sys.stderr)
                return 1
            key = _do_upload(args)
        else:
            key = _do_delete(args)
    except Exception as exc:
        print("[upload_udf] ERROR: {}".format(exc), file=sys.stderr)
        return 1

    print(key)
    return 0


if __name__ == "__main__":
    sys.exit(main())
