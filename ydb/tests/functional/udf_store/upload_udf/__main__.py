#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Standalone helper: upload or delete a UDF / WASM library in YDB UDF store tables.

A module is identified by `name` (for a WASM UDF that is the manifest's
`module_name`, i.e. the name YQL queries call it by). Re-uploading the same
name replaces the row. `uid` is generated on each upload and can be used as a
handle to delete that row. `md5` is only a checksum of the uploaded body,
checked when the chunks are joined back together.

Upload:
  NATIVE_UNSAFE: KV volume (key=name) + modules row (chunk_count=0)
  WASM udf / library: modules row + module_chunks

Delete:
  --uid or --name. Also --library-name for library.
  WASM: modules + module_chunks + best-effort AOT artifacts (kind=module, id=name)
  library: modules + module_chunks + best-effort AOT artifacts (kind=library, id=name)
  NATIVE_UNSAFE: modules (KV orphan left; service removes on-disk copy on snapshot)
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import uuid

import ydb

from ydb.tests.functional.udf_store.lib.constants import (
    UDF_ARTIFACTS_DIR_PATH,
    UDF_KV_BINARIES_PATH,
    UDF_TABLE_MODULE_CHUNKS_PATH,
    UDF_TABLE_MODULES_PATH,
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


def _upload_to_kv(endpoint: str, database: str, udf_file: str, key: str) -> None:
    full_volume_path = "{}/{}".format(database, UDF_KV_BINARIES_PATH)
    cmd = [
        _kv_tool(), "upload",
        "-e", endpoint,
        "-d", database,
        "-p", full_volume_path,
        "-v",
        "--partition-id", "0",
        "--key", key,
        "--file", udf_file,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.returncode != 0:
        raise RuntimeError(
            "kv_volume_tool upload failed (rc={}): stdout={!r}, stderr={!r}".format(
                result.returncode, result.stdout, result.stderr)
        )


def _upsert_source_chunks(pool, database: str, owner_key: str, chunks: list) -> None:
    full_table = "{}/{}".format(database, UDF_TABLE_MODULE_CHUNKS_PATH)
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


def _find_uid(pool, database: str, *, name: str = "", module_type: str = "") -> str:
    if not name:
        return ""
    full_table = "{}/{}".format(database, UDF_TABLE_MODULES_PATH)
    query = (
        "DECLARE $name AS Utf8; "
        "DECLARE $type AS Utf8; "
        "SELECT uid FROM `{}` WHERE name = $name AND type = $type LIMIT 1;"
    ).format(full_table)
    result = pool.execute_with_retries(query, {"$name": name, "$type": module_type})
    if not result or not result[0].rows:
        return ""
    return list(result[0].rows[0].values())[0]


def _delete_chunks(pool, database: str, uid: str) -> None:
    if not uid:
        return
    _delete_by_key(pool, database, UDF_TABLE_MODULE_CHUNKS_PATH, "owner_key", uid)


def _upsert_module_row(
    pool,
    database: str,
    uid: str,
    md5: str,
    name: str,
    size: int,
    module_type: str,
    chunk_count: int = 0,
    manifest: str = "",
    version: int = 1,
    compile_status: str = "",
) -> None:
    full_table = "{}/{}".format(database, UDF_TABLE_MODULES_PATH)
    params = {
        "$uid": uid,
        "$md5": md5,
        "$size": _uint64(size),
        "$name": name,
        "$type": module_type,
        "$version": _uint64(version),
        "$chunk_count": _uint64(chunk_count),
    }
    columns = "uid, md5, size, name, type, version, chunk_count, created_at"
    values = "$uid, $md5, $size, $name, $type, $version, $chunk_count, CurrentUtcTimestamp()"
    decls = (
        "DECLARE $uid AS Utf8; "
        "DECLARE $md5 AS Utf8; "
        "DECLARE $size AS Uint64; "
        "DECLARE $name AS Utf8; "
        "DECLARE $type AS Utf8; "
        "DECLARE $version AS Uint64; "
        "DECLARE $chunk_count AS Uint64; "
    )
    if module_type in ("WASM", "LIBRARY"):
        params["$compile_status"] = compile_status or "pending"
        decls += "DECLARE $compile_status AS Utf8; "
        columns += ", compile_status"
        values += ", $compile_status"
    if module_type == "WASM":
        params["$manifest"] = _json(manifest)
        decls += "DECLARE $manifest AS Json; "
        columns += ", manifest"
        values += ", $manifest"
    query = (
        "{decls}"
        "UPSERT INTO `{table}` ({columns}) "
        "VALUES ({values});"
    ).format(decls=decls, table=full_table, columns=columns, values=values)
    pool.execute_with_retries(query, params)


def _upsert_wasm_or_library(
    pool,
    database: str,
    *,
    module_type: str,
    md5: str,
    name: str,
    version: int,
    body: bytes,
    manifest: str = "",
) -> str:
    existing_uid = _find_uid(pool, database, name=name, module_type=module_type)
    if existing_uid:
        _delete_chunks(pool, database, existing_uid)
    uid = str(uuid.uuid4())

    chunks = _split_blob(body)
    _upsert_source_chunks(pool, database, uid, chunks)
    _upsert_module_row(
        pool,
        database,
        uid=uid,
        md5=md5,
        name=name,
        size=len(body),
        module_type=module_type,
        chunk_count=len(chunks),
        manifest=manifest,
        version=version,
        compile_status="pending",
    )
    return uid


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


def _select_module_row(pool, database: str, *, uid: str = "", name: str = "", module_type: str = "") -> dict:
    """Return {uid, md5, name, type} for a modules row, or {} if missing."""
    full_table = "{}/{}".format(database, UDF_TABLE_MODULES_PATH)
    if uid:
        query = (
            "DECLARE $uid AS Utf8; "
            "SELECT uid, md5, name, type FROM `{}` WHERE uid = $uid LIMIT 1;"
        ).format(full_table)
        result = pool.execute_with_retries(query, {"$uid": uid})
    elif name:
        decls = "DECLARE $name AS Utf8; "
        where = "name = $name"
        params = {"$name": name}
        if module_type:
            decls += "DECLARE $type AS Utf8; "
            where += " AND type = $type"
            params["$type"] = module_type
        query = (
            decls + "SELECT uid, md5, name, type FROM `{}` WHERE {} LIMIT 1;"
        ).format(full_table, where)
        result = pool.execute_with_retries(query, params)
    else:
        return {}
    if not result or not result[0].rows:
        return {}
    values = list(result[0].rows[0].values())
    return {
        "uid": values[0],
        "md5": values[1],
        "name": values[2],
        "type": values[3],
    }


def _delete_module_by_uid(driver, pool, database: str, uid: str, udf_type: str = "") -> str:
    row = _select_module_row(pool, database, uid=uid)
    if not row:
        raise RuntimeError(
            "no modules row with uid={} (already deleted or wrong --database)".format(uid)
        )
    module_type = row["type"] or udf_type
    md5 = row["md5"] or ""
    name = row["name"] or ""
    print(
        "[upload_udf] deleting module: uid={} md5={} name={} type={}".format(
            uid, md5, name, module_type
        ),
        file=sys.stderr,
    )
    _delete_chunks(pool, database, uid)
    _delete_by_key(pool, database, UDF_TABLE_MODULES_PATH, "name", name)
    if _select_module_row(pool, database, uid=uid):
        raise RuntimeError("delete failed: modules row still present for uid={}".format(uid))
    if module_type in ("WASM", "LIBRARY") and name:
        kind = "library" if module_type == "LIBRARY" else "module"
        _delete_artifacts(driver, pool, database, name, kind)
    print(
        "[upload_udf] deleted from tables: name={} uid={}. "
        "In-memory unload waits for UDF store metadata refresh (~10s)".format(name, uid),
        file=sys.stderr,
    )
    return name


def _delete_udf(driver, pool, database: str, name: str) -> None:
    row = _select_module_row(pool, database, name=name)
    if not row:
        raise RuntimeError(
            "no modules row with name={} (already deleted or wrong --database). "
            "List rows: SELECT uid, name, type FROM `{}/{}`".format(
                name, database, UDF_TABLE_MODULES_PATH
            )
        )
    _delete_module_by_uid(driver, pool, database, row["uid"], row["type"])


def _delete_library(driver, pool, database: str, name: str) -> None:
    uid = _find_uid(pool, database, name=name, module_type="LIBRARY")
    if not uid:
        raise RuntimeError(
            "no LIBRARY modules row with name={} (already deleted or wrong --database)".format(name)
        )
    _delete_module_by_uid(driver, pool, database, uid, "LIBRARY")


def _module_name_from_manifest(manifest_text: str) -> str:
    try:
        obj = json.loads(manifest_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("manifest is not valid JSON: {}".format(exc)) from exc
    name = obj.get("module_name") if isinstance(obj, dict) else None
    if not isinstance(name, str) or not name.strip():
        raise RuntimeError("manifest must contain a non-empty module_name")
    return name.strip()


def _do_upload(args) -> str:
    udf_basename = os.path.basename(args.udf_file)
    udf_name = udf_basename.rsplit(".", 1)[0]

    manifest_text = ""
    if args.type == "WASM" and args.kind == "udf":
        if not args.manifest:
            raise RuntimeError("--manifest is required for WASM uploads")
        with open(args.manifest, "r", encoding="utf-8") as manifest_file:
            manifest_text = manifest_file.read().strip()
        udf_name = _module_name_from_manifest(manifest_text)

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
                uid = _upsert_wasm_or_library(
                    pool,
                    args.database,
                    module_type="LIBRARY",
                    md5=md5,
                    name=args.library_name,
                    version=args.version,
                    body=body,
                )
                print("[upload_udf] library uploaded: name={} uid={} md5={}".format(
                    args.library_name, uid, md5), file=sys.stderr)
                return args.library_name
            if args.type == "WASM":
                uid = _upsert_wasm_or_library(
                    pool,
                    args.database,
                    module_type="WASM",
                    md5=md5,
                    name=udf_name,
                    version=args.version,
                    body=body,
                    manifest=manifest_text,
                )
                print("[upload_udf] WASM module uploaded: name={} uid={} md5={}".format(
                    udf_name, uid, md5), file=sys.stderr)
                return udf_name

            uid = str(uuid.uuid4())
            _upload_to_kv(args.endpoint, args.database, args.udf_file, udf_name)
            _upsert_module_row(
                pool,
                args.database,
                uid=uid,
                md5=md5,
                name=udf_name,
                size=size,
                module_type="NATIVE_UNSAFE",
                chunk_count=0,
                version=args.version,
            )
            print("[upload_udf] native binary uploaded to KV: name={} uid={} md5={}".format(
                udf_name, uid, md5), file=sys.stderr)
            return udf_name


def _do_delete(args) -> str:
    with ydb.Driver(ydb.DriverConfig(endpoint=args.endpoint, database=args.database)) as driver:
        driver.wait(timeout=30, fail_fast=True)
        with ydb.QuerySessionPool(driver, size=1) as pool:
            if args.uid:
                return _delete_module_by_uid(driver, pool, args.database, args.uid, args.type)

            if args.kind == "library":
                name = args.library_name or args.name
                if not name:
                    raise RuntimeError("--library-name or --name is required for library delete (or pass --uid)")
                _delete_library(driver, pool, args.database, name)
                return name

            name = args.name
            if not name and args.manifest:
                with open(args.manifest, "r", encoding="utf-8") as manifest_file:
                    name = _module_name_from_manifest(manifest_file.read().strip())
            if not name:
                raise RuntimeError("delete udf requires --uid, --name, or --manifest")
            _delete_udf(driver, pool, args.database, name)
            return name


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload or delete a UDF / library in the YDB UDF store.")
    parser.add_argument("--action", default="upload", choices=["upload", "delete"])
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--udf-file", default="")
    parser.add_argument(
        "--uid",
        default="",
        help="modules.uid generated at upload, for --action delete",
    )
    parser.add_argument(
        "--name",
        default="",
        help="modules.name (PK): manifest module_name for a WASM UDF, library name, or native basename",
    )
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
