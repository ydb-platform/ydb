# -*- coding: utf-8 -*-
import hashlib
import json
import logging
import os
import shutil
import subprocess
import time
import zipfile

import pytest
import yaml
import yatest.common

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.functional.udf_store.lib.constants import (
    UDF_TABLE_MODULES_PATH,
    UDF_TABLE_MODULE_CHUNKS_PATH,
    UDF_KV_BINARIES_PATH,
)

logger = logging.getLogger(__name__)

CLUSTER_CONFIG = dict(
    additional_log_configs={
        "METADATA_PROVIDER": 7,  # DEBUG
    }
)

UDF_OUTPUT_DIR = yatest.common.output_path("ydb_udfs")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _run_query(config, query):
    with ydb.Driver(config) as driver:
        with ydb.QuerySessionPool(driver, size=1) as pool:
            return pool.execute_with_retries(query)


def _wait_for_condition(condition_fn, timeout_seconds=60, poll_interval=1, description="condition"):
    """Poll until condition_fn() returns True or timeout expires."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if condition_fn():
            return True
        logger.info("Waiting for %s... (%.0fs remaining)", description, deadline - time.time())
        time.sleep(poll_interval)
    return False


def _module_compile_ready(endpoint, database, name):
    import grpc
    from ydb.public.api.grpc.ydb_udf_v1_pb2_grpc import UdfServiceStub
    from ydb.public.api.protos import ydb_udf_pb2 as udf

    target = endpoint.removeprefix("grpc://")
    try:
        with grpc.insecure_channel(target) as channel:
            response = UdfServiceStub(channel).DescribeModule(
                udf.DescribeModuleRequest(name=name),
                metadata=(("x-ydb-database", database),),
                timeout=30,
            )
        result = udf.DescribeModuleResult()
        if not response.operation.result.Unpack(result) or not result.platforms:
            return False
        return all(platform.status == udf.READY for platform in result.platforms)
    except Exception as error:
        logger.debug("module %s compile state not ready yet: %s", name, error)
        return False


def _kv_volume_tool():
    return yatest.common.binary_path(os.environ["YDB_KV_VOLUME_TOOL_PATH"])


def _run_kv_tool(endpoint, database, path, command, *extra_args):
    """Run kv_volume_tool with the given command; raise RuntimeError on failure."""
    cmd = [_kv_volume_tool(), command, "-e", endpoint, "-d", database, "-p", path, "-v", *extra_args]
    logger.info("Running kv_volume_tool: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    logger.info("kv_volume_tool stdout: %s", result.stdout)
    if result.stderr:
        logger.info("kv_volume_tool stderr: %s", result.stderr)
    if result.returncode != 0:
        raise RuntimeError(
            f"kv_volume_tool {command} failed (rc={result.returncode}): "
            f"stdout={result.stdout}, stderr={result.stderr}"
        )
    return result


def _table_exists(config, database, table_path=UDF_TABLE_MODULES_PATH):
    """Return True if the UDF metadata table can be queried."""
    try:
        result = _run_query(
            config,
            "SELECT COUNT(*) AS cnt FROM `{database}/{path}`".format(database=database, path=table_path),
        )
        return bool(result and result[0].rows)
    except Exception as e:
        logger.debug("UDF metadata table not ready yet: %s", e)
        return False


def _kv_volume_exists(endpoint, database, path=UDF_KV_BINARIES_PATH):
    """Return True if the KV volume responds to 'describe'."""
    try:
        _run_kv_tool(endpoint, database, path, "describe")
        return True
    except RuntimeError as e:
        logger.debug("KV volume not ready yet: %s", e)
        return False


@pytest.mark.parametrize("enable_udf_store", [True, False], ids=["flag_on", "flag_off"])
def test_udf_store_feature_flag(enable_udf_store):
    """
    When udf_store_config.enabled=true  → both the UDF metadata table and the KV volume must be created.
    When udf_store_config is absent or disabled → neither must appear.
    """
    database = "/Root/test"
    cluster = _make_cluster(enable_udf_store=enable_udf_store)
    db_nodes = _create_database(cluster, database)
    try:
        node = cluster.nodes[1]
        driver_config = ydb.DriverConfig(
            endpoint="%s:%s" % (node.host, node.port),
            database=database,
        )
        grpc_endpoint = "grpc://%s:%s" % (node.host, node.port)
        timeout = _SETTLE_TIMEOUT if enable_udf_store else _ABSENT_TIMEOUT

        table_appeared = _wait_for_condition(
            lambda: _table_exists(driver_config, database),
            timeout_seconds=timeout,
            description="UDF metadata table (enable_udf_store=%s)" % enable_udf_store,
        )
        kv_appeared = _wait_for_condition(
            lambda: _kv_volume_exists(grpc_endpoint, database),
            timeout_seconds=timeout,
            description="KV volume (enable_udf_store=%s)" % enable_udf_store,
        )

        if enable_udf_store:
            assert table_appeared, (
                "UDF metadata table `%s` was NOT created within %ds when udf_store_config.enabled=true"
                % (UDF_TABLE_MODULES_PATH, _SETTLE_TIMEOUT)
            )
            assert kv_appeared, (
                "KV volume `%s` was NOT created within %ds when udf_store_config.enabled=true"
                % (UDF_KV_BINARIES_PATH, _SETTLE_TIMEOUT)
            )
        else:
            assert not table_appeared, (
                "UDF metadata table `%s` appeared even though udf_store_config is disabled" % UDF_TABLE_MODULES_PATH
            )
            assert not kv_appeared, (
                "KV volume `%s` appeared even though udf_store_config is disabled" % UDF_KV_BINARIES_PATH
            )
    finally:
        cluster.remove_database(database)
        cluster.unregister_and_stop_slots(db_nodes)
        cluster.stop()


def _upload_udf_binary():
    return yatest.common.binary_path(os.environ["YDB_UPLOAD_UDF_PATH"])


def _run_upload_udf(
    endpoint,
    database,
    udf_file_path="",
    udf_type="NATIVE_UNSAFE",
    manifest_path="",
    kind="udf",
    library_name="",
    action="upload",
    name="",
):
    """
    Invoke the upload_udf binary as a subprocess.

    Returns the module name printed by the binary on stdout.
    Raises RuntimeError if the binary exits with a non-zero code.
    """
    if udf_type == "WASM":
        if action == "delete":
            return _run_ydb_udf(endpoint, database, "delete", "--name", name or library_name)
        if kind == "library" and not manifest_path:
            manifest_path = _write_manifest(library_name)
        result = json.loads(_run_ydb_udf(
            endpoint, database, "upload", "--file", udf_file_path,
            "--manifest", manifest_path, "--format", "json",
        ))
        return result["name"]
    cmd = [
        _upload_udf_binary(),
        "--action", action,
        "--endpoint", endpoint,
        "--database", database,
        "--type", udf_type,
        "--kind", kind,
    ]
    if udf_file_path:
        cmd.extend(["--udf-file", udf_file_path])
    if name:
        cmd.extend(["--name", name])
    if manifest_path:
        cmd.extend(["--manifest", manifest_path])
    if library_name:
        cmd.extend(["--library-name", library_name])
    # Resolve YDB_KV_VOLUME_TOOL_PATH to an absolute path so the subprocess
    # can find the binary regardless of its working directory.
    env = os.environ.copy()
    env["YDB_KV_VOLUME_TOOL_PATH"] = _kv_volume_tool()
    logger.info("Running upload_udf: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120, env=env)
    if result.stderr:
        logger.info("upload_udf stderr:\n%s", result.stderr.strip())
    if result.returncode != 0:
        raise RuntimeError(
            f"upload_udf failed (rc={result.returncode}): {result.stderr}"
        )
    return result.stdout.strip()


def _run_upload_library(endpoint, database, library_file_path, library_name):
    """Upload a textual WASM fixture with its library manifest."""
    manifest_path = yatest.common.output_path(library_name + ".manifest.json")
    with open(manifest_path, "w") as output:
        json.dump(dict(module_name=library_name, module_type="library", module_kind="wasm",
                       module_extension="wat"), output)
    return _run_upload_udf(
        endpoint,
        database,
        udf_file_path=library_file_path,
        udf_type="WASM",
        kind="library",
        library_name=library_name,
        manifest_path=manifest_path,
    )


def _run_delete_udf(endpoint, database, name, udf_type="WASM"):
    """Delete a UDF module row (and related chunks/artifacts) by name."""
    return _run_upload_udf(
        endpoint,
        database,
        udf_type=udf_type,
        kind="udf",
        action="delete",
        name=name,
    )


def _run_delete_library(endpoint, database, library_name):
    """Delete a WASM library by name."""
    return _run_upload_udf(
        endpoint,
        database,
        udf_type="WASM",
        kind="library",
        library_name=library_name,
        action="delete",
    )


def test_using_native_unsafe_udf():
    """
    1. Use the pre-built dicts UDF shared library as the binary to upload.
    2. Delegate upload + metadata registration to the upload_udf helper binary.
    3. TUdfStoreService detects the new metadata row, fetches the binary
       from the KV store, and writes it to UnsafeNativeUdfDir/<name>.
    4. Assert that the file exists, its size and md5 match the uploaded body.
    """
    udf_output_dir = UDF_OUTPUT_DIR
    database = "/Root/test"
    cluster = _make_cluster(enable_udf_store=True, enable_native_udf=True, native_udf_dir=udf_output_dir)
    db_nodes = _create_database(cluster, database)
    try:
        node = cluster.nodes[1]
        driver_config = ydb.DriverConfig(
            endpoint="%s:%s" % (node.host, node.port),
            database=database,
        )
        endpoint = "grpc://%s:%s" % (node.host, node.port)

        # --- Step 0: Wait for UDF metadata table ---
        assert _wait_for_condition(
            lambda: _table_exists(driver_config, database),
            timeout_seconds=60,
            description="UDF metadata table creation at startup",
        ), "UDF metadata table was not created at startup within timeout"

        # --- Step 1: Clean output directory so we start from a known state ---
        if os.path.exists(udf_output_dir):
            shutil.rmtree(udf_output_dir)

        # --- Step 2: Resolve the pre-built dicts UDF path ---
        udf_so_path = yatest.common.binary_path(os.environ["YDB_DICTS_UDF_PATH"])
        logger.info("Dicts UDF binary path: %s", udf_so_path)

        # --- Step 3: Wait for KV volume ---
        assert _wait_for_condition(
            lambda: _kv_volume_exists(endpoint, database),
            timeout_seconds=60,
            description="KV volume creation at startup",
        ), f"KV volume at {UDF_KV_BINARIES_PATH} was not created at startup within timeout"

        # --- Step 4+5: Upload binary and register metadata (with size) via upload_udf ---
        udf_name = _run_upload_udf(endpoint, database, udf_so_path)
        logger.info("upload_udf reported name=%s", udf_name)

        # --- Step 6: Wait for binary to appear in UnsafeNativeUdfDir ---
        # TKvBodyReadActor names the output file after the module name.
        expected_file_path = os.path.join(udf_output_dir, udf_name)
        assert _wait_for_condition(
            lambda: os.path.isfile(expected_file_path),
            timeout_seconds=120,
            description=f"native UDF file {expected_file_path}",
        ), (
            f"Native UDF file was not created at {expected_file_path} within timeout. "
            f"Expected TUdfStoreService to fetch the binary from KV and write it to "
            f"UnsafeNativeUdfDir='{udf_output_dir}' under filename=name='{udf_name}'."
        )

        # --- Step 7: Verify file size and md5 ---
        CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB
        binary_size = os.path.getsize(udf_so_path)
        saved_size = os.path.getsize(expected_file_path)
        assert saved_size == binary_size, (
            f"File size mismatch: expected {binary_size}, got {saved_size}"
        )
        expected_md5 = hashlib.md5()
        with open(udf_so_path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                expected_md5.update(chunk)
        file_md5_ctx = hashlib.md5()
        with open(expected_file_path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                file_md5_ctx.update(chunk)
        saved_md5 = file_md5_ctx.hexdigest()
        assert saved_md5 == expected_md5.hexdigest(), (
            f"MD5 mismatch: expected {expected_md5.hexdigest()}, got {saved_md5}"
        )

        # --- Step 8: Execute a query using the loaded UDF and verify the result ---
        # TKvBodyReadActor calls LoadUdfs() after writing the file to disk, but the
        # function registry update may not be visible on the query layer immediately.
        # Poll until the query succeeds (the UDF module may take a moment to register).
        UDF_QUERY = 'SELECT Dicts::StrToInt("Sorted");'
        udf_query_result = [None]

        def try_udf_query():
            try:
                udf_query_result[0] = _run_query(driver_config, UDF_QUERY)
                return True
            except Exception as e:
                logger.debug("UDF query not ready yet: %s", e)
                return False

        assert _wait_for_condition(
            try_udf_query,
            timeout_seconds=60,
            description="Dicts UDF query execution",
        ), "UDF query did not succeed within timeout after the binary was written to disk"

        # Dicts::StrToInt("Sorted") returns a dict mapping number-word strings to ints,
        # e.g. {b'zero': 0, b'one': 1, ..., b'nine': 9}.
        rows = udf_query_result[0][0].rows
        assert len(rows) == 1, "UDF query returned wrong number of rows"
        result_value = list(rows[0].values())[0]
        assert isinstance(result_value, dict), (
            f"Dicts::StrToInt('Sorted') expected a dict, got {type(result_value)}: {result_value!r}"
        )
        assert result_value.get(b'zero') == 0, (
            f"Expected result_value[b'zero'] == 0, got {result_value!r}"
        )
        assert result_value.get(b'nine') == 9, (
            f"Expected result_value[b'nine'] == 9, got {result_value!r}"
        )
        logger.info("Test passed: dicts UDF (name=%s) appeared at %s and query returned %s",
                    udf_name, expected_file_path, result_value)

    finally:
        cluster.remove_database(database)
        cluster.unregister_and_stop_slots(db_nodes)
        cluster.stop()


def _write_manifest(name, module_type="library", module_kind="wasm", **fields):
    manifest = dict(module_name=name, module_type=module_type, module_kind=module_kind)
    if module_kind == "wasm":
        manifest["module_extension"] = "wat"
    manifest.update(fields)
    path = yatest.common.output_path(name + ".manifest.json")
    with open(path, "w") as output:
        json.dump(manifest, output)
    return path


def _ydb_cli_binary():
    return yatest.common.binary_path(os.environ["YDB_CLI_BINARY"])


def _run_ydb_udf(endpoint, database, *args):
    cmd = [
        _ydb_cli_binary(),
        "-e", endpoint,
        "-d", database,
        "experimental",
        "udf",
        *args,
    ]
    logger.info("Running ydb experimental udf: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.stderr:
        logger.info("ydb experimental udf stderr:\n%s", result.stderr.strip())
    if result.returncode != 0:
        raise RuntimeError(
            f"ydb experimental udf failed (rc={result.returncode}): stdout={result.stdout} stderr={result.stderr}"
        )
    return result.stdout


def _run_ydb_udf_expect_failure(endpoint, database, *args):
    """Run `ydb experimental udf` expecting a refusal; returns the combined output to match on."""
    with pytest.raises(RuntimeError) as failure:
        _run_ydb_udf(endpoint, database, *args)
    return str(failure.value)


def test_ydb_udf_cli_library_roundtrip():
    """
    Smoke-test the experimental UdfService CLI path used by `ydb experimental udf`:
    upload a LIBRARY → list → describe → delete.
    Does not wait for AOT compile; mutation only needs the store enabled.
    """
    database = "/Root/test"
    cluster = _make_cluster(enable_udf_store=True, enable_wasm_udf=True)
    db_nodes = _create_database(cluster, database)
    try:
        # The store lives in the tables of the database, and the service writes
        # them through the tenant it runs in, so the calls have to go to a node
        # serving this database rather than to the domain node.
        node = db_nodes[0]
        driver_config = ydb.DriverConfig(
            endpoint="%s:%s" % (node.host, node.port),
            database=database,
        )
        endpoint = "grpc://%s:%s" % (node.host, node.port)

        assert _wait_for_condition(
            lambda: _table_exists(driver_config, database),
            timeout_seconds=60,
            description="UDF metadata table creation at startup",
        )

        sdk_path = yatest.common.source_path(
            "ydb/tests/functional/udf_store/data/wasm/sdk_stub.wat"
        )
        library_name = "cli_sdk_stub"

        upload_out = _run_ydb_udf(
            endpoint,
            database,
            "upload",
            "--manifest", _write_manifest(library_name),
            "--file", sdk_path,
            "--format", "json",
        )
        upload = json.loads(upload_out)
        assert upload["name"] == library_name
        assert upload["uid"]
        assert upload["size"] > 0
        assert "compile_status" not in upload
        uid = upload["uid"]

        list_out = _run_ydb_udf(
            endpoint, database, "list", "--type", "library", "--format", "json"
        )
        listed = json.loads(list_out)
        names = {m["name"] for m in listed["modules"]}
        assert library_name in names

        describe_out = _run_ydb_udf(
            endpoint, database, "describe", "--name", library_name, "--format", "json"
        )
        described = json.loads(describe_out)
        assert described["module"]["name"] == library_name
        assert described["module"]["uid"] == uid
        assert described["module"]["module_type"] == "library"
        assert described["module"]["module_kind"] == "wasm"
        assert json.loads(described["manifest_json"])["module_name"] == library_name
        yaml_out = _run_ydb_udf(endpoint, database, "describe", "--name", library_name, "--format", "yaml")
        assert yaml.safe_load(yaml_out)["module"]["module_type"] == "library"
        assert "ModuleType" in _run_ydb_udf(endpoint, database, "list", "--format", "table")

        _run_ydb_udf(endpoint, database, "delete", "--name", library_name)

        package_name = "cli_sdk_package"
        package_path = yatest.common.output_path(package_name + ".zip")
        manifest_path = _write_manifest(package_name)
        with zipfile.ZipFile(package_path, "w", compression=zipfile.ZIP_DEFLATED) as package:
            package.write(manifest_path, "manifest.json")
            package.write(sdk_path, "sdk_stub.wat")
        package_upload = json.loads(_run_ydb_udf(
            endpoint, database, "upload", "--package", package_path, "--format", "json"
        ))
        assert package_upload["name"] == package_name
        assert package_upload["size"] == os.path.getsize(sdk_path)
        packaged = json.loads(_run_ydb_udf(
            endpoint, database, "describe", "--name", package_name, "--format", "json"
        ))
        with open(sdk_path, "rb") as package_body:
            package_md5 = hashlib.md5(package_body.read()).hexdigest()
        assert packaged["module"]["md5"] == package_md5
        assert json.loads(packaged["manifest_json"])["module_name"] == package_name
        _run_ydb_udf(endpoint, database, "delete", "--name", package_name)

        list_after = json.loads(
            _run_ydb_udf(endpoint, database, "list", "--type", "library", "--format", "json")
        )
        assert library_name not in {m["name"] for m in list_after["modules"]}
    finally:
        cluster.remove_database(database)
        cluster.unregister_and_stop_slots(db_nodes)
        cluster.stop()


def test_ydb_udf_cli_write_preconditions():
    """
    The conditions an upload or a delete is allowed to carry, all of which the
    service has to check inside the transaction that writes: write modes, the
    uid a caller compares against, and the name a UDF takes from its manifest.
    Also checks what a replace leaves behind: created_at stays, the chunks of
    the previous upload go, and the ones of the new upload are all there.
    """
    database = "/Root/test"
    cluster = _make_cluster(enable_udf_store=True, enable_wasm_udf=True)
    db_nodes = _create_database(cluster, database)
    try:
        node = db_nodes[0]
        driver_config = ydb.DriverConfig(
            endpoint="%s:%s" % (node.host, node.port),
            database=database,
        )
        endpoint = "grpc://%s:%s" % (node.host, node.port)

        assert _wait_for_condition(
            lambda: _table_exists(driver_config, database),
            timeout_seconds=60,
            description="UDF metadata table creation at startup",
        )

        data_dir = "ydb/tests/functional/udf_store/data/wasm"
        sdk_path = yatest.common.source_path("%s/sdk_stub.wat" % data_dir)
        udf_path = yatest.common.source_path("%s/local_udf.wat" % data_dir)
        manifest_path = yatest.common.source_path("%s/local_udf_manifest.json" % data_dir)

        def module_row(name):
            result = _run_query(
                driver_config,
                'SELECT uid, size, chunk_count, created_at FROM `{database}/{path}`'
                ' WHERE name = "{name}"'.format(
                    database=database, path=UDF_TABLE_MODULES_PATH, name=name
                ),
            )
            rows = result[0].rows
            return rows[0] if rows else None

        def chunk_stats(uid):
            result = _run_query(
                driver_config,
                'SELECT COUNT(*) AS cnt, SUM(LEN(data)) AS total FROM `{database}/{path}`'
                ' WHERE owner_key = "{uid}"'.format(
                    database=database, path=UDF_TABLE_MODULE_CHUNKS_PATH, uid=uid
                ),
            )
            row = result[0].rows[0]
            return row["cnt"], row["total"] or 0

        # --- Write modes and expected_uid on a library ---
        name = "precond_lib"
        created = json.loads(_run_ydb_udf(
            endpoint, database, "upload", "--manifest", _write_manifest(name),
            "--file", sdk_path, "--create-only", "--format", "json",
        ))
        first_uid = created["uid"]
        assert created["replaced_existing"] is False
        first_row = module_row(name)
        assert first_row["uid"] == first_uid

        # A name already taken is not a bad request but a name already taken.
        error = _run_ydb_udf_expect_failure(
            endpoint, database, "upload", "--manifest", _write_manifest(name),
            "--file", sdk_path, "--create-only",
        )
        assert "ALREADY_EXISTS" in error, error
        assert module_row(name)["uid"] == first_uid, "the refused upload changed the row"

        # A stale uid means someone else got there first, which is an abort.
        error = _run_ydb_udf_expect_failure(
            endpoint, database, "upload", "--manifest", _write_manifest(name),
            "--file", sdk_path, "--expected-uid", "0" * 32,
        )
        assert "ABORTED" in error, error
        assert module_row(name)["uid"] == first_uid

        replaced = json.loads(_run_ydb_udf(
            endpoint, database, "upload", "--manifest", _write_manifest(name),
            "--file", sdk_path, "--expected-uid", first_uid, "--format", "json",
        ))
        second_uid = replaced["uid"]
        assert second_uid != first_uid
        assert replaced["replaced_existing"] is True

        second_row = module_row(name)
        assert second_row["uid"] == second_uid
        # created_at describes the module, not the upload behind it right now.
        assert second_row["created_at"] == first_row["created_at"]

        # The body of the live uid is whole and the previous one is collected.
        count, total = chunk_stats(second_uid)
        assert count == second_row["chunk_count"], (count, second_row["chunk_count"])
        assert total == second_row["size"], (total, second_row["size"])
        assert chunk_stats(first_uid)[0] == 0, "chunks of the replaced upload were left behind"

        error = _run_ydb_udf_expect_failure(
            endpoint, database, "delete", "--name", name, "--expected-uid", first_uid,
        )
        assert "ABORTED" in error, error
        assert module_row(name) is not None

        _run_ydb_udf(endpoint, database, "delete", "--name", name, "--expected-uid", second_uid)
        assert module_row(name) is None
        assert chunk_stats(second_uid)[0] == 0, "delete left the chunks of the module behind"

        # --- Missing module against the modes that require one ---
        error = _run_ydb_udf_expect_failure(
            endpoint, database, "upload", "--manifest", _write_manifest("precond_absent"),
            "--file", sdk_path, "--replace-only",
        )
        assert "NOT_FOUND" in error, error

        error = _run_ydb_udf_expect_failure(
            endpoint, database, "delete", "--name", "precond_absent",
        )
        assert "NOT_FOUND" in error, error

        # --- A UDF is named by its manifest ---
        error = _run_ydb_udf_expect_failure(
            endpoint, database, "upload", "--manifest", manifest_path,
            "--file", udf_path, "--name", "NotTheManifestName",
        )
        assert "name" in error.lower(), error

        uploaded = json.loads(_run_ydb_udf(
            endpoint, database, "upload", "--manifest", manifest_path,
            "--file", udf_path, "--format", "json",
        ))
        assert uploaded["name"] == "LocalUdf", uploaded
        described = json.loads(_run_ydb_udf(
            endpoint, database, "describe", "--name", "LocalUdf", "--format", "json"
        ))
        assert described["module"]["module_type"] == "module"
        assert described["module"]["uid"] == uploaded["uid"]

        # A library cannot take over a name a UDF holds, whichever way round.
        error = _run_ydb_udf_expect_failure(
            endpoint, database, "upload", "--manifest", _write_manifest("LocalUdf"),
            "--file", sdk_path,
        )
        assert "PRECONDITION_FAILED" in error, error
        assert module_row("LocalUdf")["uid"] == uploaded["uid"]

        error = _run_ydb_udf_expect_failure(
            endpoint, database, "delete", "--name", "LocalUdf", "--type", "library",
        )
        assert "PRECONDITION_FAILED" in error, error
        assert module_row("LocalUdf") is not None
    finally:
        cluster.remove_database(database)
        cluster.unregister_and_stop_slots(db_nodes)
        cluster.stop()


def test_ydb_udf_cli_yaml_preserves_types():
    import concurrent.futures
    import grpc
    from ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc as discovery_grpc
    from ydb.public.api.grpc import ydb_udf_v1_pb2_grpc as udf_grpc
    from ydb.public.api.protos import ydb_discovery_pb2 as discovery
    from ydb.public.api.protos import ydb_udf_pb2 as udf
    from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

    names = ["true", "false", "yes", "on", "null", "~", "12345", "00123", "1e3",
             "2026-09-16", 'quotes" and\\slashes', "строка\nс переносом"]
    modules = [udf.ModuleInfo(
        name=name, module_type=udf.LIBRARY, module_kind=udf.WASM,
        uid="test-upload-uid", md5="00123456789012345678901234567890",
        size=8, version=2**64 - 1,
    ) for name in names]

    def response(response_type, result):
        value = response_type()
        value.operation.ready = True
        value.operation.status = StatusIds.SUCCESS
        value.operation.result.Pack(result)
        return value

    class DiscoveryService(discovery_grpc.DiscoveryServiceServicer):
        def ListEndpoints(self, request, context):
            return response(discovery.ListEndpointsResponse, discovery.ListEndpointsResult(
                endpoints=[discovery.EndpointInfo(address="127.0.0.1", port=port, node_id=1)]))

    class UdfService(udf_grpc.UdfServiceServicer):
        def ListModules(self, request, context):
            return response(udf.ListModulesResponse, udf.ListModulesResult(modules=modules))

        def DescribeModule(self, request, context):
            module = next(module for module in modules if module.name == request.name)
            return response(udf.DescribeModuleResponse, udf.DescribeModuleResult(
                module_info=module, manifest_json=json.dumps(dict(module_name=module.name)),
                platforms=[udf.PlatformCompileStatus(cpu_spec="12345", status=udf.FAILED,
                                                     compile_error='false\n"quoted"\\error')],
            ))

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        server = grpc.server(executor)
        port = server.add_insecure_port("127.0.0.1:0")
        discovery_grpc.add_DiscoveryServiceServicer_to_server(DiscoveryService(), server)
        udf_grpc.add_UdfServiceServicer_to_server(UdfService(), server)
        server.start()
        try:
            for command in [("list",)] + [("describe", "--name", name) for name in names]:
                json_value = json.loads(_run_ydb_udf(
                    "grpc://127.0.0.1:%d" % port, "/Root/test", *command, "--format", "json"))
                yaml_value = yaml.safe_load(_run_ydb_udf(
                    "grpc://127.0.0.1:%d" % port, "/Root/test", *command, "--format", "yaml"))
                assert yaml_value == json_value, (command, yaml_value, json_value)
                result_modules = yaml_value["modules"] if command[0] == "list" else [yaml_value["module"]]
                for module in result_modules:
                    assert isinstance(module["name"], str)
                    assert isinstance(module["size"], int)
                    assert isinstance(module["version"], int)
        finally:
            server.stop(0).wait()


def test_ydb_udf_rpc_validation_and_pagination():
    import concurrent.futures
    import grpc
    from ydb.public.api.grpc.ydb_udf_v1_pb2_grpc import UdfServiceStub
    from ydb.public.api.protos import ydb_udf_pb2 as udf
    from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

    database = "/Root/test"
    cluster = _make_cluster(enable_udf_store=True, enable_wasm_udf=True)
    db_nodes = _create_database(cluster, database)
    try:
        node = db_nodes[0]
        endpoint = "grpc://%s:%s" % (node.host, node.port)
        driver_config = ydb.DriverConfig(endpoint=endpoint, database=database)
        assert _wait_for_condition(lambda: _table_exists(driver_config, database))
        metadata = (("x-ydb-database", database),)
        body = b"\x00asm\x01\x00\x00\x00"
        with grpc.insecure_channel("%s:%s" % (node.host, node.port)) as channel:
            stub = UdfServiceStub(channel)

            def manifest(name="rpc_library", **overrides):
                result = dict(module_name=name, module_type="library", module_kind="wasm")
                result.update(overrides)
                return result

            def header(value, size=len(body), **params):
                return udf.UploadModuleChunk(metadata=udf.UploadModuleMetadata(
                    params=udf.UploadModuleParams(manifest_json=json.dumps(value), **params), total_size=size,
                ))

            def upload(messages, expected):
                responses = list(stub.UploadModule(iter(messages), metadata=metadata, timeout=60))
                assert len(responses) == 1
                operation = responses[0].operation
                assert operation.status == expected, operation
                result = udf.UploadModuleResult()
                if operation.status == StatusIds.SUCCESS:
                    assert operation.result.Unpack(result)
                return result

            for value in [
                {}, [], manifest(module_name=""), manifest(module_type="udf"),
                manifest(module_kind="WASM"), manifest(functions=[]), manifest(required_libraries=[]),
                manifest(module_type="module", functions="invalid"),
            ]:
                upload([header(value)], StatusIds.BAD_REQUEST)
            for module_type in ("module", "library"):
                upload([header(manifest(module_type=module_type, module_kind="native"))], StatusIds.PRECONDITION_FAILED)
            data = udf.UploadModuleChunk(data=body)
            for messages in [[], [data], [header(manifest(), 0)], [header(manifest()), header(manifest())],
                             [header(manifest(), len(body) + 1), data], [header(manifest(), 1), data],
                             [header(manifest()), udf.UploadModuleChunk()]]:
                upload(messages, StatusIds.BAD_REQUEST)
            upload([header(manifest(), 3), udf.UploadModuleChunk(data=b"bad")], StatusIds.BAD_REQUEST)
            upload([header(manifest(module_extension="wat")), data], StatusIds.BAD_REQUEST)
            upload([header(manifest(), expected_md5="0" * 32), data], StatusIds.PRECONDITION_FAILED)
            first = upload([header(manifest()), data], StatusIds.SUCCESS)
            assert first.md5 == hashlib.md5(body).hexdigest()

            def describe():
                response = stub.DescribeModule(udf.DescribeModuleRequest(name=first.name), metadata=metadata, timeout=60)
                assert response.operation.status == StatusIds.SUCCESS, response
                result = udf.DescribeModuleResult()
                assert response.operation.result.Unpack(result)
                return result

            assert json.loads(describe().manifest_json) == manifest()
            assert _wait_for_condition(lambda: bool(describe().platforms), description="known compile platform")
            upload([header(manifest(), 3), udf.UploadModuleChunk(data=b"bad")], StatusIds.BAD_REQUEST)
            assert describe().module_info.uid == first.uid

            upload([header(manifest(), write_mode=udf.CREATE_ONLY), data], StatusIds.ALREADY_EXISTS)
            upload([header(manifest(), expected_uid="stale-uid"), data], StatusIds.ABORTED)
            assert describe().module_info.uid == first.uid
            upload([header(manifest("absent"), write_mode=udf.REPLACE_ONLY), data], StatusIds.NOT_FOUND)
            stale_delete = stub.DeleteModule(udf.DeleteModuleRequest(
                name=first.name, expected_uid="stale-uid"), metadata=metadata, timeout=30)
            assert stale_delete.operation.status == StatusIds.ABORTED

            def replace():
                response = list(stub.UploadModule(iter([
                    header(manifest(), expected_uid=first.uid), data,
                ]), metadata=metadata, timeout=60))
                return response[0].operation.status
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                statuses = list(executor.map(lambda _: replace(), range(2)))
            assert sorted(statuses) == sorted([StatusIds.SUCCESS, StatusIds.ABORTED]), statuses

            # Exercise server pagination without depending on a client SDK or CLI.
            for index in range(101):
                upload([header(manifest("page_%03d" % index)), data], StatusIds.SUCCESS)
            names = []
            token = ""
            seen_tokens = set()
            while True:
                response = stub.ListModules(udf.ListModulesRequest(
                    type_filter=udf.LIBRARY, page_size=17, page_token=token), metadata=metadata, timeout=30)
                assert response.operation.status == StatusIds.SUCCESS, response
                page = udf.ListModulesResult()
                assert response.operation.result.Unpack(page)
                assert len(page.modules) <= 17
                names.extend(module.name for module in page.modules)
                token = page.next_page_token
                if not token:
                    break
                assert token not in seen_tokens
                seen_tokens.add(token)
            assert len(names) == len(set(names))
            assert {"page_%03d" % index for index in range(101)} <= set(names)
            assert seen_tokens
            native_delete = stub.DeleteModule(udf.DeleteModuleRequest(
                name=first.name, module_kind=udf.NATIVE), metadata=metadata, timeout=30)
            assert native_delete.operation.status == StatusIds.PRECONDITION_FAILED
            native = stub.ListModules(udf.ListModulesRequest(kind_filter=udf.NATIVE), metadata=metadata, timeout=30)
            assert native.operation.status == StatusIds.PRECONDITION_FAILED
            wrong_db = stub.ListModules(udf.ListModulesRequest(), metadata=(("x-ydb-database", "/Root"),), timeout=30)
            assert wrong_db.operation.status == StatusIds.BAD_REQUEST

    finally:
        cluster.remove_database(database)
        cluster.unregister_and_stop_slots(db_nodes)
        cluster.stop()


def test_ydb_udf_cli_pagination_and_streaming():
    import grpc
    from ydb.public.api.grpc.ydb_udf_v1_pb2_grpc import UdfServiceStub
    from ydb.public.api.protos import ydb_udf_pb2 as udf
    from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

    database = "/Root/test"
    cluster = _make_cluster(enable_udf_store=True, enable_wasm_udf=True)
    db_nodes = _create_database(cluster, database)
    try:
        node = db_nodes[0]
        endpoint = "grpc://%s:%s" % (node.host, node.port)
        driver_config = ydb.DriverConfig(endpoint=endpoint, database=database)
        assert _wait_for_condition(lambda: _table_exists(driver_config, database))
        metadata = (("x-ydb-database", database),)
        body = b"\x00asm\x01\x00\x00\x00"
        with grpc.insecure_channel("%s:%s" % (node.host, node.port)) as channel:
            stub = UdfServiceStub(channel)

            def manifest(name="rpc_library", **overrides):
                result = dict(module_name=name, module_type="library", module_kind="wasm")
                result.update(overrides)
                return result

            def header(value, size=len(body), **params):
                return udf.UploadModuleChunk(metadata=udf.UploadModuleMetadata(
                    params=udf.UploadModuleParams(manifest_json=json.dumps(value), **params), total_size=size,
                ))

            def upload(messages, expected):
                responses = list(stub.UploadModule(iter(messages), metadata=metadata, timeout=60))
                assert len(responses) == 1
                operation = responses[0].operation
                assert operation.status == expected, operation
                result = udf.UploadModuleResult()
                if operation.status == StatusIds.SUCCESS:
                    assert operation.result.Unpack(result)
                return result

            data = udf.UploadModuleChunk(data=body)
            first = upload([header(manifest()), data], StatusIds.SUCCESS)
            for command in [("list", "--kind", "NATIVE"), ("delete", "--name", first.name, "--kind", "NATIVE")]:
                assert "PRECONDITION_FAILED" in _run_ydb_udf_expect_failure(endpoint, database, *command)
            for kind in ("module", "library"):
                path = _write_manifest("native_" + kind, kind, "native")
                body_path = yatest.common.source_path("ydb/tests/functional/udf_store/data/wasm/sdk_stub.wat")
                assert "PRECONDITION_FAILED" in _run_ydb_udf_expect_failure(
                    endpoint, database, "upload", "--file", body_path, "--manifest", path)

            # Exceed the default RPC page size and ensure CLI retrieves all pages.
            for index in range(101):
                upload([header(manifest("page_%03d" % index)), data], StatusIds.SUCCESS)
            listed = json.loads(_run_ydb_udf(
                endpoint, database, "list", "--type", "library", "--kind", "WASM", "--format", "json"))
            assert {"page_%03d" % index for index in range(101)} <= {m["name"] for m in listed["modules"]}
            native = stub.ListModules(udf.ListModulesRequest(kind_filter=udf.NATIVE), metadata=metadata, timeout=30)
            assert native.operation.status == StatusIds.PRECONDITION_FAILED
            wrong_db = stub.ListModules(udf.ListModulesRequest(), metadata=(("x-ydb-database", "/Root"),), timeout=30)
            assert wrong_db.operation.status == StatusIds.BAD_REQUEST

            # Several SDK chunks; the filename deliberately disagrees with the
            # manifest's textual format. Verify all bytes reached the server.
            large_body = b"(module)\n(;" + b" " * (2 * 1024 * 1024) + b";)"
            large_path = yatest.common.output_path("multi_chunk.wasm")
            with open(large_path, "wb") as output:
                output.write(large_body)
            result = json.loads(_run_ydb_udf(
                endpoint, database, "upload", "--file", large_path,
                "--manifest", _write_manifest("multi_chunk"), "--format", "json"))
            assert result["size"] == len(large_body)
            assert result["md5"] == hashlib.md5(large_body).hexdigest()
            assert "Cannot read module file" in _run_ydb_udf_expect_failure(
                endpoint, database, "upload", "--file", large_path + ".missing",
                "--manifest", _write_manifest("missing_file"))
    finally:
        cluster.remove_database(database)
        cluster.unregister_and_stop_slots(db_nodes)
        cluster.stop()


@pytest.mark.parametrize("database_admin", [False, True])
def test_ydb_udf_administrator_access(database_admin):
    import grpc
    from ydb.public.api.grpc.ydb_udf_v1_pb2_grpc import UdfServiceStub
    from ydb.public.api.protos import ydb_udf_pb2 as udf
    from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

    database = "/Root/test"
    cluster = _make_cluster(enable_udf_store=True, enable_wasm_udf=True, database_admin=database_admin)
    db_nodes = []
    try:
        db_nodes = _create_database(cluster, database, token="root@builtin")
        node = db_nodes[0]
        endpoint = "%s:%s" % (node.host, node.port)
        config = ydb.DriverConfig(endpoint, database, credentials=ydb.AuthTokenCredentials("root@builtin"))
        assert _wait_for_condition(lambda: _table_exists(config, database))
        with ydb.Driver(config) as driver:
            driver.wait(timeout=30)
            with ydb.SessionPool(driver) as pool:
                with pool.checkout() as session:
                    session.execute_scheme('GRANT "ydb.generic.use" ON `%s` TO `ordinary@builtin`' % database)
            driver.scheme_client.modify_permissions(
                database, ydb.ModifyPermissionsSettings().change_owner("owner@builtin"))
        body = b"\x00asm\x01\x00\x00\x00"
        with grpc.insecure_channel(endpoint) as channel:
            stub = UdfServiceStub(channel)
            for user, allowed in [("root@builtin", True), ("owner@builtin", database_admin), ("ordinary@builtin", False)]:
                metadata = (("x-ydb-database", database), ("x-ydb-auth-ticket", user))
                expected = StatusIds.SUCCESS if allowed else StatusIds.UNAUTHORIZED
                name = user.split("@")[0]
                manifest = json.dumps(dict(module_name=name, module_type="library", module_kind="wasm"))
                chunks = [udf.UploadModuleChunk(metadata=udf.UploadModuleMetadata(
                    params=udf.UploadModuleParams(manifest_json=manifest), total_size=len(body))),
                    udf.UploadModuleChunk(data=body)]
                responses = list(stub.UploadModule(iter(chunks), metadata=metadata, timeout=60))
                assert len(responses) == 1
                assert responses[0].operation.status == expected, responses
                listed = stub.ListModules(udf.ListModulesRequest(), metadata=metadata, timeout=30)
                assert listed.operation.status == expected, listed
                described = stub.DescribeModule(udf.DescribeModuleRequest(name=name), metadata=metadata, timeout=60)
                assert described.operation.status == expected, described
                deleted = stub.DeleteModule(udf.DeleteModuleRequest(name=name), metadata=metadata, timeout=30)
                assert deleted.operation.status == expected, deleted
    finally:
        try:
            if db_nodes:
                cluster.remove_database(database, token="root@builtin")
        finally:
            if db_nodes:
                cluster.unregister_and_stop_slots(db_nodes)
            cluster.stop()


def test_ydb_udf_cli_large_upload():
    """Measure publish latency and client RSS without waiting for AOT."""
    database = "/Root/test"
    cluster = _make_cluster(enable_udf_store=True, enable_wasm_udf=True)
    db_nodes = _create_database(cluster, database)
    try:
        node = db_nodes[0]
        endpoint = "grpc://%s:%s" % (node.host, node.port)
        config = ydb.DriverConfig(endpoint=endpoint, database=database)
        assert _wait_for_condition(lambda: _table_exists(config, database))
        measurements = []
        for size_mib in (2, 64):
            # A large custom section is valid binary WASM and adds no AOT work.
            size = size_mib * 1024 * 1024
            leb_size = 3 if size_mib == 2 else 4
            section_size = size - 9 - leb_size  # header + section id + LEB128
            leb = []
            value = section_size
            while value:
                leb.append((value & 0x7f) | (0x80 if value > 0x7f else 0))
                value >>= 7
            assert len(leb) == leb_size
            body = b"\x00asm\x01\x00\x00\x00\x00" + bytes(leb) + b"\x00" + b" " * (section_size - 1)
            assert len(body) == size
            path = yatest.common.output_path("large_%d.wat" % size_mib)
            with open(path, "wb") as output:
                output.write(body)
            manifest = _write_manifest("large_%d" % size_mib, module_extension="wasm")
            args = [_ydb_cli_binary(), "-e", endpoint, "-d", database, "experimental", "udf", "upload",
                    "--file", path, "--manifest", manifest, "--format", "json"]
            started = time.monotonic()
            peak_rss_kib = 0
            with open(path + ".stdout", "w+") as stdout, open(path + ".stderr", "w+") as stderr:
                with subprocess.Popen(args, stdout=stdout, stderr=stderr) as process:
                    while process.poll() is None:
                        try:
                            with open("/proc/%d/status" % process.pid) as status:
                                for line in status:
                                    if line.startswith("VmHWM:"):
                                        peak_rss_kib = max(peak_rss_kib, int(line.split()[1]))
                        except FileNotFoundError:
                            pass
                        if time.monotonic() - started > 120:
                            process.kill()
                            pytest.fail("large upload timed out")
                        time.sleep(0.02)
                    stdout.seek(0)
                    stderr.seek(0)
                    assert process.returncode == 0, stderr.read()
                    result = json.load(stdout)
            elapsed = time.monotonic() - started
            assert result["size"] == size
            assert result["md5"] == hashlib.md5(body).hexdigest()
            measurements.append(dict(size_mib=size_mib, seconds=elapsed, peak_rss_kib=peak_rss_kib))
        # Keep measurements in test artifacts; wall-clock thresholds on a shared
        # CI host would be flaky. Both uploads use the same bounded chunk buffer.
        with open(yatest.common.output_path("upload_metrics.json"), "w") as output:
            json.dump(measurements, output, indent=2)
        logger.info("CLI upload measurements: %s", measurements)
    finally:
        cluster.remove_database(database)
        cluster.unregister_and_stop_slots(db_nodes)
        cluster.stop()


def _pad_wat(path, size):
    # Whitespace keeps compilation cheap while exercising multi-page source
    # and wasm_data artifact reads with a payload above the 48 MiB limit.
    padded_path = yatest.common.output_path("padded_" + os.path.basename(path))
    shutil.copyfile(path, padded_path)
    with open(padded_path, "ab") as output:
        remaining = size - output.tell()
        while remaining > 0:
            padding = b" " * min(remaining, 1024 * 1024)
            output.write(padding)
            remaining -= len(padding)
    return padded_path


@pytest.mark.parametrize("source_size", [0, 64 * 1024 * 1024, 75 * 1024 * 1024],
                         ids=["small", "full_pages", "partial_page"])
def test_using_wasm_udf(source_size):
    """
    Upload a WASM UDF (.wat) with JSON manifest into modules(+chunks) tables,
    wait for TUdfStoreService to compile and load from the artifact table, then query.
    """
    database = "/Root/test"
    cluster = _make_cluster(
        enable_udf_store=True,
        enable_wasm_udf=True,
    )
    db_nodes = _create_database(cluster, database)
    try:
        node = db_nodes[0]
        driver_config = ydb.DriverConfig(
            endpoint="%s:%s" % (node.host, node.port),
            database=database,
        )
        endpoint = "grpc://%s:%s" % (node.host, node.port)

        assert _wait_for_condition(
            lambda: _table_exists(driver_config, database),
            timeout_seconds=60,
            description="UDF metadata table creation at startup",
        )

        wasm_file_path = yatest.common.source_path(
            "ydb/tests/functional/udf_store/data/wasm/local_udf.wat"
        )
        manifest_path = yatest.common.source_path(
            "ydb/tests/functional/udf_store/data/wasm/local_udf_manifest.json"
        )
        if source_size:
            wasm_file_path = _pad_wat(wasm_file_path, source_size)

        assert _wait_for_condition(
            lambda: _kv_volume_exists(endpoint, database),
            timeout_seconds=60,
            description="KV volume creation at startup",
        )

        udf_name = _run_upload_udf(
            endpoint, database, wasm_file_path, udf_type="WASM", manifest_path=manifest_path
        )

        def _wasm_compile_ready():
            return _module_compile_ready(endpoint, database, udf_name)

        assert _wait_for_condition(
            _wasm_compile_ready,
            timeout_seconds=180,
            description="WASM UDF compile_status=ready for name=%s" % udf_name,
        ), "WASM UDF was not compiled within timeout"

        UDF_QUERY = "SELECT LocalUdf::udf_add(1, 2);"
        udf_query_result = [None]

        def try_wasm_query():
            try:
                udf_query_result[0] = _run_query(driver_config, UDF_QUERY)
                return True
            except Exception as e:
                logger.debug("WASM UDF query not ready yet: %s", e)
                return False

        assert _wait_for_condition(
            try_wasm_query,
            timeout_seconds=120,
            description="LocalUdf::udf_add query execution",
        ), "WASM UDF query did not succeed within timeout"

        rows = udf_query_result[0][0].rows
        assert len(rows) == 1
        result_value = list(rows[0].values())[0]
        assert result_value == 3, "Expected LocalUdf::udf_add(1, 2) == 3, got %r" % result_value

        # After marshalling, module static data at a low address must still be intact.
        cookie_result = _run_query(driver_config, "SELECT LocalUdf::udf_rodata_cookie();")
        cookie_rows = cookie_result[0].rows
        assert len(cookie_rows) == 1
        cookie_value = list(cookie_rows[0].values())[0]
        assert cookie_value == 0x0102030405060708, (
            "Expected LocalUdf::udf_rodata_cookie() == 0x0102030405060708, got %r" % cookie_value
        )

    finally:
        cluster.remove_database(database)
        cluster.unregister_and_stop_slots(db_nodes)
        cluster.stop()


@pytest.mark.parametrize("module_type", ["WASM", "LIBRARY"])
def test_wasm_chunk_query_error_is_persisted(module_type):
    import grpc
    from ydb.public.api.grpc.ydb_udf_v1_pb2_grpc import UdfServiceStub
    from ydb.public.api.protos import ydb_udf_pb2 as udf

    database = "/Root/test"
    cluster = _make_cluster(enable_udf_store=True, enable_wasm_udf=True)
    db_nodes = _create_database(cluster, database)
    try:
        node = db_nodes[0]
        config = ydb.DriverConfig(endpoint="%s:%s" % (node.host, node.port), database=database)
        assert _wait_for_condition(lambda: _table_exists(config, database))
        chunks_path = ".metadata/udf_store/module_chunks"
        assert _wait_for_condition(lambda: _table_exists(config, database, chunks_path))
        manifest_path = yatest.common.source_path(
            "ydb/tests/functional/udf_store/data/wasm/local_udf_manifest.json"
        )
        with open(manifest_path) as manifest_file:
            manifest = json.dumps(manifest_file.read())

        def publish(uid):
            _run_query(config, f'''UPSERT INTO `{database}/{UDF_TABLE_MODULES_PATH}`
                (name, type, uid, md5, size, chunk_count, version, manifest, created_at)
                VALUES ("LocalUdf", "{module_type}", "{uid}", "00000000000000000000000000000000",
                        1ul, 1ul, 1ul, CAST({manifest} AS Json), CurrentUtcTimestamp());''')

        with grpc.insecure_channel("%s:%s" % (node.host, node.port)) as channel:
            stub = UdfServiceStub(channel)

            def describe():
                response = stub.DescribeModule(
                    udf.DescribeModuleRequest(name="LocalUdf"),
                    metadata=(("x-ydb-database", database),),
                    timeout=30,
                )
                result = udf.DescribeModuleResult()
                assert response.operation.result.Unpack(result), response.operation
                return result

            # Database creation returns before its compile controller is always
            # available. First let a harmless corrupt-source attempt prove that
            # the controller and worker are connected; otherwise dropping the
            # chunks table can race startup and no compile is ever assigned.
            publish("controller-probe")
            assert _wait_for_condition(
                lambda: any(platform.status == udf.FAILED for platform in describe().platforms),
                timeout_seconds=180,
                description="compile controller readiness",
            )

            # Publish a new uid after removing the source table: ReadModuleChunks /
            # ReadLibraryChunks must fail as a query, before parsing or compiling.
            _run_query(config, f"DROP TABLE `{database}/{chunks_path}`")
            publish("missing-chunks")
            observed = {}

            def failed():
                result = describe()
                failed_platforms = [platform for platform in result.platforms if platform.status == udf.FAILED]
                if not failed_platforms:
                    return False
                observed["compile_error"] = failed_platforms[0].compile_error
                return True

            assert _wait_for_condition(failed, timeout_seconds=60), observed
        assert "YQL request failed" in observed["compile_error"], observed
        assert "step 3" in observed["compile_error"], observed
    finally:
        cluster.remove_database(database)
        cluster.unregister_and_stop_slots(db_nodes)
        cluster.stop()


def test_using_wasm_bridge_dict():
    """
    Upload sdk + a bridge-calling-convention WASM UDF (.wat) and run Dict lookup.
    """
    database = "/Root/test"
    cluster = _make_cluster(
        enable_udf_store=True,
        enable_wasm_udf=True,
    )
    db_nodes = _create_database(cluster, database)
    try:
        node = db_nodes[0]
        driver_config = ydb.DriverConfig(
            endpoint="%s:%s" % (node.host, node.port),
            database=database,
        )
        endpoint = "grpc://%s:%s" % (node.host, node.port)

        assert _wait_for_condition(
            lambda: _table_exists(driver_config, database),
            timeout_seconds=60,
            description="UDF metadata table creation at startup",
        )
        assert _wait_for_condition(
            lambda: _kv_volume_exists(endpoint, database),
            timeout_seconds=60,
            description="KV volume creation at startup",
        )

        data_dir = "ydb/tests/functional/udf_store/data/wasm"
        sdk_path = yatest.common.source_path("%s/sdk_stub.wat" % data_dir)
        udf_path = yatest.common.source_path("%s/bridge_dict_lookup.wat" % data_dir)
        manifest_path = yatest.common.source_path(
            "%s/bridge_dict_lookup_manifest.json" % data_dir
        )

        _run_upload_library(endpoint, database, sdk_path, "sdk")

        def _library_compile_ready(name):
            return _module_compile_ready(endpoint, database, name)

        assert _wait_for_condition(
            lambda: _library_compile_ready("sdk"),
            timeout_seconds=180,
            description="library sdk compile_status=ready",
        )

        udf_name = _run_upload_udf(
            endpoint, database, udf_path, udf_type="WASM", manifest_path=manifest_path
        )

        def _wasm_compile_ready():
            return _module_compile_ready(endpoint, database, udf_name)

        assert _wait_for_condition(
            _wasm_compile_ready,
            timeout_seconds=180,
            description="bridge WASM UDF compile_status=ready for name=%s" % udf_name,
        )

        udf_query = (
            'SELECT Unwrap(BridgeDict::Lookup(AsDict(AsTuple("a", 42l)), "a")) AS hit;'
        )
        udf_query_result = [None]

        def try_bridge_query():
            try:
                udf_query_result[0] = _run_query(driver_config, udf_query)
                return True
            except Exception as e:
                logger.debug("bridge WASM UDF query not ready yet: %s", e)
                return False

        assert _wait_for_condition(
            try_bridge_query,
            timeout_seconds=120,
            description="BridgeDict::Lookup query execution",
        )

        rows = udf_query_result[0][0].rows
        assert len(rows) == 1
        hit = list(rows[0].values())[0]
        assert hit == 42, "Expected BridgeDict::Lookup hit == 42, got %r" % hit

    finally:
        cluster.remove_database(database)
        cluster.unregister_and_stop_slots(db_nodes)
        cluster.stop()


@pytest.mark.parametrize("large_library", [False, True], ids=["small", "large_library"])
def test_using_wasm_udf_with_sdk_and_library(large_library):
    """
    Upload sdk + helpers libraries, then a WASM UDF that depends on both
    (required_libraries: ["sdk", "helpers"]), and run WithHelpers::scale(7).
    """
    database = "/Root/test"
    cluster = _make_cluster(
        enable_udf_store=True,
        enable_wasm_udf=True,
    )
    db_nodes = _create_database(cluster, database)
    try:
        node = db_nodes[0]
        driver_config = ydb.DriverConfig(
            endpoint="%s:%s" % (node.host, node.port),
            database=database,
        )
        endpoint = "grpc://%s:%s" % (node.host, node.port)

        assert _wait_for_condition(
            lambda: _table_exists(driver_config, database),
            timeout_seconds=60,
            description="UDF metadata table creation at startup",
        )
        assert _wait_for_condition(
            lambda: _kv_volume_exists(endpoint, database),
            timeout_seconds=60,
            description="KV volume creation at startup",
        )

        data_dir = "ydb/tests/functional/udf_store/data/wasm"
        sdk_path = yatest.common.source_path("%s/sdk_stub.wat" % data_dir)
        helpers_path = yatest.common.source_path("%s/helpers.wat" % data_dir)
        udf_path = yatest.common.source_path("%s/with_helpers.wat" % data_dir)
        manifest_path = yatest.common.source_path("%s/with_helpers_manifest.json" % data_dir)

        if large_library:
            helpers_path = _pad_wat(helpers_path, 75 * 1024 * 1024)

        _run_upload_library(endpoint, database, sdk_path, "sdk")
        _run_upload_library(endpoint, database, helpers_path, "helpers")

        def _library_compile_ready(name):
            return _module_compile_ready(endpoint, database, name)

        assert _wait_for_condition(
            lambda: _library_compile_ready("sdk"),
            timeout_seconds=180,
            description="library sdk compile_status=ready",
        )
        assert _wait_for_condition(
            lambda: _library_compile_ready("helpers"),
            timeout_seconds=180,
            description="library helpers compile_status=ready",
        )

        udf_name = _run_upload_udf(
            endpoint, database, udf_path, udf_type="WASM", manifest_path=manifest_path
        )

        def _wasm_compile_ready():
            return _module_compile_ready(endpoint, database, udf_name)

        assert _wait_for_condition(
            _wasm_compile_ready,
            timeout_seconds=180,
            description="WithHelpers WASM compile_status=ready for name=%s" % udf_name,
        ), "WithHelpers WASM UDF was not compiled within timeout"

        UDF_QUERY = "SELECT WithHelpers::scale(7);"
        udf_query_result = [None]

        def try_wasm_query():
            try:
                udf_query_result[0] = _run_query(driver_config, UDF_QUERY)
                return True
            except Exception as e:
                logger.debug("WithHelpers query not ready yet: %s", e)
                return False

        assert _wait_for_condition(
            try_wasm_query,
            timeout_seconds=120,
            description="WithHelpers::scale query execution",
        ), "WithHelpers WASM UDF query did not succeed within timeout"

        rows = udf_query_result[0][0].rows
        assert len(rows) == 1
        result_value = list(rows[0].values())[0]
        assert result_value == 21, "Expected WithHelpers::scale(7) == 21, got %r" % result_value

    finally:
        cluster.remove_database(database)
        cluster.unregister_and_stop_slots(db_nodes)
        cluster.stop()


def test_delete_wasm_udf_and_library():
    """
    Upload sdk + helpers + WithHelpers, verify the query works, then delete
    the module and libraries via upload_udf --action delete and assert they
    disappear from tables and the UDF is unloaded.
    """
    database = "/Root/test"
    cluster = _make_cluster(
        enable_udf_store=True,
        enable_wasm_udf=True,
    )
    db_nodes = _create_database(cluster, database)
    try:
        node = db_nodes[0]
        driver_config = ydb.DriverConfig(
            endpoint="%s:%s" % (node.host, node.port),
            database=database,
        )
        endpoint = "grpc://%s:%s" % (node.host, node.port)

        assert _wait_for_condition(
            lambda: _table_exists(driver_config, database),
            timeout_seconds=60,
            description="UDF metadata table creation at startup",
        )
        assert _wait_for_condition(
            lambda: _kv_volume_exists(endpoint, database),
            timeout_seconds=60,
            description="KV volume creation at startup",
        )

        data_dir = "ydb/tests/functional/udf_store/data/wasm"
        sdk_path = yatest.common.source_path("%s/sdk_stub.wat" % data_dir)
        helpers_path = yatest.common.source_path("%s/helpers.wat" % data_dir)
        udf_path = yatest.common.source_path("%s/with_helpers.wat" % data_dir)
        manifest_path = yatest.common.source_path("%s/with_helpers_manifest.json" % data_dir)

        _run_upload_library(endpoint, database, sdk_path, "sdk")
        _run_upload_library(endpoint, database, helpers_path, "helpers")

        def _library_compile_ready(name):
            return _module_compile_ready(endpoint, database, name)

        assert _wait_for_condition(
            lambda: _library_compile_ready("sdk"),
            timeout_seconds=180,
            description="library sdk compile_status=ready",
        )
        assert _wait_for_condition(
            lambda: _library_compile_ready("helpers"),
            timeout_seconds=180,
            description="library helpers compile_status=ready",
        )

        udf_name = _run_upload_udf(
            endpoint, database, udf_path, udf_type="WASM", manifest_path=manifest_path
        )

        def _wasm_compile_ready():
            return _module_compile_ready(endpoint, database, udf_name)

        assert _wait_for_condition(
            _wasm_compile_ready,
            timeout_seconds=180,
            description="WithHelpers WASM compile_status=ready for name=%s" % udf_name,
        )

        UDF_QUERY = "SELECT WithHelpers::scale(7);"
        udf_query_result = [None]

        def try_wasm_query():
            try:
                udf_query_result[0] = _run_query(driver_config, UDF_QUERY)
                return True
            except Exception as e:
                logger.debug("WithHelpers query not ready yet: %s", e)
                return False

        assert _wait_for_condition(
            try_wasm_query,
            timeout_seconds=120,
            description="WithHelpers::scale query execution before delete",
        )
        assert list(udf_query_result[0][0].rows[0].values())[0] == 21

        _run_delete_udf(endpoint, database, udf_name, udf_type="WASM")

        def _meta_row_gone():
            try:
                result = _run_query(
                    driver_config,
                    'SELECT COUNT(*) AS cnt FROM `{database}/{path}` WHERE name = "{name}"'.format(
                        database=database,
                        path=UDF_TABLE_MODULES_PATH,
                        name=udf_name,
                    ),
                )
                return result and result[0].rows and list(result[0].rows[0].values())[0] == 0
            except Exception as e:
                logger.debug("meta delete check failed: %s", e)
                return False

        assert _wait_for_condition(
            _meta_row_gone,
            timeout_seconds=30,
            description="meta row deleted for name=%s" % udf_name,
        )

        def _udf_unloaded():
            try:
                _run_query(driver_config, UDF_QUERY)
                return False
            except Exception as e:
                logger.info("expected failure after UDF delete: %s", e)
                return True

        assert _wait_for_condition(
            _udf_unloaded,
            timeout_seconds=60,
            description="WithHelpers unloaded after meta delete",
        ), "WithHelpers query still succeeded after module delete"

        _run_delete_library(endpoint, database, "helpers")
        _run_delete_library(endpoint, database, "sdk")

        def _library_gone(name):
            try:
                result = _run_query(
                    driver_config,
                    'SELECT COUNT(*) AS cnt FROM `{database}/{path}` WHERE name = "{name}" AND type = "LIBRARY"'.format(
                        database=database,
                        path=UDF_TABLE_MODULES_PATH,
                        name=name,
                    ),
                )
                return result and result[0].rows and list(result[0].rows[0].values())[0] == 0
            except Exception as e:
                logger.debug("library delete check failed for %s: %s", name, e)
                return False

        assert _wait_for_condition(
            lambda: _library_gone("helpers") and _library_gone("sdk"),
            timeout_seconds=30,
            description="library module rows deleted",
        )
        logger.info("Test passed: deleted UDF name=%s and libraries sdk/helpers", udf_name)

    finally:
        cluster.remove_database(database)
        cluster.unregister_and_stop_slots(db_nodes)
        cluster.stop()


# ---------------------------------------------------------------------------
# Feature-flag test: parametrised over udf_store_config enabled / disabled
# ---------------------------------------------------------------------------

_SETTLE_TIMEOUT = 60   # seconds to wait when flag is ON
_ABSENT_TIMEOUT = 15   # seconds to confirm absence when flag is OFF


def _make_cluster(
    enable_udf_store: bool,
    enable_native_udf: bool = False,
    native_udf_dir: str = "",
    enable_wasm_udf: bool = False,
    database_admin=None,
):
    # Dynamic-node bootstrap uses unauthenticated infrastructure RPCs. Keep
    # token enforcement at its default; the explicit administrator SID list
    # still exercises UdfService authorization for every caller below.
    auth_options = dict(default_clusteradmin="root@builtin") if database_admin is not None else {}
    configurator = KikimrConfigGenerator(
        **auth_options,
        additional_log_configs={"METADATA_PROVIDER": 7},
        # WASM UDFs are only ever compiled by the per-database
        # WasmCompileController tablet, which this flag creates.
        extra_feature_flags=["enable_wasm_compile_controller"] if enable_wasm_udf else None,
        # The public UdfService is off unless the endpoint lists it, and the
        # harness default list does not.
        extra_grpc_services=["udf"],
    )
    if database_admin is not None:
        configurator.yaml_config.setdefault("feature_flags", {})["enable_database_admin"] = database_admin
    if enable_udf_store:
        udf_store_config = {"enabled": True, "kv_storage_media": "hdd"}
        if enable_native_udf:
            udf_store_config["enable_unsafe_native_udf"] = True
            udf_store_config["unsafe_native_udf_dir"] = native_udf_dir
        if enable_wasm_udf:
            udf_store_config["enable_wasm_udf"] = True
        configurator.yaml_config["udf_store_config"] = udf_store_config
    cluster = KiKiMR(configurator=configurator)
    cluster.start()
    return cluster


def _create_database(cluster, database, token=None):
    cluster.create_database(database, storage_pool_units_count={"hdd": 1}, token=token)
    nodes = cluster.register_and_start_slots(database, count=1)
    cluster.wait_tenant_up(database, token=token)
    return nodes
