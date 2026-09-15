# -*- coding: utf-8 -*-
import hashlib
import json
import logging
import os
import shutil
import subprocess
import time

import pytest
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
    """Upload a WASM library via upload_udf --kind library."""
    return _run_upload_udf(
        endpoint,
        database,
        udf_file_path=library_file_path,
        udf_type="WASM",
        kind="library",
        library_name=library_name,
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


def _ydb_cli_binary():
    return yatest.common.binary_path(os.environ["YDB_CLI_BINARY"])


def _run_ydb_udf(endpoint, database, *args):
    cmd = [
        _ydb_cli_binary(),
        "-e", endpoint,
        "-d", database,
        "udf",
        *args,
    ]
    logger.info("Running ydb udf: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    if result.stderr:
        logger.info("ydb udf stderr:\n%s", result.stderr.strip())
    if result.returncode != 0:
        raise RuntimeError(
            f"ydb udf failed (rc={result.returncode}): stdout={result.stdout} stderr={result.stderr}"
        )
    return result.stdout


def _run_ydb_udf_expect_failure(endpoint, database, *args):
    """Run `ydb udf` expecting a refusal; returns the combined output to match on."""
    with pytest.raises(RuntimeError) as failure:
        _run_ydb_udf(endpoint, database, *args)
    return str(failure.value)


def test_ydb_udf_cli_library_roundtrip():
    """
    Smoke-test the public UdfService path used by `ydb udf`:
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
            "--kind", "library",
            "--name", library_name,
            "--file", sdk_path,
            "--format", "json",
        )
        upload = json.loads(upload_out)
        assert upload["name"] == library_name
        assert upload["uid"]
        assert upload["size"] > 0
        assert upload["compile_status"] == "pending"
        uid = upload["uid"]

        list_out = _run_ydb_udf(
            endpoint, database, "list", "--kind", "library", "--format", "json"
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
        assert described["module"]["kind"] == "library"

        _run_ydb_udf(endpoint, database, "delete", "--name", library_name)

        list_after = json.loads(
            _run_ydb_udf(endpoint, database, "list", "--kind", "library", "--format", "json")
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
            endpoint, database, "upload", "--kind", "library", "--name", name,
            "--file", sdk_path, "--create-only", "--format", "json",
        ))
        first_uid = created["uid"]
        assert created["replaced_existing"] is False
        first_row = module_row(name)
        assert first_row["uid"] == first_uid

        # A name already taken is not a bad request but a name already taken.
        error = _run_ydb_udf_expect_failure(
            endpoint, database, "upload", "--kind", "library", "--name", name,
            "--file", sdk_path, "--create-only",
        )
        assert "ALREADY_EXISTS" in error, error
        assert module_row(name)["uid"] == first_uid, "the refused upload changed the row"

        # A stale uid means someone else got there first, which is an abort.
        error = _run_ydb_udf_expect_failure(
            endpoint, database, "upload", "--kind", "library", "--name", name,
            "--file", sdk_path, "--expected-uid", "0" * 32,
        )
        assert "ABORTED" in error, error
        assert module_row(name)["uid"] == first_uid

        replaced = json.loads(_run_ydb_udf(
            endpoint, database, "upload", "--kind", "library", "--name", name,
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
            endpoint, database, "upload", "--kind", "library", "--name", "precond_absent",
            "--file", sdk_path, "--replace-only",
        )
        assert "NOT_FOUND" in error, error

        error = _run_ydb_udf_expect_failure(
            endpoint, database, "delete", "--name", "precond_absent",
        )
        assert "NOT_FOUND" in error, error

        # --- A UDF is named by its manifest ---
        error = _run_ydb_udf_expect_failure(
            endpoint, database, "upload", "--kind", "udf", "--manifest", manifest_path,
            "--file", udf_path, "--name", "NotTheManifestName",
        )
        assert "manifest" in error, error

        uploaded = json.loads(_run_ydb_udf(
            endpoint, database, "upload", "--kind", "udf", "--manifest", manifest_path,
            "--file", udf_path, "--format", "json",
        ))
        assert uploaded["name"] == "LocalUdf", uploaded
        described = json.loads(_run_ydb_udf(
            endpoint, database, "describe", "--name", "LocalUdf", "--format", "json"
        ))
        assert described["module"]["kind"] == "udf"
        assert described["module"]["uid"] == uploaded["uid"]

        # A library cannot take over a name a UDF holds, whichever way round.
        error = _run_ydb_udf_expect_failure(
            endpoint, database, "upload", "--kind", "library", "--name", "LocalUdf",
            "--file", sdk_path,
        )
        assert "PRECONDITION_FAILED" in error, error
        assert module_row("LocalUdf")["uid"] == uploaded["uid"]

        error = _run_ydb_udf_expect_failure(
            endpoint, database, "delete", "--name", "LocalUdf", "--kind", "library",
        )
        assert "PRECONDITION_FAILED" in error, error
        assert module_row("LocalUdf") is not None
    finally:
        cluster.remove_database(database)
        cluster.unregister_and_stop_slots(db_nodes)
        cluster.stop()


def test_using_wasm_udf():
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
        node = cluster.nodes[1]
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

        assert _wait_for_condition(
            lambda: _kv_volume_exists(endpoint, database),
            timeout_seconds=60,
            description="KV volume creation at startup",
        )

        udf_name = _run_upload_udf(
            endpoint, database, wasm_file_path, udf_type="WASM", manifest_path=manifest_path
        )

        def _wasm_compile_ready():
            try:
                result = _run_query(
                    driver_config,
                    'SELECT compile_status FROM `{database}/{path}` WHERE name = "{name}"'.format(
                        database=database,
                        path=UDF_TABLE_MODULES_PATH,
                        name=udf_name,
                    ),
                )
                if not result or not result[0].rows:
                    return False
                return list(result[0].rows[0].values())[0] == "ready"
            except Exception as e:
                logger.debug("WASM compile status not ready yet: %s", e)
                return False

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
        node = cluster.nodes[1]
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
            try:
                result = _run_query(
                    driver_config,
                    'SELECT compile_status FROM `{database}/{path}` WHERE name = "{name}" AND type = "LIBRARY"'.format(
                        database=database,
                        path=UDF_TABLE_MODULES_PATH,
                        name=name,
                    ),
                )
                if not result or not result[0].rows:
                    return False
                return list(result[0].rows[0].values())[0] == "ready"
            except Exception as e:
                logger.debug("library %s compile status not ready yet: %s", name, e)
                return False

        assert _wait_for_condition(
            lambda: _library_compile_ready("sdk"),
            timeout_seconds=180,
            description="library sdk compile_status=ready",
        )

        udf_name = _run_upload_udf(
            endpoint, database, udf_path, udf_type="WASM", manifest_path=manifest_path
        )

        def _wasm_compile_ready():
            try:
                result = _run_query(
                    driver_config,
                    'SELECT compile_status FROM `{database}/{path}` WHERE name = "{name}"'.format(
                        database=database,
                        path=UDF_TABLE_MODULES_PATH,
                        name=udf_name,
                    ),
                )
                if not result or not result[0].rows:
                    return False
                return list(result[0].rows[0].values())[0] == "ready"
            except Exception as e:
                logger.debug("WASM compile status not ready yet: %s", e)
                return False

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


def test_using_wasm_udf_with_sdk_and_library():
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
        node = cluster.nodes[1]
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
            try:
                result = _run_query(
                    driver_config,
                    'SELECT compile_status FROM `{database}/{path}` WHERE name = "{name}" AND type = "LIBRARY"'.format(
                        database=database,
                        path=UDF_TABLE_MODULES_PATH,
                        name=name,
                    ),
                )
                if not result or not result[0].rows:
                    return False
                return list(result[0].rows[0].values())[0] == "ready"
            except Exception as e:
                logger.debug("library %s compile status not ready yet: %s", name, e)
                return False

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
            try:
                result = _run_query(
                    driver_config,
                    'SELECT compile_status FROM `{database}/{path}` WHERE name = "{name}"'.format(
                        database=database,
                        path=UDF_TABLE_MODULES_PATH,
                        name=udf_name,
                    ),
                )
                if not result or not result[0].rows:
                    return False
                return list(result[0].rows[0].values())[0] == "ready"
            except Exception as e:
                logger.debug("WASM compile status not ready yet: %s", e)
                return False

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
        node = cluster.nodes[1]
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
            try:
                result = _run_query(
                    driver_config,
                    'SELECT compile_status FROM `{database}/{path}` WHERE name = "{name}" AND type = "LIBRARY"'.format(
                        database=database,
                        path=UDF_TABLE_MODULES_PATH,
                        name=name,
                    ),
                )
                if not result or not result[0].rows:
                    return False
                return list(result[0].rows[0].values())[0] == "ready"
            except Exception as e:
                logger.debug("library %s compile status not ready yet: %s", name, e)
                return False

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
            try:
                result = _run_query(
                    driver_config,
                    'SELECT compile_status FROM `{database}/{path}` WHERE name = "{name}"'.format(
                        database=database,
                        path=UDF_TABLE_MODULES_PATH,
                        name=udf_name,
                    ),
                )
                if not result or not result[0].rows:
                    return False
                return list(result[0].rows[0].values())[0] == "ready"
            except Exception as e:
                logger.debug("WASM compile status not ready yet: %s", e)
                return False

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
):
    configurator = KikimrConfigGenerator(
        additional_log_configs={"METADATA_PROVIDER": 7},
        # WASM UDFs are only ever compiled by the per-database
        # WasmCompileController tablet, which this flag creates.
        extra_feature_flags=["enable_wasm_compile_controller"] if enable_wasm_udf else None,
        # The public UdfService is off unless the endpoint lists it, and the
        # harness default list does not.
        extra_grpc_services=["udf"],
    )
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


def _create_database(cluster, database):
    cluster.create_database(database, storage_pool_units_count={"hdd": 1})
    nodes = cluster.register_and_start_slots(database, count=1)
    cluster.wait_tenant_up(database)
    return nodes
