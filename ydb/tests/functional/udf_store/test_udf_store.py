# -*- coding: utf-8 -*-
import hashlib
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
from ydb.tests.functional.udf_store.lib.constants import UDF_TABLE_META_PATH, UDF_KV_BINARIES_PATH

logger = logging.getLogger(__name__)

CLUSTER_CONFIG = dict(
    additional_log_configs={
        "METADATA_PROVIDER": 7,  # DEBUG
    }
)

UDF_OUTPUT_DIR = "/tmp/ydb_udfs"


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


def _table_exists(config, database, table_path=UDF_TABLE_META_PATH):
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
                % (UDF_TABLE_META_PATH, _SETTLE_TIMEOUT)
            )
            assert kv_appeared, (
                "KV volume `%s` was NOT created within %ds when udf_store_config.enabled=true"
                % (UDF_KV_BINARIES_PATH, _SETTLE_TIMEOUT)
            )
        else:
            assert not table_appeared, (
                "UDF metadata table `%s` appeared even though udf_store_config is disabled" % UDF_TABLE_META_PATH
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


def _run_upload_udf(endpoint, database, udf_so_path):
    """
    Invoke the upload_udf binary as a subprocess.

    Returns the md5 hex-digest printed by the binary on stdout.
    Raises RuntimeError if the binary exits with a non-zero code.
    """
    cmd = [
        _upload_udf_binary(),
        "--endpoint", endpoint,
        "--database", database,
        "--udf-file", udf_so_path,
    ]
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


def test_using_native_unsafe_udf():
    """
    1. Use the pre-built dicts UDF shared library as the binary to upload.
    2. Delegate upload + metadata registration to the upload_udf helper binary.
    3. TUdfStoreService detects the new metadata row, fetches the binary
       from the KV store, and writes it to UnsafeNativeUdfDir/<md5>.
    4. Assert that the file exists, its size and md5 match metadata.
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
        udf_md5 = _run_upload_udf(endpoint, database, udf_so_path)
        logger.info("upload_udf reported md5=%s", udf_md5)

        # --- Step 6: Wait for binary to appear in UnsafeNativeUdfDir ---
        # TKvBodyReadActor names the output file after the md5 checksum.
        expected_file_path = os.path.join(udf_output_dir, udf_md5)
        assert _wait_for_condition(
            lambda: os.path.isfile(expected_file_path),
            timeout_seconds=120,
            description=f"native UDF file {expected_file_path}",
        ), (
            f"Native UDF file was not created at {expected_file_path} within timeout. "
            f"Expected TUdfStoreService to fetch the binary from KV and write it to "
            f"UnsafeNativeUdfDir='{udf_output_dir}' under filename=md5='{udf_md5}'."
        )

        # --- Step 7: Verify file size and md5 ---
        CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB
        binary_size = os.path.getsize(udf_so_path)
        saved_size = os.path.getsize(expected_file_path)
        assert saved_size == binary_size, (
            f"File size mismatch: expected {binary_size}, got {saved_size}"
        )
        file_md5_ctx = hashlib.md5()
        with open(expected_file_path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                file_md5_ctx.update(chunk)
        saved_md5 = file_md5_ctx.hexdigest()
        assert saved_md5 == udf_md5, (
            f"MD5 mismatch: expected {udf_md5}, got {saved_md5}"
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
        assert len(rows) == 1, "UDF query returned wrong number no rows"
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
        logger.info("Test passed: dicts UDF (md5=%s) appeared at %s and query returned %s",
                    udf_md5, expected_file_path, result_value)

    finally:
        cluster.remove_database(database)
        cluster.unregister_and_stop_slots(db_nodes)
        cluster.stop()


# ---------------------------------------------------------------------------
# Feature-flag test: parametrised over udf_store_config enabled / disabled
# ---------------------------------------------------------------------------

_SETTLE_TIMEOUT = 60   # seconds to wait when flag is ON
_ABSENT_TIMEOUT = 15   # seconds to confirm absence when flag is OFF


def _make_cluster(enable_udf_store: bool, enable_native_udf: bool = False, native_udf_dir: str = ""):
    configurator = KikimrConfigGenerator(
        additional_log_configs={"METADATA_PROVIDER": 7},
    )
    if enable_udf_store:
        udf_store_config = {"enabled": True, "kv_storage_media": "hdd"}
        if enable_native_udf:
            udf_store_config["enable_unsafe_native_udf"] = True
            udf_store_config["unsafe_native_udf_dir"] = native_udf_dir
        configurator.yaml_config["udf_store_config"] = udf_store_config
    cluster = KiKiMR(configurator=configurator)
    cluster.start()
    return cluster


def _create_database(cluster, database):
    cluster.create_database(database, storage_pool_units_count={"hdd": 1})
    nodes = cluster.register_and_start_slots(database, count=1)
    cluster.wait_tenant_up(database)
    return nodes
