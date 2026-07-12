# -*- coding: utf-8 -*-
import logging

import pytest

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.oss.ydb_sdk_import import ydb

pytest_plugins = ["ydb.tests.library.fixtures"]

logger = logging.getLogger(__name__)

ROOT = "/Root"
DATABASE = f"{ROOT}/test"

# Directory where native UDFs are written by TKvBodyReadActor.
# Must match UnsafeNativeUdfDir in the cluster config.
UDF_OUTPUT_DIR = "/tmp/ydb_udfs"

CLUSTER_CONFIG = dict(
    additional_log_configs={
        "METADATA_PROVIDER": 7,  # DEBUG
    }
)


@pytest.fixture(scope="module")
def ydb_configurator(ydb_cluster_configuration):
    """Override to inject TUdfStoreConfig with Enabled=true and EnableUnsafeNativeUdf=true."""
    configurator = KikimrConfigGenerator(**ydb_cluster_configuration)
    configurator.yaml_config["udf_store_config"] = {
        "enabled": True,
        "kv_storage_media": "hdd",
        "enable_unsafe_native_udf": True,
        "unsafe_native_udf_dir": UDF_OUTPUT_DIR,
    }
    return configurator


@pytest.fixture(scope="module")
def db_fixture(ydb_cluster):
    ydb_cluster.create_database(DATABASE, storage_pool_units_count={"hdd": 1})
    database_nodes = ydb_cluster.register_and_start_slots(DATABASE, count=1)
    ydb_cluster.wait_tenant_up(DATABASE)

    config = ydb.DriverConfig(
        endpoint="%s:%s" % (ydb_cluster.nodes[1].host, ydb_cluster.nodes[1].port),
        database=DATABASE,
    )

    yield config

    ydb_cluster.remove_database(DATABASE)
    ydb_cluster.unregister_and_stop_slots(database_nodes)


@pytest.fixture(scope="module")
def udf_output_dir():
    """Output directory for native UDF binaries (UnsafeNativeUdfDir in cluster config)."""
    return "/tmp/ydb_udfs"
