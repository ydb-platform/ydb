# -*- coding: utf-8 -*-
import copy
import logging
import os

import pytest
from hamcrest import assert_that, equal_to

import ydb.public.api.protos.ydb_cms_pb2 as cms_pb
from ydb.tests.library.common.wait_for import wait_for_and_assert
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)

# local configuration for the ydb cluster (fetched by ydb_cluster_configuration fixture)
CLUSTER_CONFIG = dict(
    additional_log_configs={
        # more logs
        'CMS_TENANTS': LogLevels.TRACE,
        'HIVE': LogLevels.TRACE,
        # less logs
        'KQP_PROXY': LogLevels.CRIT,
        'KQP_WORKER': LogLevels.CRIT,
        'KQP_GATEWAY': LogLevels.CRIT,
        'GRPC_PROXY': LogLevels.ERROR,
        'TX_DATASHARD': LogLevels.ERROR,
        'TX_PROXY_SCHEME_CACHE': LogLevels.ERROR,
        'KQP_YQL': LogLevels.ERROR,
        'KQP_SESSION': LogLevels.CRIT,
        'KQP_COMPILE_ACTOR': LogLevels.CRIT,
        'PERSQUEUE_CLUSTER_TRACKER': LogLevels.CRIT,
        'SCHEME_BOARD_SUBSCRIBER': LogLevels.CRIT,
        'SCHEME_BOARD_POPULATOR': LogLevels.CRIT,
    },
    extra_feature_flags=[
        'enable_cut_history'
    ],
    # do not clutter logs with resource pools auto creation
    enable_resource_pools=False,
    hive_config={
        'cut_history_allow_list': ''  # allow all
    },
)


# fixtures.ydb_cluster_configuration local override
@pytest.fixture(scope='module')
def ydb_cluster_configuration():
    conf = copy.deepcopy(CLUSTER_CONFIG)
    return conf


def test_remove_storage_group(ydb_cluster, ydb_root, ydb_safe_test_name, ydb_client):
    database = os.path.join(ydb_root, ydb_safe_test_name)

    ydb_cluster.create_database(
        database,
        storage_pool_units_count={'hdd': 1},
    )
    database_nodes = ydb_cluster.register_and_start_slots(database, count=1)
    ydb_cluster.wait_tenant_up(database)

    ydb_cluster.alter_database(
        database,
        storage_units_to_add={'hdd': 2},
    )

    driver = ydb_client(database)
    with ydb.QuerySessionPool(driver, size=1) as pool:
        table_name = f"`{database}/table`"
        pool.execute_with_retries("CREATE TABLE " + table_name + " (key Int32, value String, primary key(key));")

        pool.execute_with_retries("""UPSERT INTO """ + table_name + """ (key, value) VALUES (1, "value1");""")

    ydb_cluster.alter_database(
        database,
        storage_units_to_remove={'hdd': 2},
    )

    ydb_cluster.restart_slots()

    def get_storage_units():
        status = ydb_cluster.get_database_status(database)
        units = sum([unit.count for unit in status.allocated_resources.storage_units])
        return units

    wait_for_and_assert(
        get_storage_units,
        equal_to(1),
        timeout_seconds=120,
        step_seconds=10,
    )

    status = ydb_cluster.get_database_status(database)
    assert_that(status.state, equal_to(cms_pb.GetDatabaseStatusResult.RUNNING))

    ydb_cluster.unregister_and_stop_slots(database_nodes)
    ydb_cluster.remove_database(database)
