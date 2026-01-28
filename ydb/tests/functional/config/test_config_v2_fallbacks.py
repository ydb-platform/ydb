# -*- coding: utf-8 -*-
import logging
import pytest
import yaml
from hamcrest import assert_that, is_

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.clients.kikimr_config_client import ConfigClient
from ydb.tests.library.clients.kikimr_dynconfig_client import DynConfigClient
from ydb.tests.library.harness.util import LogLevels
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
import ydb.public.api.protos.draft.ydb_dynamic_config_pb2 as dynconfig

logger = logging.getLogger(__name__)

@pytest.fixture(scope="module")
def erasure():
    return Erasure.NONE

@pytest.fixture(scope="module")
def nodes_count():
    return 1

@pytest.fixture(scope="module")
def configurator(erasure, nodes_count):
    log_configs = {
        'BS_NODE': LogLevels.DEBUG,
        'GRPC_SERVER': LogLevels.DEBUG,
        'GRPC_PROXY': LogLevels.DEBUG,
        'BS_CONTROLLER': LogLevels.DEBUG,
    }
    return KikimrConfigGenerator(
        erasure=erasure,
        nodes=nodes_count,
        use_in_memory_pdisks=True,
        separate_node_configs=True,
        simple_config=True,
        extra_grpc_services=['config'],
        additional_log_configs=log_configs,
        explicit_hosts_and_host_configs=True,
    )

@pytest.fixture(scope="module")
def cluster(configurator):
    cluster = KiKiMR(configurator=configurator)
    cluster.start()
    yield cluster
    cluster.stop()

@pytest.fixture(scope="module")
def config_client(cluster):
    host = cluster.nodes[1].host
    port = cluster.nodes[1].port
    return ConfigClient(host, port)

@pytest.fixture(scope="module")
def dynconfig_client(cluster):
    host = cluster.nodes[1].host
    port = cluster.nodes[1].port
    return DynConfigClient(host, port)

def test_v2_commands_on_v1_cluster(cluster, config_client, dynconfig_client):
    resp = dynconfig_client.fetch_startup_config()
    result = dynconfig.FetchStartupConfigResult()
    resp.operation.result.Unpack(result)
    startup_config = result.config
    assert_that(resp.operation.status, is_(StatusIds.SUCCESS))
    logger.info("Successfully fetched startup config via DynConfig")

    # try V2 FetchConfig
    # we expect this to return UNSUPPORTED status because V2 is not enabled via feature flag
    resp = config_client.fetch_all_configs()
    logger.info(f"V2 FetchConfig response on V1 cluster: {resp}")
    assert_that(resp.operation.status, is_(StatusIds.UNSUPPORTED))
    assert_that(resp.operation.issues[0].message, is_("failed to fetch config: configuration v2 is disabled"))

    # try V2 ReplaceConfig
    # we expect this to return UNSUPPORTED status because V2 is not enabled via feature flag
    yaml_config = yaml.safe_load(startup_config)
    replace_config = dict()
    replace_config["metadata"] = {
        "kind": "MainConfig",
        "version": 0,
        "cluster": ""
    }
    replace_config["config"] = yaml_config
    dumped_replace_config = yaml.dump(replace_config)
    logger.debug("Config %s", dumped_replace_config)
    resp = config_client.replace_config(dumped_replace_config)
    logger.info(f"V2 ReplaceConfig response on V1 cluster: {resp}")

    assert_that(resp.operation.status, is_(StatusIds.UNSUPPORTED))
    assert_that(resp.operation.issues[0].message, is_("failed to replace configuration: InvalidRequest: configuration v2 is disabled"))
