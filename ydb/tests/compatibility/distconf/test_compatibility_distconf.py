# -*- coding: utf-8 -*-
import pytest
import logging
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.library.clients.kikimr_config_client import ConfigClient
from ydb.tests.library.clients.kikimr_dynconfig_client import DynConfigClient
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.common.types import Erasure

logger = logging.getLogger(__name__)


class TestCompatibilityDistConf(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if len(self.versions) < 2:
            pytest.skip("Test requires two versions")

        initial_version = self.versions[0]
        final_version = self.versions[1]

        if initial_version == final_version:
            pytest.skip(f"Test requires different versions, got {initial_version} -> {final_version}")

        if min(self.versions) < (25, 4):
            pytest.skip(f"Test requires min version >= 25.4, got {min(self.versions)}")

        if initial_version > final_version:
            pytest.skip(f"Skipping downgrade test {initial_version} -> {final_version}")

        log_configs = {
            'BS_NODE': LogLevels.DEBUG,
            'BS_CONTROLLER': LogLevels.DEBUG,
        }
        yield from self.setup_cluster(
            extra_grpc_services=['config'],
            erasure=Erasure.NONE,
            nodes=1,
            additional_log_configs=log_configs,
        )

    def test_v2_to_v1_migration(self):
        logger.info(f"Test running with versions: {self.versions}")

        # step 1: check V2 is enabled on the old version (25.4)
        logger.info("Checking V2 status on old version")
        config_client = ConfigClient(self.cluster.nodes[1].host, self.cluster.nodes[1].port)

        # we expect V2 to be enabled by default on 25.4
        # FetchConfig (V2 API) should succeed
        resp = config_client.fetch_all_configs()
        logger.debug(resp)
        assert resp.operation.status == StatusIds.SUCCESS, ("V2 FetchConfig failed on old version: %s", (resp.operation.issues))

        # step 2: restart to current version
        logger.info("Restarting cluster to current version")
        self.change_cluster_version()

        # step 3: check V2 is disabled on the new version (26.1+)
        logger.info("Checking V2 status on new version")
        config_client = ConfigClient(self.cluster.nodes[1].host, self.cluster.nodes[1].port)

        # we expect V2 to be disabled because SwitchToConfigV2 is not in YAML
        # FetchConfig (V2 API) should return UNSUPPORTED
        resp = config_client.fetch_all_configs()

        logger.debug(resp)
        assert resp.operation.status == StatusIds.UNSUPPORTED, ("V2 FetchConfig should be UNSUPPORTED on current version, got %s", (resp))

        # verify V1 API works
        dynconfig_client = DynConfigClient(self.cluster.nodes[1].host, self.cluster.nodes[1].port)
        # we can check that we can use V1.
        # FetchStartupConfig is a V1 method
        resp = dynconfig_client.fetch_config()
        assert resp.operation.status == StatusIds.SUCCESS, ("V1 FetchConfig failed on current version: %s", (resp))
