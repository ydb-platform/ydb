# -*- coding: utf-8 -*-
import logging
import time
import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient

logger = logging.getLogger(__name__)


class TestModuleParameter:

    @pytest.fixture(autouse=True)
    def setup(self):
        log_configs = {
            'GRPC_SERVER': LogLevels.DEBUG,
            'TX_PROXY': LogLevels.DEBUG,
        }
        # Create configurator with module parameter
        self.configurator = KikimrConfigGenerator(
            erasure=Erasure.NONE,
            nodes=1,
            use_in_memory_pdisks=True,
            additional_log_configs=log_configs,
            module="TEST-MODULE-01"
        )

        self.cluster = KiKiMR(configurator=self.configurator)
        self.cluster.start()

        yield

        self.cluster.stop()

    def test_cluster_starts_and_is_operational_with_module_parameter(self):
        # Test that cluster starts successfully with module parameter and is operational.
        # Check that cluster is running
        assert len(self.cluster.nodes) > 0
        # Check that first node is alive
        node = self.cluster.nodes[1]
        assert node.is_alive()
        logger.info("Cluster started successfully with module parameter")
        # Get swagger client for monitoring
        swagger_client = SwaggerClient(node.host, node.mon_port)
        # Wait a bit for cluster to stabilize
        time.sleep(2)
        # Check nodes info to verify cluster is operational
        nodes_info = swagger_client.nodes_info()
        assert nodes_info is not None
        logger.info("Cluster is operational, nodes info retrieved successfully")

    def test_module_parameter_in_command(self):
        # Test that --module parameter is present in the command line.
        node = self.cluster.nodes[1]
        command = node.command
        # Check that --module parameter is in the command
        has_module_param = any("--module=" in arg for arg in command)
        assert has_module_param
        logger.info(f"Command contains --module parameter: {command}")
