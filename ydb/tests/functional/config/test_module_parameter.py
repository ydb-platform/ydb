# -*- coding: utf-8 -*-
import logging
import time
from hamcrest import assert_that

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.clients.kikimr_http_client import SwaggerClient

logger = logging.getLogger(__name__)


class TestModuleParameter(object):

    @classmethod
    def setup_class(cls):
        """
        Setup a simple cluster with module parameter.
        """
        log_configs = {
            'GRPC_SERVER': LogLevels.DEBUG,
            'TX_PROXY': LogLevels.DEBUG,
        }
        
        # Create configurator with module parameter
        cls.configurator = KikimrConfigGenerator(
            erasure=Erasure.NONE,
            nodes=1,
            use_in_memory_pdisks=True,
            additional_log_configs=log_configs,
            module="TEST-MODULE-01"
        )

        cls.cluster = KiKiMR(configurator=cls.configurator)
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test_cluster_starts_with_module_parameter(self):
        """
        Test that cluster starts successfully with module parameter.
        """
        # Check that cluster is running
        assert_that(len(self.cluster.nodes) > 0)
        
        # Check that first node is alive
        node = self.cluster.nodes[1]
        assert_that(node.is_alive())
        
        logger.info("Cluster started successfully with module parameter")

    def test_cluster_is_operational(self):
        """
        Test that cluster is operational after starting with module parameter.
        """
        # Get swagger client for monitoring
        node = self.cluster.nodes[1]
        swagger_client = SwaggerClient(node.host, node.mon_port)
        
        # Wait a bit for cluster to stabilize
        time.sleep(2)
        
        # Check nodes info
        try:
            nodes_info = swagger_client.nodes_info()
            assert_that(nodes_info is not None)
            logger.info("Cluster is operational, nodes info retrieved successfully")
        except Exception as e:
            logger.error(f"Failed to get nodes info: {e}")
            raise

    def test_module_parameter_in_command(self):
        """
        Test that --module parameter is present in the command line.
        """
        node = self.cluster.nodes[1]
        command = node.command
        
        # Check that --module parameter is in the command
        has_module_param = any("--module=" in arg for arg in command)
        assert_that(has_module_param)
        
        logger.info(f"Command contains --module parameter: {command}")

