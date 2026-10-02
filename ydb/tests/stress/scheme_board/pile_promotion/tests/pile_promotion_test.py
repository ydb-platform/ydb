# -*- coding: utf-8 -*-
import os
import yatest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels


class PilePromotionTest(object):

    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(
            KikimrConfigGenerator(
                erasure=Erasure.BLOCK_4_2,
                nodes=16,
                use_in_memory_pdisks=False,
                use_config_store=True,
                metadata_section={
                    "kind": "MainConfig",
                    "version": 0,
                    "cluster": "",
                },
                separate_node_configs=True,
                simple_config=True,
                use_self_management=True,
                extra_grpc_services=["bridge"],
                bridge_config={"piles": [{"name": "r1"}, {"name": "r2"}]},
                additional_log_configs={
                    "BS_NODE": LogLevels.DEBUG,
                    "CMS_TENANTS": LogLevels.DEBUG,
                    "FLAT_TX_SCHEMESHARD": LogLevels.DEBUG,
                    "INTERCONNECT": LogLevels.INFO,
                    "KQP_COMPILE_ACTOR": LogLevels.DEBUG,
                    "KQP_GATEWAY": LogLevels.DEBUG,
                    "KQP_SESSION": LogLevels.DEBUG,
                    "SCHEME_BOARD_POPULATOR": LogLevels.DEBUG,
                    "SCHEME_BOARD_REPLICA": LogLevels.DEBUG,
                    "SCHEME_BOARD_SUBSCRIBER": LogLevels.TRACE,
                    "TX_PROXY": LogLevels.DEBUG,
                    "TX_PROXY_SCHEME_CACHE": LogLevels.DEBUG,
                },
            )
        )
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def do_test(self):
        cmd = [
            yatest.common.binary_path(os.getenv("STRESS_TEST_UTILITY")),
            "--grpc_endpoint", f"grpc://{self.cluster.nodes[1].host}:{self.cluster.nodes[1].grpc_port}",
            "--http_endpoint", f"http://{self.cluster.nodes[1].host}:{self.cluster.nodes[1].mon_port}",
            "--database", "/Root",
            "--path", "test",
            "--duration", "180",
        ]

        yatest.common.execute(cmd, wait=True)
