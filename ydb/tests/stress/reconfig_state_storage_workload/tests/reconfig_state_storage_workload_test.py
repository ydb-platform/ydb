# -*- coding: utf-8 -*-
import os
import yatest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels


class ReconfigStateStorageWorkloadTest(object):

    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(
            erasure=Erasure.BLOCK_4_2,
            use_self_management=True,
            simple_config=True,
            metadata_section={
                "kind": "MainConfig",
                "version": 0,
                "cluster": "",
            },
            additional_log_configs={
                'BS_NODE': LogLevels.DEBUG,
                'BOARD_LOOKUP': LogLevels.DEBUG,
                'DISCOVERY': LogLevels.DEBUG,
                'INTERCONNECT': LogLevels.INFO,
                'FLAT_TX_SCHEMESHARD': LogLevels.DEBUG,
                'SCHEME_BOARD_POPULATOR': LogLevels.DEBUG,
                'SCHEME_BOARD_REPLICA': LogLevels.DEBUG,
                'SCHEME_BOARD_SUBSCRIBER': LogLevels.DEBUG,
                'TX_PROXY_SCHEME_CACHE': LogLevels.DEBUG,
                'KQP_COMPILE_ACTOR': LogLevels.DEBUG,
                'CMS_TENANTS': LogLevels.DEBUG,
                # 'STATESTORAGE': LogLevels.DEBUG,
            }
        ))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def do_test(self, config_name):
        cmd = [
            yatest.common.binary_path(os.getenv("STRESS_TEST_UTILITY")),
            "--grpc_endpoint", f"grpc://{self.cluster.nodes[1].host}:{self.cluster.nodes[1].grpc_port}",
            "--http_endpoint", f"http://{self.cluster.nodes[1].host}:{self.cluster.nodes[1].mon_port}",
            "--database", "/Root",
            "--path", "test",
            "--duration", "120",
            "--config_name", config_name
        ]

        yatest.common.execute(cmd, wait=True)
