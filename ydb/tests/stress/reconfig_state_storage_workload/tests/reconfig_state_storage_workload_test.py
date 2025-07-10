# -*- coding: utf-8 -*-
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.stress.common.common import YdbClient
from ydb.tests.library.harness.util import LogLevels


class ReconfigStateStorageWorkloadTest(object):

    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(
            KikimrConfigGenerator(
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
                },
            )
        )
        cls.cluster.start()
        cls.client = YdbClient(f'grpc://localhost:{cls.cluster.nodes[1].grpc_port}', '/Root', True)
        cls.client.wait_connection()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()
