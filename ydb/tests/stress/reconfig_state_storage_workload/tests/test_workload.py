# -*- coding: utf-8 -*-
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.stress.common.common import YdbClient
from ydb.tests.stress.reconfig_state_storage_workload.workload import WorkloadRunner
from ydb.tests.library.harness.util import LogLevels


class TestReconfigStateStorageWorkload(object):

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
            column_shard_config={
                "allow_nullable_columns_in_pk": True,
            },
            additional_log_configs={
                'BS_NODE': LogLevels.DEBUG,
                'BOARD_LOOKUP': LogLevels.DEBUG,
                # 'STATESTORAGE': LogLevels.DEBUG,
            }
        ))
        cls.cluster.start()
        cls.client = YdbClient(f'grpc://localhost:{cls.cluster.nodes[1].grpc_port}', '/Root', True)
        cls.client.wait_connection()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test_state_storage(self):
        with WorkloadRunner(self.client, self.cluster, 'reconfig_state_storage_workload', 120, True, "StateStorage") as runner:
            runner.run()

    def test_state_storage_board(self):
        with WorkloadRunner(self.client, self.cluster, 'reconfig_state_storage_workload', 120, True, "StateStorageBoard") as runner:
            runner.run()

    # def test_scheme_board(self):
    #     with WorkloadRunner(self.client, self.cluster, 'reconfig_state_storage_workload', 120, True, "SchemeBoard") as runner:
    #         runner.run()
