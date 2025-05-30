# -*- coding: utf-8 -*-
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.stress.common.common import YdbClient
from ydb.tests.stress.node_broker.workload import WorkloadRunner
from ydb.tests.library.harness.util import LogLevels


class TestYdbWorkload(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(
            KikimrConfigGenerator(
                erasure=Erasure.MIRROR_3_DC,
                additional_log_configs={
                    "NODE_BROKER": LogLevels.TRACE,
                    "NAMESERVICE": LogLevels.TRACE,
                },
            )
        )
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test(self):
        client = YdbClient(f"grpc://localhost:{self.cluster.nodes[1].grpc_port}", "/Root", True)
        client.wait_connection()
        with WorkloadRunner(client, f"http://localhost:{self.cluster.nodes[1].mon_port}", 120) as runner:
            runner.run()
