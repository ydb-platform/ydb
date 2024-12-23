# -*- coding: utf-8 -*-
import yatest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure


class TestYdbKvWorkload(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=Erasure.MIRROR_3_DC))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test(self):
        workload_path = yatest.common.build_path("ydb/tests/workloads/simple_queue/simple_queue")
        yatest.common.execute(
            [
                workload_path,
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database=/Root",
                "--duration", "60",
                "--mode", "column",
            ],
            wait=True
        )
