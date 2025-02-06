# -*- coding: utf-8 -*-
import yatest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


class TestYdbWorkload(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator())
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test(self):
        workload_path = yatest.common.build_path("ydb/tests/stress/oltp_workload/oltp_workload")
        yatest.common.execute(
            [
                workload_path,
                "--endpoint", f"grpc://localhost:{self.cluster.nodes[1].grpc_port}",
                "--database=/Root",
                "--duration", "120",
            ],
            wait=True
        )
