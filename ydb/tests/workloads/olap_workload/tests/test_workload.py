# -*- coding: utf-8 -*-
import yatest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure


class TestYdbWorkload(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            column_shard_config={
                "allow_nullable_columns_in_pk": True,
            }
        ))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test(self):
        workload_path = yatest.common.build_path("ydb/tests/workloads/olap_workload/olap_workload")
        yatest.common.execute(
            [
                workload_path,
                "--endpoint", f"grpc://localhost:{self.cluster.nodes[1].grpc_port}",
                "--database=/Root",
                "--duration", "120",
                "--allow-nullables-in-pk", "1",
            ],
            wait=True
        )
