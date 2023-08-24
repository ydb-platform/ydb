# -*- coding: utf-8 -*-
import os

from ydb.tests.library.common import yatest_common
from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure


class TestYdbKvWorkload(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(KikimrConfigGenerator(erasure=Erasure.MIRROR_3_DC))
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test(self):
        yatest_common.execute(
            [
                yatest_common.binary_path(os.getenv("YDB_CLI_BINARY")),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database=/Root",

                "workload", "kv", "init",

                "--min-partitions", "1",
                "--partition-size", "10",
                "--auto-partition", "0",
                "--init-upserts", "0",
                "--cols", "5",
                "--int-cols", "2",
                "--key-cols", "3"
            ],
            wait=True
        )

        yatest_common.execute(
            [
                yatest_common.binary_path(os.getenv("YDB_CLI_BINARY")),
                "--verbose",
                "--endpoint", "grpc://localhost:%d" % self.cluster.nodes[1].grpc_port,
                "--database=/Root",

                "workload", "kv", "run", "mixed",
                "--seconds", "100",
                "--threads", "10",

                "--cols", "5",
                "--len", "200",
                "--int-cols", "2",
                "--key-cols", "3"
            ],
            wait=True
        )
