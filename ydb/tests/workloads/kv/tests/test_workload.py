# -*- coding: utf-8 -*-
import os

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
        yatest.common.execute(
            [
                yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
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
                "--key-cols", "3",
                "--store", "column",
            ],
            wait=True
        )

        yatest.common.execute(
            [
                yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
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
