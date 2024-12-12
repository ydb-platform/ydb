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
        cls.column_cluster = KiKiMR(KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            column_shard_config={
                'allow_nullable_columns_in_pk': True,
            },
        ))
        cls.column_cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()
        cls.column_cluster.stop()

    def RunTest(self, useColumns):
        cluster = self.column_cluster if useColumns else self.cluster
        args = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % cluster.nodes[1].grpc_port,
            "--database=/Root",

            "workload", "kv", "init",

            "--min-partitions", "1",
            "--partition-size", "10",
            "--auto-partition", "0",
            "--init-upserts", "0",
            "--cols", "5",
            "--int-cols", "2",
            "--key-cols", "3"
        ]

        if useColumns:
            args += ["--column-tables", "1"]

        yatest.common.execute(
            args,
            wait=True
        )

        argsx = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % cluster.nodes[1].grpc_port,
            "--database=/Root",

            "workload", "kv", "run", "mixed",
            "--seconds", "100",
            "--threads", "10",

            "--cols", "5",
            "--len", "200",
            "--int-cols", "2",
            "--key-cols", "3"
        ]

        if useColumns:
            argsx += [
                "--executer", "generic",
                "--do-select", "1",
                "--do-read-rows", "0"
            ]

        yatest.common.execute(
            argsx,
            wait=True
        )

    def test(self):
        self.RunTest(False)

    def test_columns(self):
        self.RunTest(True)
