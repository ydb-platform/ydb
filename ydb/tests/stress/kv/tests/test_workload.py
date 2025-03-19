# -*- coding: utf-8 -*-
import os

import pytest

import yatest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure


class TestYdbKvWorkload(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=Erasure.MIRROR_3_DC))
        cls.cluster.start()
        cls.init_command_prefix = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "workload", "kv", "init",
            "--min-partitions", "1",
            "--partition-size", "10",
            "--auto-partition", "0",
            "--init-upserts", "0",
            "--cols", "5",
            "--int-cols", "2",
            "--key-cols", "3",
        ]

        cls.run_command_prefix = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "--verbose",
            "--endpoint", "grpc://localhost:%d" % cls.cluster.nodes[1].grpc_port,
            "--database=/Root",
            "workload", "kv", "run", "mixed",
            "--seconds", "100",
            "--threads", "10",
            "--cols", "5",
            "--len", "200",
            "--int-cols", "2",
            "--key-cols", "3"
        ]

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test(self, store_type):
        init_command = self.init_command_prefix
        init_command.extend([
            "--path", store_type,
            "--store", store_type,
        ])
        run_command = self.run_command_prefix
        run_command.extend([
            "--path", store_type,
        ])
        yatest.common.execute(init_command, wait=True)
        yatest.common.execute(run_command, wait=True)
