# -*- coding: utf-8 -*-
import yatest
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.param_constants import kikimr_driver_path
from ydb.tests.library.common.types import Erasure
from ydb.tests.oss.ydb_sdk_import import ydb

# -*- coding: utf-8 -*-
import os

import pytest

import yatest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure


class TestCompatibility(object):
    @classmethod
    def setup_class(cls):
        last_stable_path = yatest.common.binary_path("ydb/tests/library/compatibility/ydbd-last-stable")
        binary_paths = [kikimr_driver_path(), last_stable_path]
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=Erasure.MIRROR_3_DC, binary_paths=binary_paths))
        cls.cluster.start()
        cls.endpoint = "%s:%s" % (
            cls.cluster.nodes[1].host, cls.cluster.nodes[1].port
        )
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=cls.endpoint
            )
        )
        cls.driver.wait()
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
            "--seconds", "10",
            "--threads", "20",
            "--cols", "5",
            "--len", "200",
            "--int-cols", "2",
            "--key-cols", "3"
        ]

    @classmethod
    def teardown_class(cls):
        cls.driver.stop()
        cls.cluster.stop(kill=True)  # TODO fix

    @pytest.mark.parametrize("store_type", ["column"])
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
