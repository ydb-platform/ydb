# -*- coding: utf-8 -*-
import os
import yatest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
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
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", f"grpc://localhost:{self.cluster.nodes[1].grpc_port}",
            "--mon-endpoint", f"http://localhost:{self.cluster.nodes[1].mon_port}",
            "--database", "/Root",
            "--duration", "120",
        ]
        yatest.common.execute(cmd, wait=True)
