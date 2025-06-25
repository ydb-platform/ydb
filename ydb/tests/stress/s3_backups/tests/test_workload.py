# -*- coding: utf-8 -*-
import os
import yatest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


class TestYdbWorkload(object):
    @classmethod
    def setup_class(cls):
        config = KikimrConfigGenerator(
            extra_feature_flags={
                "enable_export_auto_dropping": True,
                "enable_changefeeds_export": True,
            }
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    def test(self):
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", f"grpc://localhost:{self.cluster.nodes[1].grpc_port}",
            "--database", "/Root",
            "--duration", "120",
        ]
        yatest.common.execute(cmd, wait=True)
