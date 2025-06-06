# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


class TestShowCreateViewWorkload(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = KiKiMR(
            KikimrConfigGenerator(
                extra_feature_flags={
                    "enable_show_create": True,
                }
            )
        )
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        cls.cluster.stop()

    @pytest.mark.parametrize(
        "duration, path_prefix",
        [
            (30, None),
            (30, "test_scv"),
        ],
    )
    def test_show_create_view_workload(self, duration, path_prefix):
        cmd = [
            yatest.common.binary_path(os.getenv("STRESS_TEST_UTILITY")),
            "--endpoint", f"grpc://localhost:{self.cluster.nodes[1].grpc_port}",
            "--database", "/Root",
            "--duration", str(duration),
        ]

        if path_prefix is not None:
            cmd.extend(["--path-prefix", path_prefix])

        yatest.common.execute(cmd, wait=True)
