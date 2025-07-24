# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_show_create": True,
            }
        )

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
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", str(duration),
        ]

        if path_prefix is not None:
            cmd.extend(["--path-prefix", path_prefix])

        yatest.common.execute(cmd, wait=True)
