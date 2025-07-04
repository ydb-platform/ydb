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
                "enable_export_auto_dropping": True,
                "enable_changefeeds_export": True,
            }
        )

    def test(self):
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", f"grpc://localhost:{self.cluster.nodes[1].grpc_port}",
            "--database", "/Root",
            "--duration", "120",
        ]
        yatest.common.execute(cmd, wait=True)
