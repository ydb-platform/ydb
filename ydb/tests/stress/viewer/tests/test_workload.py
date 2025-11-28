# -*- coding: utf-8 -*-
import pytest
import os
import yatest
from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test(self):
        if not self.mon_endpoint:
            self.mon_endpoint = "http://localhost:8765"
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--mon_endpoint", self.mon_endpoint,
            "--database", self.database,
            "--duration", "60",
        ]
        yatest.common.execute(cmd, wait=True)
