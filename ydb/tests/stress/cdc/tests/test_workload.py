# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            additional_log_configs={
                "CHANGE_EXCHANGE": LogLevels.DEBUG,
            },
        )

    def test(self):
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", "120",
        ]
        yatest.common.execute(cmd, wait=True)
