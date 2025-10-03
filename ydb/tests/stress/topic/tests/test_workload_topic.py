# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbTopicWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test(self):
        cmd_args = [
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", "60",
            "--consumers", "50",
            "--producers", "100",
        ]
        if os.environ.get("YDB_STRESS_TEST_LIMIT_MEMORY", "0").lower() in ['true', '1', 'y', 'yes']:
            cmd_args.append('--limit-memory-usage')
        yatest.common.execute(cmd_args)
