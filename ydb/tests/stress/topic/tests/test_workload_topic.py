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
        limit_memory_usage = os.environ.get("YDB_STRESS_TEST_LIMIT_MEMORY", "0").lower() in ['true', '1', 'y', 'yes']
        consumers = 50
        producers = 100
        if limit_memory_usage:
            consumers //= 3
            producers //= 3
        cmd_args = [
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", self.base_duration,
            "--consumers", str(consumers),
            "--producers", str(producers),
        ]
        if limit_memory_usage:
            cmd_args.append('--limit-memory-usage')
        yatest.common.execute(cmd_args)
