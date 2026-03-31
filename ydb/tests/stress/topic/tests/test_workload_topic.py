# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture
from ydb.tests.stress.topic.workload import YdbTopicWorkload


def create_test_methods(chunk_size):
    def decorator(cls):
        workload = YdbTopicWorkload(
            endpoint=None,
            database=None,
            duration=1,
            consumers=1,
            producers=1,
            tables_prefix="table_prefix",
            limit_memory_usage=False,
            chunk_index=None,
            chunk_size=None
        )

        count = len(workload.get_workload_thread_funcs())
        count += (chunk_size - 1)
        count //= chunk_size

        for k in range(count):
            def make_test_method(index):
                def test_method(self):
                    self._run_chunk(k, chunk_size)
                test_method.__name__ = f"test_{index}"
                return test_method
            setattr(cls, f"test_{k}", make_test_method(k))

        return cls

    return decorator


@create_test_methods(chunk_size=3)
class TestYdbTopicWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def _run_chunk(self, chunk_index, chunk_size):
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
            "--chunk-index", str(chunk_index),
            "--chunk-size", str(chunk_size),
        ]
        if limit_memory_usage:
            cmd_args.append('--limit-memory-usage')
        yatest.common.execute(cmd_args)
