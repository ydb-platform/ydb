import math
import random
import sys
import threading
import time
from conftest import BaseTestSet
from typing import List, Dict, Any
from ydb import PrimitiveType
from ydb.tests.olap.common.range_allocator import RangeAllocator
from ydb.tests.olap.common.thread_helper import TestThread, TestThreads
from ydb.tests.olap.common.time_histogram import TimeHistogram
from ydb.tests.olap.common.utils import random_string
from ydb.tests.olap.scenario.helpers import (
    ScenarioTestHelper,
    TestContext,
    CreateTable,
)
from ydb.tests.olap.lib.utils import get_external_param


class TestReadUpdateWriteLoad(BaseTestSet):
    big_table_schema = (
        ScenarioTestHelper.Schema()
        .with_column(name="key", type=PrimitiveType.Int64, not_null=True)
        .with_column(name="value", type=PrimitiveType.Utf8)
        .with_key_columns("key")
    )

    big_table_name = "big_table"

    range_allocator = RangeAllocator()

    def _loop_upsert(self, ctx: TestContext, size_kib: int):
        sth = ScenarioTestHelper(ctx)
        count_keys = math.ceil(size_kib / 100)
        hist = TimeHistogram("Write")
        for j in range(0, count_keys, 200):
            keys_range = self.range_allocator.allocate_range(min(count_keys - j, 200))
            batch: List[Dict[str, Any]] = []
            for key in range(keys_range.left, keys_range.right):
                value = random_string(100 * 1024)  # 100 KiB
                batch.append({"key": key, "value": value})
            hist.timeit(lambda: sth.bulk_upsert_data(self.big_table_name, self.big_table_schema, batch))
        print(hist, file=sys.stderr)

    def _loop_random_read(self, ctx: TestContext, finished: threading.Event):
        sth = ScenarioTestHelper(ctx)
        hist = TimeHistogram("Read")
        while not finished.is_set():
            read_to = random.randint(0, self.range_allocator.get_border)
            read_from = max(read_to - 1024, 0)
            hist.timeit(
                lambda: sth.execute_query(
                    yql=f'SELECT * FROM `{self.big_table_name}` WHERE {read_from} <= key and key <= {read_to};',
                    retries=20,
                    fail_on_error=False,
                )
            )
        print(hist, file=sys.stderr)

    def _loop_random_update(self, ctx: TestContext, finished: threading.Event):
        sth = ScenarioTestHelper(ctx)
        hist = TimeHistogram("Update")
        while not finished.is_set():
            batch: List[Dict[str, Any]] = []
            write_to = random.randint(0, self.range_allocator.get_border)
            write_from = max(write_to - 30, 0)
            for key in range(write_from, write_to):
                value = random_string(100 * 1024)  # 100 KiB
                batch.append({"key": key, "value": value})
            hist.timeit(lambda: sth.bulk_upsert_data(self.big_table_name, self.big_table_schema, batch))
        print(hist, file=sys.stderr)

    def _progress_tracker(self, finished: threading.Event):
        prev = 0
        while not finished.is_set():
            curr = self.range_allocator.get_border * 100 / 1024
            print(f"Was written: {curr} MiB, Speed: {(curr - prev) / 60} MiB/s", file=sys.stderr)
            for _ in range(60):
                if finished.is_set():
                    break
                time.sleep(1)
            prev = curr

    def scenario_read_update_write_load(self, ctx: TestContext):
        sth = ScenarioTestHelper(ctx)
        table_size_mib = int(get_external_param("table_size_mib", "64"))

        assert table_size_mib >= 64, "invalid table_size_mib parameter"

        sth.execute_scheme_query(CreateTable(self.big_table_name).with_schema(self.big_table_schema))

        progress_finished = threading.Event()
        progress_tracker_threads: TestThreads = TestThreads()
        progress_tracker_threads.append(TestThread(target=self._progress_tracker, args=[progress_finished]))
        progress_tracker_threads.start_all()

        print("Step 1. only write", file=sys.stderr)

        math.ceil(table_size_mib * 1024 / 10 / 64)

        upsert_only_threads: TestThreads = TestThreads()
        for i in range(64):
            upsert_only_threads.append(
                TestThread(target=self._loop_upsert, args=[ctx, math.ceil(table_size_mib * 1024 / 10 / 64)])
            )
        upsert_only_threads.start_all()
        upsert_only_threads.join_all()

        print("Step 2. read write", file=sys.stderr)
        upsert_threads: TestThreads = TestThreads()
        for i in range(64):
            upsert_threads.append(
                TestThread(target=self._loop_upsert, args=[ctx, math.ceil(table_size_mib * 1024 / 10 / 64)])
            )

        finished = threading.Event()
        read_threads: TestThreads = TestThreads()
        read_threads.append(TestThread(target=self._loop_random_read, args=[ctx, finished]))

        read_threads.start_all()
        upsert_threads.start_all()

        upsert_threads.join_all()
        finished.set()
        read_threads.join_all()

        print("Step 3. write modify", file=sys.stderr)

        upsert_threads: TestThreads = TestThreads()
        for i in range(64):
            upsert_threads.append(
                TestThread(target=self._loop_upsert, args=[ctx, math.ceil(table_size_mib * 1024 / 10 / 64)])
            )

        finished = threading.Event()
        update_threads: TestThreads = TestThreads()
        update_threads.append(TestThread(target=self._loop_random_update, args=[ctx, finished]))

        update_threads.start_all()
        upsert_threads.start_all()

        upsert_threads.join_all()
        finished.set()
        update_threads.join_all()

        print("Step 4. read modify write", file=sys.stderr)

        upsert_threads: TestThreads = TestThreads()
        for i in range(64):
            upsert_threads.append(
                TestThread(target=self._loop_upsert, args=[ctx, math.ceil(table_size_mib * 1024 * 7 / 10 / 64)])
            )

        finished = threading.Event()
        update_threads: TestThreads = TestThreads()
        update_threads.append(TestThread(target=self._loop_random_update, args=[ctx, finished]))

        read_threads: TestThreads = TestThreads()
        read_threads.append(TestThread(target=self._loop_random_read, args=[ctx, finished]))

        read_threads.start_all()
        update_threads.start_all()
        upsert_threads.start_all()

        upsert_threads.join_all()
        finished.set()
        update_threads.join_all()
        read_threads.join_all()

        progress_finished.set()
        progress_tracker_threads.join_all()
