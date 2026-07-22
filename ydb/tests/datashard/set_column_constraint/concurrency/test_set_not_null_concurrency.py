# -*- coding: utf-8 -*-
import concurrent.futures
import random
import threading
import time

import pytest
import ydb

from ydb.tests.sql.lib.test_base import TestBase


_RETRIABLE_NAMES = [
    "Aborted",
    "Overloaded",
    "Unavailable",
    "BadSession",
    "SessionBusy",
    "SchemeError",
    "Undetermined",
    "Timeout",
]

RETRIABLE_ERRORS = tuple(
    getattr(ydb.issues, name) for name in _RETRIABLE_NAMES if hasattr(ydb.issues, name)
)


def _is_set_not_null_in_progress(err) -> bool:
    return "is currently in progress" in str(err)


class TestSetNotNullConcurrency(TestBase):
    @classmethod
    def get_extra_feature_flags(cls):
        return super().get_extra_feature_flags() + ["enable_set_column_constraint"]

    def _create_table(self, table_name: str):
        self.query(f"""
            CREATE TABLE `{table_name}` (
                id Uint64 NOT NULL,
                value Int64,
                PRIMARY KEY (id)
            )
        """)

    def _count_nulls(self, table_name: str) -> int:
        result = self.query(f"SELECT COUNT(*) AS cnt FROM `{table_name}` WHERE value IS NULL")
        return result[0]["cnt"]

    def _row_count(self, table_name: str) -> int:
        result = self.query(f"SELECT COUNT(*) AS cnt FROM `{table_name}`")
        return result[0]["cnt"]

    def _upsert_range(self, table_name: str, begin: int, end: int):
        values = ", ".join(f"({i}, {i * 10 + 1})" for i in range(begin, end))
        self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES {values}")

    def test_set_not_null_with_concurrent_upserts(self):
        table_name = f"{self.table_path}_concurrency"
        self._create_table(table_name)

        seed_count = 200
        self._upsert_range(table_name, 0, seed_count)
        assert self._count_nulls(table_name) == 0

        duration = 20
        deadline = time.time() + duration
        stop = threading.Event()
        errors = []

        next_key = seed_count
        next_key_lock = threading.Lock()

        def upsert_worker():
            nonlocal next_key
            while not stop.is_set() and time.time() < deadline:
                try:
                    with next_key_lock:
                        key = next_key
                        next_key += 1
                    self.query(
                        f"UPSERT INTO `{table_name}` (id, value) VALUES ({key}, {random.randint(1, 10**9)})"
                    )
                    existing = random.randint(0, key)
                    self.query(
                        f"UPSERT INTO `{table_name}` (id, value) VALUES ({existing}, {random.randint(1, 10**9)})"
                    )
                except RETRIABLE_ERRORS:
                    continue
                except ydb.issues.Error as e:
                    if _is_set_not_null_in_progress(e):
                        continue
                    errors.append(("upsert", str(e)))
                    return

        alter_cycles = 0

        def alter_worker():
            nonlocal alter_cycles
            while not stop.is_set() and time.time() < deadline:
                try:
                    self.query(f"ALTER TABLE `{table_name}` ALTER COLUMN value SET NOT NULL")
                    self.query(f"ALTER TABLE `{table_name}` ALTER COLUMN value DROP NOT NULL")
                    alter_cycles += 1
                except ydb.issues.PreconditionFailed as e:
                    errors.append(("set_not_null_precondition", str(e)))
                    return
                except RETRIABLE_ERRORS:
                    continue
                except ydb.issues.Error as e:
                    errors.append(("alter", str(e)))
                    return
                time.sleep(0.6)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(upsert_worker) for _ in range(4)]
            futures.append(executor.submit(alter_worker))

            concurrent.futures.wait(futures, timeout=duration + 60)
            stop.set()
            for future in futures:
                future.result(timeout=10)

        assert not errors, f"Unexpected errors during concurrent execution: {errors}"
        assert alter_cycles > 0, "SET NOT NULL / DROP NOT NULL never completed a cycle"

        final_count = self._row_count(table_name)
        assert final_count > seed_count, f"expected more than {seed_count} rows, got {final_count}"

        assert self._count_nulls(table_name) == 0

        self.query(f"ALTER TABLE `{table_name}` ALTER COLUMN value SET NOT NULL")
        assert self._count_nulls(table_name) == 0
        with pytest.raises(ydb.issues.Error):
            self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES (999999999, NULL)")

    def test_set_not_null_rejects_concurrent_null_upserts(self):
        table_name = f"{self.table_path}_null_race"
        self._create_table(table_name)
        self._upsert_range(table_name, 0, 100)

        stop = threading.Event()
        errors = []

        def null_upsert_worker():
            key = 1000
            while not stop.is_set():
                try:
                    self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES ({key}, NULL)")
                    key += 1
                except RETRIABLE_ERRORS:
                    continue
                except ydb.issues.Error:
                    continue

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            writer = executor.submit(null_upsert_worker)
            try:
                for _ in range(15):
                    try:
                        self.query(f"ALTER TABLE `{table_name}` ALTER COLUMN value SET NOT NULL")
                        assert self._count_nulls(table_name) == 0
                        with pytest.raises(ydb.issues.Error):
                            self.query(f"UPSERT INTO `{table_name}` (id, value) VALUES (2000, NULL)")
                        self.query(f"ALTER TABLE `{table_name}` ALTER COLUMN value DROP NOT NULL")
                    except ydb.issues.PreconditionFailed:
                        pass
                    except RETRIABLE_ERRORS:
                        continue
                    time.sleep(0.6)
            finally:
                stop.set()
                writer.result(timeout=10)

        assert not errors, f"Unexpected errors: {errors}"
