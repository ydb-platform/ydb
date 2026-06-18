# -*- coding: utf-8 -*-
import threading
import time

from ydb.tests.stress.common.common import WorkloadBase


class WorkloadTruncateInsert(WorkloadBase):
    """
    Stress test that repeatedly inserts data into a column table, verifies the count,
    truncates the table, and verifies the table is empty. This exercises the full
    TRUNCATE TABLE lifecycle for column tables.
    """

    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "truncate_insert", stop)
        self.iterations = 0
        self.inserts = 0
        self.truncates = 0
        self.table_name = "truncate_insert_table"
        self.lock = threading.Lock()

    def get_stat(self):
        with self.lock:
            return f"Iterations: {self.iterations}, Inserts: {self.inserts}, Truncates: {self.truncates}"

    def _loop(self):
        table_path = self.get_table_path(self.table_name)
        self.client.query(
            f"""
                CREATE TABLE `{table_path}` (
                id Int64 NOT NULL,
                val Int64,
                str_val Utf8,
                PRIMARY KEY(id)
                )
                PARTITION BY HASH(id)
                WITH (
                    STORE = COLUMN
                )
        """,
            True,
        )

        iteration = 0
        while not self.is_stop_requested():
            iteration += 1
            batch_size = 10

            # Insert a batch of rows
            values = ", ".join(
                [f"({iteration * batch_size + i}, {i * 100 + iteration}, CAST('row_{iteration}_{i}' AS Utf8))"
                 for i in range(batch_size)]
            )
            self.client.query(
                f"""
                INSERT INTO `{table_path}` (`id`, `val`, `str_val`)
                VALUES {values}
            """,
                False,
            )

            with self.lock:
                self.inserts += batch_size

            # Verify data is present
            result = self.client.query(
                f"""
                SELECT COUNT(*) as cnt FROM `{table_path}`
            """,
                False,
            )[0].rows[0]

            if result["cnt"] == 0:
                raise Exception(f"Expected non-zero count after insert, got 0 at iteration {iteration}")

            # Truncate the table
            self.client.query(
                f"""
                TRUNCATE TABLE `{table_path}`
            """,
                True,
            )

            with self.lock:
                self.truncates += 1

            # Verify table is empty after truncation
            result = self.client.query(
                f"""
                SELECT COUNT(*) as cnt FROM `{table_path}`
            """,
                False,
            )[0].rows[0]

            if result["cnt"] != 0:
                raise Exception(
                    f"Expected 0 rows after TRUNCATE, got {result['cnt']} at iteration {iteration}"
                )

            with self.lock:
                self.iterations += 1

    def get_workload_thread_funcs(self):
        return [self._loop]
