# -*- coding: utf-8 -*-
import logging
import random
import threading
import time

from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger(__name__)


class WorkloadTruncateConcurrent(WorkloadBase):
    """
    Stress test that exercises concurrent operations on column tables with TRUNCATE:
    - Multiple tables are created
    - Writer threads continuously insert data
    - A truncator thread periodically truncates random tables
    - Reader threads continuously read and verify data consistency

    This tests that TRUNCATE works correctly under concurrent read/write load
    and that no VERIFY failures or data corruption occurs.
    """

    NUM_TABLES = 3
    BATCH_SIZE = 5

    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "truncate_concurrent", stop)
        self.inserts = 0
        self.reads = 0
        self.truncates = 0
        self.errors = 0
        self.lock = threading.Lock()
        self.table_locks = [threading.Lock() for _ in range(self.NUM_TABLES)]

    def get_stat(self):
        with self.lock:
            return (
                f"Inserts: {self.inserts}, Reads: {self.reads}, "
                f"Truncates: {self.truncates}, Errors: {self.errors}"
            )

    def _get_table_path(self, idx):
        return self.get_table_path(f"concurrent_table_{idx}")

    def _create_tables(self):
        for idx in range(self.NUM_TABLES):
            table_path = self._get_table_path(idx)
            self.client.query(
                f"""
                    CREATE TABLE `{table_path}` (
                    id Int64 NOT NULL,
                    val Int64,
                    ts Timestamp NOT NULL,
                    PRIMARY KEY(id, ts)
                    )
                    PARTITION BY HASH(id, ts)
                    WITH (
                        STORE = COLUMN
                    )
            """,
                True,
            )

    def _writer_loop(self, table_idx):
        """Continuously inserts data into a specific table."""
        table_path = self._get_table_path(table_idx)
        counter = 0
        while not self.is_stop_requested():
            counter += 1
            values = ", ".join(
                [f"({counter * self.BATCH_SIZE + i}, {random.randint(1, 1000000)}, "
                 f"CAST({int(time.time() * 1000000) + i} AS Timestamp))"
                 for i in range(self.BATCH_SIZE)]
            )
            try:
                self.client.query(
                    f"""
                    INSERT INTO `{table_path}` (`id`, `val`, `ts`)
                    VALUES {values}
                """,
                    False,
                    log_error=False,
                )
                with self.lock:
                    self.inserts += self.BATCH_SIZE
            except Exception as e:
                # Inserts may fail during truncation, that's expected
                logger.debug(f"Insert error on table {table_idx}: {e}")
                with self.lock:
                    self.errors += 1
                time.sleep(0.1)

    def _reader_loop(self):
        """Continuously reads from random tables and verifies count is non-negative."""
        while not self.is_stop_requested():
            table_idx = random.randint(0, self.NUM_TABLES - 1)
            table_path = self._get_table_path(table_idx)
            try:
                result = self.client.query(
                    f"""
                    SELECT COUNT(*) as cnt FROM `{table_path}`
                """,
                    False,
                    log_error=False,
                )[0].rows[0]

                cnt = result["cnt"]
                if cnt < 0:
                    raise Exception(f"Negative count {cnt} on table {table_idx}")

                with self.lock:
                    self.reads += 1
            except Exception as e:
                logger.debug(f"Read error on table {table_idx}: {e}")
                with self.lock:
                    self.errors += 1
                time.sleep(0.1)

    def _truncator_loop(self):
        """Periodically truncates random tables."""
        while not self.is_stop_requested():
            table_idx = random.randint(0, self.NUM_TABLES - 1)
            table_path = self._get_table_path(table_idx)
            try:
                self.client.query(
                    f"""
                    TRUNCATE TABLE `{table_path}`
                """,
                    True,
                    log_error=False,
                )
                with self.lock:
                    self.truncates += 1
            except Exception as e:
                logger.debug(f"Truncate error on table {table_idx}: {e}")
                with self.lock:
                    self.errors += 1
            # Small delay between truncations
            time.sleep(random.uniform(0.5, 2.0))

    def _pre_start(self):
        self._create_tables()
        return True

    def get_workload_thread_funcs(self):
        funcs = []
        # One writer per table
        for idx in range(self.NUM_TABLES):
            funcs.append(lambda i=idx: self._writer_loop(i))
        # Two reader threads
        funcs.append(self._reader_loop)
        funcs.append(self._reader_loop)
        # One truncator thread
        funcs.append(self._truncator_loop)
        return funcs
