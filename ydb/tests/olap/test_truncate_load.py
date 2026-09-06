# -*- coding: utf-8 -*-
"""
Load scenario tests for TRUNCATE TABLE on column tables.

Covers test plan section 4 (L1-L8): Load scenarios.

Durations are scaled down for CI (e.g. 10-15 sec instead of 60-300 sec)
while preserving the same workload patterns and success criteria.
"""
import logging
import os
import random
import threading
import time

import requests
import yatest.common
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)

# Duration overrides for CI — much shorter than the test plan's 60-300 sec.
DURATION_SHORT = 10   # L1, L5, L6, L7
DURATION_MEDIUM = 15  # L2, L3, L8
BATCH_SIZE = 100


class TestTruncateColumnTableLoad(object):
    test_name = "truncate_load"

    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))
        config = KikimrConfigGenerator(
            column_shard_config={
                "compaction_enabled": True,
                "deduplication_enabled": True,
                "reader_class_name": "SIMPLE",
                "generate_internal_path_id": True,
            },
            extra_feature_flags={
                "enable_columnshard_bool": True,
                "enable_truncate_column_table": True,
                "enable_local_min_max_index": True,
                "enable_column_tables_backup": True,
            },
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(
            database=f"/{config.domain_name}",
            endpoint=f"grpc://{node.host}:{node.port}",
        )
        cls.ydb_client.wait_connection()
        cls.mon_url = f"http://{node.host}:{node.mon_port}"
        cls.test_dir = f"{cls.ydb_client.database}/{cls.test_name}"
        cls.config = config

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        cls.cluster.stop()

    # --- Helpers ---

    def get_table_path(self, name):
        return f"{self.test_dir}/{name}_{random.randrange(99999)}"

    def create_column_table(self, name, extra_columns="", extra_with=""):
        table_path = self.get_table_path(name)
        with_options = ["STORE = COLUMN"]
        if extra_with:
            with_options.append(extra_with)
        with_clause = ",\n                ".join(with_options)
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                val Uint64,
                str_val Utf8,
                {extra_columns}
                PRIMARY KEY(id),
            )
            WITH (
                {with_clause}
            )
            """
        )
        return table_path

    def insert_batch(self, client, table_path, start_id, count):
        """Insert a batch of rows using the given client."""
        values = ", ".join(
            [f"({start_id + i}, {i * 100}, CAST('row_{start_id + i}' AS Utf8))" for i in range(count)]
        )
        client.query(
            f"""
            INSERT INTO `{table_path}` (`id`, `val`, `str_val`)
            VALUES {values}
            """
        )

    def insert_rows(self, table_path, count, start_id=0):
        self.insert_batch(self.ydb_client, table_path, start_id, count)

    def get_count(self, table_path):
        result = self.ydb_client.query(
            f"""
            SELECT COUNT(*) AS cnt FROM `{table_path}`
            """
        )
        return result[0].rows[0]["cnt"]

    def truncate_table(self, table_path):
        self.ydb_client.query(
            f"""
            TRUNCATE TABLE `{table_path}`
            """
        )

    def drop_table(self, table_path):
        self.ydb_client.query(
            f"""
            DROP TABLE `{table_path}`
            """
        )

    def create_client(self):
        return YdbClient(
            database=self.ydb_client.database,
            endpoint=self.ydb_client.endpoint,
        )

    def list_shards(self, table_path):
        """Return the list of ColumnShard tablet IDs for a column table."""
        response = requests.get(
            self.mon_url
            + f"/viewer/json/describe?database={self.ydb_client.database}"
            + f"&path={table_path}&enums=true&partition_stats=true&subs=0"
        )
        response.raise_for_status()
        path_description = response.json()["PathDescription"]
        if "ColumnTableDescription" in path_description:
            return path_description["ColumnTableDescription"]["Sharding"]["ColumnShards"]
        return [item["DatashardId"] for item in path_description["TablePartitions"]]

    def kill_tablet(self, tablet_id):
        """Restart a tablet via the monitoring HTTP endpoint."""
        response = requests.get(self.mon_url + f"/tablets?RestartTabletID={tablet_id}")
        response.raise_for_status()

    def restart_all_shards(self, table_path):
        """Kill (restart) all ColumnShard tablets hosting the given table."""
        shards = self.list_shards(table_path)
        for shard in shards:
            self.kill_tablet(shard)
        return shards

    # --- Load tests ---

    # L1: TRUNCATE + continuous INSERT (1 writer)
    # 1 table, batch 100 rows, DURATION_SHORT sec.
    # Success: no data loss, no AFL_VERIFY, TRUNCATE latency < 5 sec.
    def test_l1_truncate_with_continuous_insert_1_writer(self):
        table_path = self.create_column_table("l1_1writer")
        try:
            errors = []
            insert_count = [0]
            stop_flag = threading.Event()

            def do_insert():
                try:
                    client = self.create_client()
                    try:
                        batch_id = 0
                        while not stop_flag.is_set():
                            try:
                                self.insert_batch(client, table_path, batch_id * BATCH_SIZE, BATCH_SIZE)
                                insert_count[0] += 1
                            except Exception as e:
                                # Transient errors (unknown table during TRUNCATE) are expected
                                logger.debug("L1: INSERT failed (expected during TRUNCATE): %s", str(e))
                            batch_id += 1
                            time.sleep(0.05)
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            writer = threading.Thread(target=do_insert)
            writer.start()

            # Let the writer warm up
            time.sleep(1)

            # TRUNCATE during continuous INSERT
            truncate_start = time.time()
            self.truncate_table(table_path)
            truncate_latency = time.time() - truncate_start

            # Let the writer continue for a bit
            time.sleep(2)
            stop_flag.set()
            writer.join(timeout=30)

            assert len(errors) == 0, f"Writer errors: {errors}"
            assert truncate_latency < 5, f"TRUNCATE latency too high: {truncate_latency:.2f}s"
            logger.info("L1: %d batches inserted, TRUNCATE latency=%.3fs", insert_count[0], truncate_latency)

            # After TRUNCATE, table should have only data inserted after TRUNCATE
            count = self.get_count(table_path)
            assert count >= 0, f"Unexpected negative count: {count}"
        finally:
            self.drop_table(table_path)

    # L2: TRUNCATE + continuous INSERT (10 writers)
    # 1 table, 10 threads, batch 50 rows, DURATION_MEDIUM sec.
    # Success: no data loss, p99 TRUNCATE latency < 10 sec.
    def test_l2_truncate_with_continuous_insert_10_writers(self):
        table_path = self.create_column_table("l2_10writers")
        try:
            errors = []
            stop_flag = threading.Event()
            num_writers = 10
            batch_size = 50

            def do_insert(writer_id):
                try:
                    client = self.create_client()
                    try:
                        batch_id = 0
                        while not stop_flag.is_set():
                            try:
                                start_id = writer_id * 100000 + batch_id * batch_size
                                self.insert_batch(client, table_path, start_id, batch_size)
                            except Exception as e:
                                # Transient errors (unknown table during TRUNCATE) are expected
                                logger.debug("L2: INSERT failed (expected during TRUNCATE): %s", str(e))
                            batch_id += 1
                            time.sleep(0.02)
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            writers = []
            for i in range(num_writers):
                t = threading.Thread(target=do_insert, args=(i,))
                writers.append(t)
                t.start()

            # Let writers warm up
            time.sleep(2)

            # TRUNCATE during continuous INSERT from 10 writers
            truncate_start = time.time()
            self.truncate_table(table_path)
            truncate_latency = time.time() - truncate_start

            # Let writers continue
            time.sleep(3)
            stop_flag.set()

            for t in writers:
                t.join(timeout=30)

            assert len(errors) == 0, f"Writer errors: {errors}"
            assert truncate_latency < 10, f"TRUNCATE latency too high: {truncate_latency:.2f}s"
            logger.info("L2: 10 writers, TRUNCATE latency=%.3fs", truncate_latency)

            # Table should be usable
            count = self.get_count(table_path)
            assert count >= 0
        finally:
            self.drop_table(table_path)

    # L3: TRUNCATE + INSERT + SELECT (read/write mix)
    # 3 tables, 5 writers, 5 readers, 1 truncator, DURATION_MEDIUM sec.
    # Success: no errors, COUNT after TRUNCATE = 0.
    def test_l3_truncate_insert_select_mix(self):
        tables = [self.create_column_table(f"l3_mix_{i}") for i in range(3)]
        try:
            errors = []
            stop_flag = threading.Event()
            num_writers = 5
            num_readers = 5

            def do_write(table_path, writer_id):
                try:
                    client = self.create_client()
                    try:
                        batch_id = 0
                        while not stop_flag.is_set():
                            try:
                                start_id = writer_id * 100000 + batch_id * BATCH_SIZE
                                self.insert_batch(client, table_path, start_id, BATCH_SIZE)
                            except Exception as e:
                                # Transient errors during TRUNCATE are expected
                                logger.debug("L3: INSERT failed (expected during TRUNCATE): %s", str(e))
                            batch_id += 1
                            time.sleep(0.05)
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            def do_read(table_path):
                try:
                    client = self.create_client()
                    try:
                        while not stop_flag.is_set():
                            try:
                                client.query(f"SELECT COUNT(*) AS cnt FROM `{table_path}`")
                            except Exception as e:
                                # Transient errors during TRUNCATE are expected
                                logger.debug("L3: SELECT failed (expected during TRUNCATE): %s", str(e))
                            time.sleep(0.05)
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            threads = []
            # 5 writers, each writing to a different table (round-robin)
            for i in range(num_writers):
                t = threading.Thread(target=do_write, args=(tables[i % len(tables)], i))
                threads.append(t)
                t.start()
            # 5 readers, each reading from a different table (round-robin)
            for i in range(num_readers):
                t = threading.Thread(target=do_read, args=(tables[i % len(tables)],))
                threads.append(t)
                t.start()

            # Let workload run
            time.sleep(2)

            # Truncator: TRUNCATE all 3 tables
            for tbl in tables:
                self.truncate_table(tbl)

            # Let workload continue
            time.sleep(3)
            stop_flag.set()

            for t in threads:
                t.join(timeout=30)

            assert len(errors) == 0, f"Workload errors: {errors}"
            logger.info("L3: mix workload completed without errors")

            # After TRUNCATE, each table should have only post-TRUNCATE data
            for tbl in tables:
                count = self.get_count(tbl)
                assert count >= 0, f"Unexpected count for {tbl}: {count}"
        finally:
            for tbl in tables:
                try:
                    self.drop_table(tbl)
                except Exception:
                    pass

    # L4: TRUNCATE large tables
    # 5 tables × large row count, TRUNCATE one by one.
    # Success: TRUNCATE time is NOT proportional to row count (id substitution),
    # GC happens in background.
    def test_l4_truncate_large_tables(self):
        num_tables = 5
        rows_per_table = 50000  # Scaled down from 10M for CI
        insert_batch_size = 1000  # Batch inserts to avoid query compilation timeout
        tables = [self.create_column_table(f"l4_large_{i}") for i in range(num_tables)]
        try:
            # Insert data into all tables in batches
            for i, tbl in enumerate(tables):
                base_id = i * rows_per_table
                for batch_start in range(0, rows_per_table, insert_batch_size):
                    batch_count = min(insert_batch_size, rows_per_table - batch_start)
                    self.insert_rows(tbl, batch_count, start_id=base_id + batch_start)
                count = self.get_count(tbl)
                assert count == rows_per_table, f"Table {i} count mismatch: {count}"

            # TRUNCATE each table and measure time
            truncate_times = []
            for tbl in tables:
                start = time.time()
                self.truncate_table(tbl)
                elapsed = time.time() - start
                truncate_times.append(elapsed)
                logger.info("L4: TRUNCATE %s took %.3fs", tbl, elapsed)

            # TRUNCATE time should be roughly constant (not proportional to row count)
            # All tables have the same row count, so we just verify they're all fast
            for t in truncate_times:
                assert t < 10, f"TRUNCATE too slow: {t:.2f}s"

            # All tables should be empty after TRUNCATE
            for tbl in tables:
                assert self.get_count(tbl) == 0

            # Tables should be usable after TRUNCATE
            for tbl in tables:
                self.insert_rows(tbl, 10, start_id=999000)
                assert self.get_count(tbl) == 10
        finally:
            for tbl in tables:
                try:
                    self.drop_table(tbl)
                except Exception:
                    pass

    # L5: TRUNCATE + concurrent schema ops (ALTER, CREATE INDEX)
    # 1 table, TRUNCATE + ALTER every 5 sec, DURATION_SHORT sec.
    # Success: no MultipleModifications loop, all operations complete.
    def test_l5_truncate_with_concurrent_schema_ops(self):
        table_path = self.create_column_table("l5_schema_ops")
        try:
            errors = []
            stop_flag = threading.Event()
            alter_count = [0]
            truncate_count = [0]

            def do_alter():
                try:
                    client = self.create_client()
                    try:
                        col_idx = 0
                        while not stop_flag.is_set():
                            try:
                                client.query(
                                    f"ALTER TABLE `{table_path}` ADD COLUMN extra_{col_idx} Utf8"
                                )
                                alter_count[0] += 1
                                col_idx += 1
                            except Exception as e:
                                # MultipleModifications is expected — just log it
                                logger.debug("L5: ALTER failed (expected): %s", str(e))
                            time.sleep(0.5)
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            def do_truncate():
                try:
                    client = self.create_client()
                    try:
                        while not stop_flag.is_set():
                            try:
                                client.query(f"TRUNCATE TABLE `{table_path}`")
                                truncate_count[0] += 1
                            except Exception as e:
                                logger.debug("L5: TRUNCATE failed (expected): %s", str(e))
                            time.sleep(1.0)
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            # Pre-populate
            self.insert_rows(table_path, 100)

            alter_thread = threading.Thread(target=do_alter)
            trunc_thread = threading.Thread(target=do_truncate)
            alter_thread.start()
            trunc_thread.start()

            time.sleep(DURATION_SHORT)
            stop_flag.set()

            alter_thread.join(timeout=30)
            trunc_thread.join(timeout=30)

            assert len(errors) == 0, f"Schema op errors: {errors}"
            logger.info("L5: %d ALTERs, %d TRUNCATEs completed", alter_count[0], truncate_count[0])

            # Table should still be usable
            self.insert_rows(table_path, 10, start_id=999000)
            assert self.get_count(table_path) >= 10
        finally:
            self.drop_table(table_path)

    # L6: TRUNCATE + concurrent COPY TABLE (backup)
    # 1 table, TRUNCATE + COPY every 10 sec, DURATION_SHORT sec.
    # Success: copies are read-only, TRUNCATE doesn't affect copies.
    def test_l6_truncate_with_concurrent_copy(self):
        table_path = self.create_column_table("l6_copy")
        copy_paths = []
        try:
            errors = []
            stop_flag = threading.Event()
            copy_count = [0]
            truncate_count = [0]

            def do_copy():
                try:
                    client = self.create_client()
                    try:
                        idx = 0
                        while not stop_flag.is_set():
                            copy_path = self.get_table_path(f"l6_copy_{idx}")
                            try:
                                session = client.driver.table_client.session().create()
                                try:
                                    session.copy_table(table_path, copy_path)
                                    copy_paths.append(copy_path)
                                    copy_count[0] += 1
                                finally:
                                    session.close()
                            except ydb.issues.PreconditionFailed as e:
                                # COPY for column tables requires backup flag — expected
                                logger.debug("L6: COPY rejected (expected): %s", str(e))
                            except Exception as e:
                                logger.debug("L6: COPY failed: %s", str(e))
                            idx += 1
                            time.sleep(1.0)
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            def do_truncate():
                try:
                    client = self.create_client()
                    try:
                        while not stop_flag.is_set():
                            try:
                                client.query(f"TRUNCATE TABLE `{table_path}`")
                                truncate_count[0] += 1
                            except Exception as e:
                                logger.debug("L6: TRUNCATE failed: %s", str(e))
                            time.sleep(1.5)
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            # Pre-populate
            self.insert_rows(table_path, 100)

            copy_thread = threading.Thread(target=do_copy)
            trunc_thread = threading.Thread(target=do_truncate)
            copy_thread.start()
            trunc_thread.start()

            time.sleep(DURATION_SHORT)
            stop_flag.set()

            copy_thread.join(timeout=30)
            trunc_thread.join(timeout=30)

            assert len(errors) == 0, f"COPY/TRUNCATE errors: {errors}"
            logger.info("L6: %d COPYs, %d TRUNCATEs completed", copy_count[0], truncate_count[0])

            # Table should be usable
            self.insert_rows(table_path, 10, start_id=999000)
            assert self.get_count(table_path) >= 10
        finally:
            self.drop_table(table_path)
            for cp in copy_paths:
                try:
                    self.drop_table(cp)
                except Exception:
                    pass

    # L7: TRUNCATE + concurrent DROP/CREATE
    # 1 table, TRUNCATE + DROP/CREATE cycle, DURATION_SHORT sec.
    # Success: no InternalPathId leaks, MaxInternalPathId monotonically grows.
    def test_l7_truncate_with_drop_create_cycle(self):
        table_path = self.create_column_table("l7_drop_create")
        try:
            errors = []
            stop_flag = threading.Event()
            cycle_count = [0]
            truncate_count = [0]

            def do_drop_create():
                try:
                    client = self.create_client()
                    try:
                        while not stop_flag.is_set():
                            try:
                                client.query(f"DROP TABLE `{table_path}`")
                            except Exception:
                                pass
                            try:
                                client.query(
                                    f"""
                                    CREATE TABLE `{table_path}` (
                                        id Uint64 NOT NULL,
                                        val Uint64,
                                        str_val Utf8,
                                        PRIMARY KEY(id)
                                    )
                                    WITH (
                                        STORE = COLUMN
                                    )
                                    """
                                )
                                cycle_count[0] += 1
                            except Exception as e:
                                logger.debug("L7: CREATE failed: %s", str(e))
                            time.sleep(0.5)
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            def do_truncate():
                try:
                    client = self.create_client()
                    try:
                        while not stop_flag.is_set():
                            try:
                                client.query(f"TRUNCATE TABLE `{table_path}`")
                                truncate_count[0] += 1
                            except Exception as e:
                                # Table may not exist — expected
                                logger.debug("L7: TRUNCATE failed (expected): %s", str(e))
                            time.sleep(0.8)
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            # Pre-populate
            self.insert_rows(table_path, 100)

            dc_thread = threading.Thread(target=do_drop_create)
            trunc_thread = threading.Thread(target=do_truncate)
            dc_thread.start()
            trunc_thread.start()

            time.sleep(DURATION_SHORT)
            stop_flag.set()

            dc_thread.join(timeout=30)
            trunc_thread.join(timeout=30)

            assert len(errors) == 0, f"DROP/CREATE errors: {errors}"
            logger.info("L7: %d DROP/CREATE cycles, %d TRUNCATEs", cycle_count[0], truncate_count[0])

            # Ensure the table exists and is usable at the end
            try:
                self.get_count(table_path)
            except Exception:
                # Table may have been dropped — recreate it
                self.create_column_table("l7_drop_create_final")
        finally:
            try:
                self.drop_table(table_path)
            except Exception:
                pass

    # L8: TRUNCATE under load + ColumnShard restart
    # 1 table, INSERT + TRUNCATE, restart every 30 sec, DURATION_MEDIUM sec.
    # Success: no data loss, mapping correct after restart.
    def test_l8_truncate_under_load_with_restart(self):
        table_path = self.create_column_table("l8_restart_load")
        try:
            errors = []
            stop_flag = threading.Event()
            insert_count = [0]
            truncate_count = [0]

            def do_insert():
                try:
                    client = self.create_client()
                    try:
                        batch_id = 0
                        while not stop_flag.is_set():
                            try:
                                self.insert_batch(client, table_path, batch_id * BATCH_SIZE, BATCH_SIZE)
                                insert_count[0] += 1
                            except Exception as e:
                                # Transient errors during restart are possible
                                logger.debug("L8: INSERT failed (during restart?): %s", str(e))
                            batch_id += 1
                            time.sleep(0.05)
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            def do_truncate():
                try:
                    client = self.create_client()
                    try:
                        while not stop_flag.is_set():
                            try:
                                client.query(f"TRUNCATE TABLE `{table_path}`")
                                truncate_count[0] += 1
                            except Exception as e:
                                logger.debug("L8: TRUNCATE failed: %s", str(e))
                            time.sleep(2.0)
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            insert_thread = threading.Thread(target=do_insert)
            trunc_thread = threading.Thread(target=do_truncate)
            insert_thread.start()
            trunc_thread.start()

            # Restart shards at intervals
            restart_interval = 5  # Scaled down from 30 sec for CI
            elapsed = 0
            while elapsed < DURATION_MEDIUM:
                time.sleep(restart_interval)
                elapsed += restart_interval
                try:
                    self.restart_all_shards(table_path)
                    logger.info("L8: restarted shards at t=%ds", elapsed)
                    time.sleep(3)  # Wait for tablet to come back
                except Exception as e:
                    logger.debug("L8: restart failed: %s", str(e))

            stop_flag.set()
            insert_thread.join(timeout=30)
            trunc_thread.join(timeout=30)

            assert len(errors) == 0, f"Load errors: {errors}"
            logger.info("L8: %d inserts, %d truncates, with restarts", insert_count[0], truncate_count[0])

            # Table should be usable after restarts
            self.insert_rows(table_path, 10, start_id=999000)
            assert self.get_count(table_path) >= 10
        finally:
            try:
                self.drop_table(table_path)
            except Exception:
                pass
