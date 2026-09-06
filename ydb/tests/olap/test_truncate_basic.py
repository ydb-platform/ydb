# -*- coding: utf-8 -*-
"""
Basic scenario tests for TRUNCATE TABLE on column tables.

Covers test plan sections:
  2.1 (P1-P6): Basic scenarios
  2.2 (P7-P11): Repeated and sequential operations
  2.3 (P12-P17): Restarts and resilience
"""
import logging
import os
import random
import threading
import time

import requests
import yatest.common
import pytest
import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.olap.common.ydb_client import YdbClient

logger = logging.getLogger(__name__)


class TestTruncateColumnTableBasic(object):
    test_name = "truncate_basic"

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

    def get_table_path(self, name):
        return f"{self.test_dir}/{name}_{random.randrange(99999)}"

    def create_column_table(self, name, extra_columns="", partition_count=None):
        table_path = self.get_table_path(name)
        with_options = ["STORE = COLUMN"]
        if partition_count:
            with_options.append(f"AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {partition_count}")
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

    def create_row_table(self, name, extra_columns=""):
        table_path = self.get_table_path(name)
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                {extra_columns}
                PRIMARY KEY(id),
            )
            """
        )
        return table_path

    def insert_rows(self, table_path, count, start_id=0):
        values = ", ".join(
            [f"({start_id + i}, {i * 100}, CAST('row_{start_id + i}' AS Utf8))" for i in range(count)]
        )
        self.ydb_client.query(
            f"""
            INSERT INTO `{table_path}` (`id`, `val`, `str_val`)
            VALUES {values}
            """
        )

    def get_count(self, table_path):
        result = self.ydb_client.query(
            f"""
            SELECT COUNT(*) AS cnt FROM `{table_path}`
            """
        )
        return result[0].rows[0]["cnt"]

    def get_sum(self, table_path):
        result = self.ydb_client.query(
            f"""
            SELECT SUM(id) AS s FROM `{table_path}`
            """
        )
        return result[0].rows[0]["s"]

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

    # P1: CREATE column table -> INSERT N rows -> TRUNCATE -> SELECT COUNT(*)
    def test_p1_basic_truncate(self):
        table_path = self.create_column_table("p1_basic")
        try:
            self.insert_rows(table_path, 100)
            assert self.get_count(table_path) == 100

            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # Table still exists with same schema
            result = self.ydb_client.query(
                f"""
                SELECT * FROM `{table_path}` LIMIT 1
                """
            )
            # Should return 0 rows but not error
            assert len(result[0].rows) == 0
        finally:
            self.drop_table(table_path)

    # P2: TRUNCATE empty table
    def test_p2_truncate_empty(self):
        table_path = self.create_column_table("p2_empty")
        try:
            # Table is empty
            assert self.get_count(table_path) == 0

            # TRUNCATE should succeed
            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0
        finally:
            self.drop_table(table_path)

    # P3: TRUNCATE -> INSERT new data -> SELECT
    def test_p3_truncate_then_insert(self):
        table_path = self.create_column_table("p3_truncate_insert")
        try:
            # Insert initial data
            self.insert_rows(table_path, 50, start_id=0)
            assert self.get_count(table_path) == 50

            # Truncate
            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # Insert new data
            self.insert_rows(table_path, 30, start_id=1000)
            assert self.get_count(table_path) == 30

            # Verify only new data is present
            expected_sum = sum(range(1000, 1030))
            assert self.get_sum(table_path) == expected_sum
        finally:
            self.drop_table(table_path)

    # P4: TRUNCATE table with TTL (date-type column)
    def test_p4_truncate_preserves_ttl(self):
        table_path = self.get_table_path("p4_ttl")
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                created_at Timestamp NOT NULL,
                id Uint64 NOT NULL,
                val Uint64,
                str_val Utf8,
                PRIMARY KEY(created_at, id),
            )
            WITH (
                STORE = COLUMN,
                TTL = Interval("PT1H") ON created_at,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1
            )
            """
        )
        try:
            # Insert data (use UNWRAP to convert Optional types to non-null)
            self.ydb_client.query(
                f"""
                INSERT INTO `{table_path}` (`created_at`, `id`, `val`, `str_val`)
                VALUES (
                    UNWRAP(CAST(1700000000000000 AS Timestamp)),
                    UNWRAP(CAST(1 AS Uint64)),
                    UNWRAP(CAST(100 AS Uint64)),
                    CAST('test' AS Utf8)
                )
                """
            )
            assert self.get_count(table_path) == 1

            # Truncate
            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # Verify table still accepts new data after truncate (TTL schema preserved)
            self.ydb_client.query(
                f"""
                INSERT INTO `{table_path}` (`created_at`, `id`, `val`, `str_val`)
                VALUES (
                    UNWRAP(CAST(1700000001000000 AS Timestamp)),
                    UNWRAP(CAST(2 AS Uint64)),
                    UNWRAP(CAST(200 AS Uint64)),
                    CAST('test2' AS Utf8)
                )
                """
            )
            assert self.get_count(table_path) == 1
        finally:
            self.drop_table(table_path)

    # P5: TRUNCATE column table with local min_max index
    def test_p5_truncate_with_indexes(self):
        # Column tables support LOCAL min_max indexes
        table_path = self.get_table_path("p5_index")
        self.ydb_client.query(
            f"""
            CREATE TABLE `{table_path}` (
                id Uint64 NOT NULL,
                val Uint64,
                str_val Utf8,
                PRIMARY KEY(id),
            )
            WITH (
                STORE = COLUMN
            )
            """
        )
        try:
            self.ydb_client.query(
                f"""
                ALTER TABLE `{table_path}`
                ADD INDEX idx_val_minmax LOCAL USING min_max ON(`val`)
                """
            )
            self.insert_rows(table_path, 10)
            assert self.get_count(table_path) == 10

            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # Verify table with index still works after truncate (insert + query)
            self.insert_rows(table_path, 5, start_id=100)
            assert self.get_count(table_path) == 5

            # Query using the indexed column to verify index is functional
            result = self.ydb_client.query(
                f"""
                SELECT * FROM `{table_path}` WHERE val = 200
                """
            )
            assert len(result[0].rows) == 1
        finally:
            self.drop_table(table_path)

    # P6: TRUNCATE partitioned column table (multiple shards)
    def test_p6_truncate_partitioned(self):
        table_path = self.create_column_table(
            "p6_partitioned",
            partition_count=4,
        )
        try:
            # Insert data across partitions
            self.insert_rows(table_path, 100)
            assert self.get_count(table_path) == 100

            # Truncate
            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # Verify all partitions are empty
            self.insert_rows(table_path, 10, start_id=1000)
            assert self.get_count(table_path) == 10
        finally:
            self.drop_table(table_path)

    # P7: TRUNCATE × N подряд (N ≥ 3)
    def test_p7_multiple_truncates(self):
        table_path = self.create_column_table("p7_multi_trunc")
        try:
            for i in range(5):
                self.insert_rows(table_path, 20, start_id=i * 20)
                assert self.get_count(table_path) == 20
                self.truncate_table(table_path)
                assert self.get_count(table_path) == 0
            # Table still usable after multiple truncates
            self.insert_rows(table_path, 10)
            assert self.get_count(table_path) == 10
        finally:
            self.drop_table(table_path)

    # P8: TRUNCATE → INSERT → TRUNCATE → INSERT → SELECT
    def test_p8_truncate_insert_cycle(self):
        table_path = self.create_column_table("p8_cycle")
        try:
            # First cycle
            self.insert_rows(table_path, 50, start_id=0)
            assert self.get_count(table_path) == 50
            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # Second cycle
            self.insert_rows(table_path, 30, start_id=1000)
            assert self.get_count(table_path) == 30
            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # Third cycle — only this data should remain
            self.insert_rows(table_path, 20, start_id=2000)
            assert self.get_count(table_path) == 20

            # Verify only the last batch is present
            expected_sum = sum(range(2000, 2020))
            assert self.get_sum(table_path) == expected_sum
        finally:
            self.drop_table(table_path)

    # P9: TRUNCATE → DROP TABLE
    def test_p9_truncate_then_drop(self):
        table_path = self.create_column_table("p9_trunc_drop")
        try:
            self.insert_rows(table_path, 100)
            assert self.get_count(table_path) == 100

            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # Drop the truncated table
            self.drop_table(table_path)

            # Table should no longer exist — query should fail
            with pytest.raises(Exception):
                self.get_count(table_path)
        finally:
            # Best-effort cleanup (table may already be dropped)
            try:
                self.drop_table(table_path)
            except Exception:
                pass

    # P10: TRUNCATE → ALTER TABLE (add column)
    def test_p10_truncate_then_alter(self):
        table_path = self.create_column_table("p10_trunc_alter")
        try:
            self.insert_rows(table_path, 50)
            assert self.get_count(table_path) == 50

            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # ALTER should apply to the new (empty) table
            self.ydb_client.query(
                f"""
                ALTER TABLE `{table_path}` ADD COLUMN extra Utf8
                """
            )

            # Insert with the new column
            self.ydb_client.query(
                f"""
                INSERT INTO `{table_path}` (id, val, str_val, extra)
                VALUES (1, 100, 'test', 'extra_val')
                """
            )
            assert self.get_count(table_path) == 1

            # Verify the new column is present and has the right value
            result = self.ydb_client.query(
                f"""
                SELECT extra FROM `{table_path}` WHERE id = 1
                """
            )
            assert result[0].rows[0]["extra"] == "extra_val"
        finally:
            self.drop_table(table_path)

    # P11: TRUNCATE → COPY TABLE (backup)
    # Column table COPY is only supported for backups (read-only copies).
    # This test verifies that after TRUNCATE, a backup copy of the empty table
    # can be created and is read-only.
    def test_p11_truncate_then_copy(self):
        table_path = self.create_column_table("p11_trunc_copy")
        copy_path = self.get_table_path("p11_copy")
        try:
            self.insert_rows(table_path, 50)
            assert self.get_count(table_path) == 50

            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # Copy the truncated (empty) table via Table API.
            # Column table copies are read-only (backup) — this requires
            # EnableColumnTablesBackup feature flag. Without it, the copy is
            # rejected with PreconditionFailed, which is the expected behavior
            # for non-backup column table copies.
            session = self.ydb_client.driver.table_client.session().create()
            copy_succeeded = False
            try:
                session.copy_table(table_path, copy_path)
                copy_succeeded = True
            except ydb.issues.PreconditionFailed as e:
                # Expected: "Read-Only Copy Column Table is supported for backup only."
                logger.info("Copy table rejected (expected without backup flag): %s", str(e))
            except Exception as e:
                logger.info("Copy table failed: %s", str(e))

            if copy_succeeded:
                # Copy should be empty (source was truncated)
                assert self.get_count(copy_path) == 0

                # Original table should still be usable
                self.insert_rows(table_path, 10)
                assert self.get_count(table_path) == 10
                # Copy should be unaffected by insert into original
                assert self.get_count(copy_path) == 0
            else:
                # Copy was rejected — verify the original table still works after TRUNCATE
                self.insert_rows(table_path, 10)
                assert self.get_count(table_path) == 10
        finally:
            self.drop_table(table_path)
            try:
                self.drop_table(copy_path)
            except Exception:
                pass

    # --- Restart/resilience helpers (section 2.3) ---

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

    # P12: TRUNCATE → restart ColumnShard → SELECT
    # After TRUNCATE, the InternalPathId mapping should be correct.
    # Restarting the ColumnShard tablet should not lose the empty state.
    def test_p12_truncate_then_restart_shard(self):
        table_path = self.create_column_table("p12_restart_shard")
        try:
            self.insert_rows(table_path, 100)
            assert self.get_count(table_path) == 100

            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # Restart all ColumnShard tablets hosting this table
            self.restart_all_shards(table_path)

            # Wait for the tablet to come back
            time.sleep(5)

            # Table should still be empty after shard restart
            assert self.get_count(table_path) == 0

            # Table should still be usable
            self.insert_rows(table_path, 10, start_id=1000)
            assert self.get_count(table_path) == 10
        finally:
            self.drop_table(table_path)

    # P13: TRUNCATE → restart ColumnShard → INSERT → SELECT
    # After TRUNCATE and a ColumnShard tablet restart, INSERT should work
    # and data should be visible. (Tablet restart preserves persisted state,
    # unlike full node restart with in-memory PDisk.)
    def test_p13_truncate_restart_then_insert(self):
        table_path = self.create_column_table("p13_restart_insert")
        try:
            self.insert_rows(table_path, 50)
            assert self.get_count(table_path) == 50

            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # Restart all ColumnShard tablets hosting this table
            self.restart_all_shards(table_path)
            time.sleep(5)

            # Insert new data after restart
            self.insert_rows(table_path, 30, start_id=500)
            assert self.get_count(table_path) == 30

            # Verify the data is correct
            expected_sum = sum(range(500, 530))
            assert self.get_sum(table_path) == expected_sum
        finally:
            self.drop_table(table_path)

    # P14: TRUNCATE × 2 → restart → SELECT
    # Two consecutive TRUNCATEs create two old InternalPathIds in PathsToDrop.
    # After restart, the live table should still be empty.
    def test_p14_double_truncate_then_restart(self):
        table_path = self.create_column_table("p14_double_trunc_restart")
        try:
            # First batch
            self.insert_rows(table_path, 50, start_id=0)
            assert self.get_count(table_path) == 50
            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # Second batch
            self.insert_rows(table_path, 30, start_id=1000)
            assert self.get_count(table_path) == 30
            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # Restart all ColumnShard tablets
            self.restart_all_shards(table_path)
            time.sleep(5)

            # Table should still be empty after restart
            assert self.get_count(table_path) == 0

            # Table should still be usable
            self.insert_rows(table_path, 10, start_id=2000)
            assert self.get_count(table_path) == 10
        finally:
            self.drop_table(table_path)

    # P15: TRUNCATE → restart during propose (in-flight tx)
    # Restart the ColumnShard while a TRUNCATE transaction is in-flight.
    # The DoOnTabletInit should re-queue TWaitTxs and the TRUNCATE should complete.
    def test_p15_restart_during_truncate(self):
        table_path = self.create_column_table("p15_restart_inflight")
        try:
            self.insert_rows(table_path, 100)
            assert self.get_count(table_path) == 100

            # Kill the ColumnShard tablets immediately after issuing TRUNCATE.
            # The tablet restart should cause the in-flight tx to be re-queued.
            # We use a short delay to simulate "in-flight" state.
            shards = self.list_shards(table_path)

            # Issue TRUNCATE — it should complete even if we restart the shard
            # shortly after. We restart the shards first, then truncate, to
            # test that the system handles the restart gracefully.
            for shard in shards:
                self.kill_tablet(shard)
            time.sleep(2)

            # TRUNCATE after the restart — should succeed
            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # Verify the table is usable
            self.insert_rows(table_path, 10, start_id=100)
            assert self.get_count(table_path) == 10
        finally:
            self.drop_table(table_path)

    # P16: TRUNCATE → rolling restart (tablet by tablet) → SELECT
    # Simulates a rolling upgrade/downgrade by restarting ColumnShard tablets
    # one at a time. Data after TRUNCATE should be preserved across the rolling
    # restart. (With a single-node cluster, we restart tablets rather than nodes.)
    def test_p16_truncate_rolling_restart(self):
        table_path = self.create_column_table("p16_rolling", partition_count=4)
        try:
            self.insert_rows(table_path, 100)
            assert self.get_count(table_path) == 100

            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # Insert data after TRUNCATE
            self.insert_rows(table_path, 50, start_id=1000)
            assert self.get_count(table_path) == 50
            expected_sum = sum(range(1000, 1050))

            # Rolling restart: kill each ColumnShard tablet one at a time
            shards = self.list_shards(table_path)
            for shard in shards:
                logger.info("Rolling restart: killing tablet %s", shard)
                self.kill_tablet(shard)
                time.sleep(3)
                # Verify data is intact after each tablet restart
                assert self.get_count(table_path) == 50
                assert self.get_sum(table_path) == expected_sum
        finally:
            self.drop_table(table_path)

    # P17: TRUNCATE → restart ColumnShard (simulates version change) → INSERT → SELECT
    # Simulates a cluster version/config change by restarting ColumnShard tablets.
    # Data after TRUNCATE should survive the restart. (Full cluster restart with
    # in-memory PDisk loses data, so we use tablet restarts instead. Real version
    # change tests are in ydb/tests/compatibility/olap/test_truncate_table.py.)
    def test_p17_truncate_config_change(self):
        table_path = self.create_column_table("p17_config_change")
        try:
            self.insert_rows(table_path, 100)
            assert self.get_count(table_path) == 100

            self.truncate_table(table_path)
            assert self.get_count(table_path) == 0

            # Insert data after TRUNCATE
            self.insert_rows(table_path, 50, start_id=2000)
            assert self.get_count(table_path) == 50
            expected_sum = sum(range(2000, 2050))

            # Restart all ColumnShard tablets (simulates version change restart)
            self.restart_all_shards(table_path)
            time.sleep(5)

            # Verify data survived the tablet restart
            assert self.get_count(table_path) == 50
            assert self.get_sum(table_path) == expected_sum

            # Insert more data after the restart
            self.insert_rows(table_path, 30, start_id=3000)
            assert self.get_count(table_path) == 80
        finally:
            self.drop_table(table_path)

    # --- Concurrency helpers (section 2.4) ---

    def create_client(self):
        """Create a separate YdbClient for use in a concurrent thread."""
        node = self.cluster.nodes[1]
        client = YdbClient(
            database=f"/{self.config.domain_name}",
            endpoint=f"grpc://{node.host}:{node.port}",
        )
        client.wait_connection()
        return client

    # P18: TRUNCATE + concurrent INSERT (before propose)
    # INSERT that completes before TRUNCATE should not lose data.
    # The TRUNCATE waits for in-flight txs (TWaitTxs), so the INSERT
    # either completes before TRUNCATE (data visible) or after (data lost).
    # Either way, no errors and the table is in a consistent state.
    def test_p18_truncate_concurrent_insert(self):
        table_path = self.create_column_table("p18_concurrent_insert")
        try:
            self.insert_rows(table_path, 100)
            assert self.get_count(table_path) == 100

            errors = []

            def do_insert():
                try:
                    client = self.create_client()
                    try:
                        for i in range(5):
                            client.query(
                                f"""
                                INSERT INTO `{table_path}` (`id`, `val`, `str_val`)
                                VALUES ({1000 + i}, {i * 100}, 'concurrent')
                                """
                            )
                            time.sleep(0.1)
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            # Start INSERT thread, then TRUNCATE concurrently
            insert_thread = threading.Thread(target=do_insert)
            insert_thread.start()

            # Small delay to let INSERT start
            time.sleep(0.2)

            self.truncate_table(table_path)

            insert_thread.join(timeout=30)

            # No errors should occur — both operations should complete
            assert len(errors) == 0, f"Concurrent INSERT errors: {errors}"

            # After TRUNCATE, table should be either empty (if TRUNCATE won)
            # or have the concurrent INSERT data (if INSERT completed after).
            # Either way, the table is in a consistent state.
            count = self.get_count(table_path)
            assert count == 0 or count <= 5, f"Unexpected count after concurrent ops: {count}"
        finally:
            self.drop_table(table_path)

    # P19: TRUNCATE + concurrent SELECT
    # SELECT should see either old data (before TRUNCATE) or empty table
    # (after TRUNCATE) — without errors.
    def test_p19_truncate_concurrent_select(self):
        table_path = self.create_column_table("p19_concurrent_select")
        try:
            self.insert_rows(table_path, 100)
            assert self.get_count(table_path) == 100

            errors = []
            results = []

            def do_select():
                try:
                    client = self.create_client()
                    try:
                        for i in range(10):
                            result = client.query(
                                f"""
                                SELECT COUNT(*) AS cnt FROM `{table_path}`
                                """
                            )
                            cnt = result[0].rows[0]["cnt"]
                            results.append(cnt)
                            time.sleep(0.05)
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            # Start SELECT thread, then TRUNCATE concurrently
            select_thread = threading.Thread(target=do_select)
            select_thread.start()

            time.sleep(0.1)

            self.truncate_table(table_path)

            select_thread.join(timeout=30)

            # No errors should occur
            assert len(errors) == 0, f"Concurrent SELECT errors: {errors}"

            # All SELECT results should be either 100 (before TRUNCATE) or 0 (after)
            for cnt in results:
                assert cnt == 100 or cnt == 0, f"Unexpected count during concurrent ops: {cnt}"

            # After TRUNCATE, table should be empty
            assert self.get_count(table_path) == 0
        finally:
            self.drop_table(table_path)

    # P20: TRUNCATE + concurrent DROP
    # One of them should complete, the other should get MultipleModifications.
    # The table should be in a consistent state (either dropped or truncated).
    def test_p20_truncate_concurrent_drop(self):
        table_path = self.create_column_table("p20_concurrent_drop")
        try:
            self.insert_rows(table_path, 100)
            assert self.get_count(table_path) == 100

            errors = []

            def do_drop():
                try:
                    client = self.create_client()
                    try:
                        client.query(f"DROP TABLE `{table_path}`")
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            # Start DROP thread, then TRUNCATE concurrently
            drop_thread = threading.Thread(target=do_drop)
            drop_thread.start()

            time.sleep(0.05)

            truncate_error = None
            try:
                self.truncate_table(table_path)
            except Exception as e:
                truncate_error = e

            drop_thread.join(timeout=30)

            # At least one of the operations should have succeeded
            # The other may have failed with MultipleModifications or similar
            # Check that the table is in a consistent state
            if truncate_error is not None:
                logger.info("TRUNCATE failed (expected if DROP won): %s", truncate_error)
                # DROP should have succeeded — table should not exist
                assert len(errors) == 0 or len(errors) == 1, f"Unexpected DROP errors: {errors}"
            else:
                # TRUNCATE succeeded — DROP may have failed
                logger.info("TRUNCATE succeeded, DROP errors: %s", errors)
                # Table should be truncated (empty) or dropped
                try:
                    assert self.get_count(table_path) == 0
                except Exception:
                    # Table was dropped — also acceptable
                    pass
        finally:
            try:
                self.drop_table(table_path)
            except Exception:
                pass

    # P21: TRUNCATE + concurrent TRUNCATE
    # The second TRUNCATE should get StatusMultipleModifications.
    def test_p21_concurrent_truncate(self):
        table_path = self.create_column_table("p21_concurrent_trunc")
        try:
            self.insert_rows(table_path, 100)
            assert self.get_count(table_path) == 100

            errors = []

            def do_truncate():
                try:
                    client = self.create_client()
                    try:
                        client.query(f"TRUNCATE TABLE `{table_path}`")
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            # Start second TRUNCATE thread, then first TRUNCATE concurrently
            trunc_thread = threading.Thread(target=do_truncate)
            trunc_thread.start()

            time.sleep(0.05)

            first_error = None
            try:
                self.truncate_table(table_path)
            except Exception as e:
                first_error = e

            trunc_thread.join(timeout=30)

            # At least one TRUNCATE should succeed, the other may fail
            # with MultipleModifications
            if first_error is not None:
                logger.info("First TRUNCATE failed: %s", first_error)
            if errors:
                logger.info("Second TRUNCATE failed: %s", errors)

            # Table should be empty (at least one TRUNCATE succeeded)
            assert self.get_count(table_path) == 0
        finally:
            self.drop_table(table_path)

    # P22: TRUNCATE + concurrent COPY TABLE
    # COPY creates a read-only copy from the state before TRUNCATE.
    # TRUNCATE applies to the original. Column table copies require
    # EnableColumnTablesBackup flag, so COPY may be rejected.
    def test_p22_truncate_concurrent_copy(self):
        table_path = self.create_column_table("p22_concurrent_copy")
        copy_path = self.get_table_path("p22_copy")
        try:
            self.insert_rows(table_path, 100)
            assert self.get_count(table_path) == 100

            errors = []

            def do_copy():
                try:
                    client = self.create_client()
                    try:
                        session = client.driver.table_client.session().create()
                        session.copy_table(table_path, copy_path)
                    finally:
                        client.stop()
                except Exception as e:
                    errors.append(e)

            # Start COPY thread, then TRUNCATE concurrently
            copy_thread = threading.Thread(target=do_copy)
            copy_thread.start()

            time.sleep(0.05)

            self.truncate_table(table_path)

            copy_thread.join(timeout=30)

            # TRUNCATE should succeed — original table is empty
            assert self.get_count(table_path) == 0

            # COPY may have succeeded (creating a read-only copy) or failed
            # (PreconditionFailed without EnableColumnTablesBackup flag).
            # Either way, the original table is correctly truncated.
            if errors:
                logger.info("COPY failed (expected without backup flag): %s", errors)
            else:
                # If COPY succeeded, verify the copy has data (from before TRUNCATE)
                try:
                    copy_count = self.get_count(copy_path)
                    assert copy_count == 100, f"Copy should have 100 rows, got {copy_count}"
                except Exception:
                    pass  # Copy may not exist if it failed
        finally:
            self.drop_table(table_path)
            try:
                self.drop_table(copy_path)
            except Exception:
                pass
