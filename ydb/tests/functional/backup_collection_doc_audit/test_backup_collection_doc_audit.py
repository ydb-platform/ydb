# -*- coding: utf-8 -*-
import time

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR

import ydb


class TestBackupCollectionDocumentationAudit:
    @classmethod
    def setup_class(cls):
        config = KikimrConfigGenerator(
            erasure=Erasure.NONE,
            extra_feature_flags=["enable_backup_service"],
        )
        cls.cluster = KiKiMR(config)
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database="/Root",
                endpoint=cls.cluster.nodes[1].endpoint,
            )
        )
        cls.driver.wait(timeout=60)

    @classmethod
    def teardown_class(cls):
        cls.driver.stop()
        cls.cluster.stop()

    def execute(self, query):
        with ydb.QuerySessionPool(self.driver) as pool:
            return pool.execute_with_retries(query)

    def execute_backup_statement(self, query):
        return ydb.ScriptingClient(self.driver).execute_yql(query)

    def wait_for_snapshot_count(self, collection, expected, timeout=30):
        deadline = time.monotonic() + timeout
        path = f"/Root/.backups/collections/{collection}"
        while time.monotonic() < deadline:
            children = self.driver.scheme_client.list_directory(path).children
            snapshots = [child.name for child in children if not child.name.startswith(".")]
            if len(snapshots) >= expected:
                return snapshots
            time.sleep(0.1)
        raise AssertionError(f"Expected {expected} snapshots in {collection}")

    def wait_for_value(self, table, expected, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                result = self.execute(f"SELECT value FROM `{table}` WHERE id = 1;")
                values = [row.value for row in result[0].rows]
                if values == [expected]:
                    return
            except ydb.Error:
                pass
            time.sleep(0.1)
        raise AssertionError(f"Value {expected!r} was not restored into {table}")

    @staticmethod
    def wait_for_start_of_second():
        while time.time() % 1.0 >= 0.15:
            time.sleep(0.01)

    def test_rapid_consecutive_backups_do_not_collide(self):
        self.execute(
            """
                CREATE TABLE `rapid_orders` (
                    id Uint64 NOT NULL,
                    value Utf8,
                    PRIMARY KEY (id)
                );
            """
        )
        self.execute(
            """
                UPSERT INTO `rapid_orders` (id, value) VALUES (1, "full"u);
            """
        )
        self.execute_backup_statement(
            """
                CREATE BACKUP COLLECTION rapid_collection
                    (TABLE `/Root/rapid_orders`)
                WITH (
                    STORAGE = 'cluster',
                    INCREMENTAL_BACKUP_ENABLED = 'true'
                );
            """
        )
        self.wait_for_start_of_second()
        self.execute_backup_statement("BACKUP rapid_collection;")
        self.wait_for_snapshot_count("rapid_collection", 1)

        self.execute(
            """
                UPDATE `rapid_orders` SET value = "incremental"u WHERE id = 1;
            """
        )
        self.execute_backup_statement("BACKUP rapid_collection INCREMENTAL;")

    def test_incremental_backup_continues_after_restore(self):
        self.execute(
            """
                CREATE TABLE `restore_orders` (
                    id Uint64 NOT NULL,
                    value Utf8,
                    PRIMARY KEY (id)
                );
            """
        )
        self.execute(
            """
                UPSERT INTO `restore_orders` (id, value) VALUES (1, "full"u);
            """
        )
        self.execute_backup_statement(
            """
                CREATE BACKUP COLLECTION restore_collection
                    (TABLE `/Root/restore_orders`)
                WITH (
                    STORAGE = 'cluster',
                    INCREMENTAL_BACKUP_ENABLED = 'true'
                );
            """
        )
        self.execute_backup_statement("BACKUP restore_collection;")
        self.wait_for_snapshot_count("restore_collection", 1)

        time.sleep(1.1)
        self.execute(
            """
                UPDATE `restore_orders`
                SET value = "first incremental"u
                WHERE id = 1;
            """
        )
        self.execute_backup_statement("BACKUP restore_collection INCREMENTAL;")
        self.wait_for_snapshot_count("restore_collection", 2)

        self.execute("DROP TABLE `restore_orders`;")
        self.execute_backup_statement("RESTORE restore_collection;")
        self.wait_for_value("restore_orders", "first incremental")

        time.sleep(1.1)
        self.execute(
            """
                UPDATE `restore_orders`
                SET value = "after restore"u
                WHERE id = 1;
            """
        )

        self.execute_backup_statement("BACKUP restore_collection INCREMENTAL;")
