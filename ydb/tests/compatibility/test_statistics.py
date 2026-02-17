# -*- coding: utf-8 -*-
import json
import logging
import pytest
import random
import threading

from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.oss.ydb_sdk_import import ydb

TABLE_NAME = "table"

logger = logging.getLogger(__name__)


class TestStatisticsTLI(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            tenant_db="mydb",
        )

    def write_data(self):
        driver = self.create_driver()
        try:
            with ydb.QuerySessionPool(driver) as session_pool:
                for _ in range(10):
                    random_key = random.randint(1, 1000)

                    def operation(session, key=random_key):
                        session.transaction().execute(
                            f"""
                            UPSERT INTO {TABLE_NAME} (key, value) VALUES ({key}, 'Hello, YDB {key}!')
                            """,
                            commit_tx=True
                        )
                    session_pool.retry_operation_sync(operation)
        finally:
            driver.stop()

    def read_data(self):
        queries = [
            f"SELECT value FROM {TABLE_NAME} WHERE key > 1 AND key <= 200;",
            f"UPSERT INTO {TABLE_NAME} (key, value) SELECT key, value FROM {TABLE_NAME} WHERE key > 1 AND key <= 200;",
            f"UPSERT INTO {TABLE_NAME} (key, value) SELECT key, value FROM {TABLE_NAME} WHERE key > 200 AND key <= 400;",
            f"SELECT value FROM {TABLE_NAME} WHERE key > 200 AND key <= 400;",
            f"UPSERT INTO {TABLE_NAME} (key, value) SELECT key, value FROM {TABLE_NAME} WHERE key > 200 AND key <= 400;",
            f"UPSERT INTO {TABLE_NAME} (key, value) SELECT key, value FROM {TABLE_NAME} WHERE key > 400 AND key <= 600;",
            f"SELECT value FROM {TABLE_NAME} WHERE key > 400 AND key <= 600;",
            f"UPSERT INTO {TABLE_NAME} (key, value) SELECT key, value FROM {TABLE_NAME} WHERE key > 400 AND key <= 600;",
            f"UPSERT INTO {TABLE_NAME} (key, value) SELECT key, value FROM {TABLE_NAME} WHERE key > 600 AND key <= 800;",
            f"SELECT value FROM {TABLE_NAME} WHERE key > 600 AND key <= 800;",
            f"UPSERT INTO {TABLE_NAME} (key, value) SELECT key, value FROM {TABLE_NAME} WHERE key > 600 AND key <= 800;",
            f"UPSERT INTO {TABLE_NAME} (key, value) SELECT key, value FROM {TABLE_NAME} WHERE key > 800 AND key <= 1000;",
            f"SELECT value FROM {TABLE_NAME} WHERE key > 800 AND key <= 1000;",
        ]

        driver = self.create_driver()
        try:
            with ydb.QuerySessionPool(driver) as session_pool:
                for _ in range(10):
                    for query in queries:
                        def operation(session, q=query):
                            session.transaction().execute(q, commit_tx=True)
                        session_pool.retry_operation_sync(operation)
        finally:
            driver.stop()

    def generate_tli(self):
        errors = []

        def run_with_error_capture(target):
            try:
                target()
            except Exception as exc:
                errors.append((threading.current_thread().name, exc))

        # Create threads for write and read operations
        threads = [
            threading.Thread(target=run_with_error_capture, args=(self.write_data,), name="write_data"),
            threading.Thread(target=run_with_error_capture, args=(self.read_data,), name="read_data"),
        ]

        # Start threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete with a timeout
        for thread in threads:
            thread.join(timeout=600)
            assert not thread.is_alive(), f"{thread.name} thread timed out after 600s"

        if errors:
            name, error = errors[0]
            raise AssertionError(f"{name} worker thread failed") from error

    def check_partition_stats(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = """SELECT * FROM `.sys/partition_stats`;"""
            result_sets = session_pool.execute_with_retries(query)

            assert len(result_sets[0].rows) > 0

    def create_table(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                    CREATE TABLE {TABLE_NAME} (
                    key Int64 NOT NULL,
                    value Utf8 NOT NULL,
                    PRIMARY KEY (key)
                )
                WITH (
                    PARTITION_AT_KEYS=(0, 300, 600, 900)
                )
                """
            session_pool.execute_with_retries(query)

    def test_statistics_tli(self):
        self.create_table()

        self.generate_tli()
        self.check_partition_stats()

        self.change_cluster_version()

        self.generate_tli()
        self.check_partition_stats()


class TestStatisticsFollowersRollingUpdate(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (25, 1):
            pytest.skip("Only available since 25-1")

        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_follower_stats": True
            }
        )

    def write_data(self):
        def operation(session):
            for _ in range(100):
                random_key = random.randint(1, 1000)
                session.transaction().execute(
                    f"""
                    UPSERT INTO {TABLE_NAME} (key, value) VALUES ({random_key}, 'Hello, YDB {random_key}!')
                    """,
                    commit_tx=True
                )

        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.retry_operation_sync(operation)

    def check_statistics(self):
        queries = [
            "SELECT * FROM `.sys/partition_stats`"
        ]

        with ydb.QuerySessionPool(self.driver) as session_pool:
            for query in queries:
                result_sets = session_pool.execute_with_retries(query)
                assert len(result_sets[0].rows) > 0

    def create_table(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                CREATE TABLE {TABLE_NAME} (
                    key Int64 NOT NULL,
                    value Utf8 NOT NULL,
                    PRIMARY KEY (key)
                )
                WITH (
                    AUTO_PARTITIONING_BY_SIZE = ENABLED,
                    AUTO_PARTITIONING_PARTITION_SIZE_MB = 1,
                    READ_REPLICAS_SETTINGS = "ANY_AZ:1"
                );
            """
            session_pool.execute_with_retries(query)

    def test_example(self):
        self.create_table()

        for _ in self.roll():
            self.write_data()
            self.check_statistics()


class TestBaseStatisticsRollingUpdate(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.row_count = 0
        yield from self.setup_cluster(
            tenant_db="mydb",
            additional_log_configs={
                'STATISTICS': LogLevels.DEBUG,
            },
        )

    def create_table(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                    CREATE TABLE {TABLE_NAME} (
                    key Int64 NOT NULL,
                    value Utf8 NOT NULL,
                    PRIMARY KEY (key)
                )
                """
            session_pool.execute_with_retries(query)

    def write_data(self, count):
        next_row_count = self.row_count + count

        values_str = ", ".join(f"({k}, 'Hello, YDB {k}!')"
                               for k in range(self.row_count, next_row_count))

        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"UPSERT INTO {TABLE_NAME} (key, value) VALUES {values_str}")
        self.row_count = next_row_count

    def get_planner_row_count_estimate(self):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            res = session_pool.explain_with_retries(f"SELECT count(*) FROM {TABLE_NAME}")
            logger.debug(f"SELECT count explain: {res}")
            explain = json.loads(res)

        def get_estimate(plan_node):
            if plan_node.get("Name") == "TableFullScan" and plan_node.get("Table") == TABLE_NAME:
                rc = plan_node.get("E-Rows")
                return int(rc) if rc is not None else None
            for p in plan_node.get("Plans", []) + plan_node.get("Operators", []):
                rc = get_estimate(p)
                if rc is not None:
                    return rc

        rc = get_estimate(explain["Plan"])
        logger.debug(f"planner row count estimate: {rc}")
        return rc

    def test(self):
        BATCH_SIZE = 100
        self.create_table()
        self.write_data(BATCH_SIZE)

        assert wait_for(
            lambda: self.get_planner_row_count_estimate() == self.row_count, timeout_seconds=300), \
            "base stats not ready before node roll"

        for _ in self.roll():
            self.write_data(BATCH_SIZE)
            rc_estimate = self.get_planner_row_count_estimate()
            assert rc_estimate <= self.row_count
            assert rc_estimate >= BATCH_SIZE

        assert wait_for(
            lambda: self.get_planner_row_count_estimate() == self.row_count, timeout_seconds=300), \
            "base stats not ready after node roll"
