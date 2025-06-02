# -*- coding: utf-8 -*-
import pytest
import random
import threading
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb

TABLE_NAME = "table"


class TestStatisticsTLI(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
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

        driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=self.endpoint
            )
        )
        driver.wait()

        with ydb.QuerySessionPool(driver) as session_pool:
            session_pool.retry_operation_sync(operation)

        driver.stop()

    def read_data(self):
        def operation(session):
            for _ in range(100):
                queries = [
                    f"SELECT value FROM {TABLE_NAME} WHERE key > 1 AND key <= 200;",
                    f"UPSERT INTO {TABLE_NAME} (key, value) SELECT key, value FROM {TABLE_NAME} WHERE key > 1 AND key <= 200;"
                    f"UPSERT INTO {TABLE_NAME} (key, value) SELECT key, value FROM {TABLE_NAME} WHERE key > 200 AND key <= 400;"
                    f"SELECT value FROM {TABLE_NAME} WHERE key > 200 AND key <= 400;",
                    f"UPSERT INTO {TABLE_NAME} (key, value) SELECT key, value FROM {TABLE_NAME} WHERE key > 200 AND key <= 400;"
                    f"UPSERT INTO {TABLE_NAME} (key, value) SELECT key, value FROM {TABLE_NAME} WHERE key > 400 AND key <= 600;"
                    f"SELECT value FROM {TABLE_NAME} WHERE key > 400 AND key <= 600;",
                    f"UPSERT INTO {TABLE_NAME} (key, value) SELECT key, value FROM {TABLE_NAME} WHERE key > 400 AND key <= 600;"
                    f"UPSERT INTO {TABLE_NAME} (key, value) SELECT key, value FROM {TABLE_NAME} WHERE key > 600 AND key <= 800;"
                    f"SELECT value FROM {TABLE_NAME} WHERE key > 600 AND key <= 800;",
                    f"UPSERT INTO {TABLE_NAME} (key, value) SELECT key, value FROM {TABLE_NAME} WHERE key > 600 AND key <= 800;"
                    f"UPSERT INTO {TABLE_NAME} (key, value) SELECT key, value FROM {TABLE_NAME} WHERE key > 800 AND key <= 1000;"
                    f"SELECT value FROM {TABLE_NAME} WHERE key > 800 AND key <= 1000;"
                ]

                for query in queries:
                    session.transaction().execute(query, commit_tx=True)

        driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=self.endpoint
            )
        )
        driver.wait()

        with ydb.QuerySessionPool(driver) as session_pool:
            session_pool.retry_operation_sync(operation)

        driver.stop()

    def generate_tli(self):
        # Create threads for write and read operations
        threads = [
            threading.Thread(target=self.write_data),
            threading.Thread(target=self.read_data)
        ]

        # Start threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

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
