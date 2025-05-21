# -*- coding: utf-8 -*-
import pytest
import random
import threading
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb

TABLE_NAME = "tli_table"

class TestStatisticsFollowers(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):

        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_follower_stats": True,
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
            "SELECT * FROM `.sys/partition_stats`",
            "SELECT * FROM `.sys/top_partitions`"
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
                    WITH (
                        AUTO_PARTITIONING_BY_SIZE = ENABLED,
                        AUTO_PARTITIONING_PARTITION_SIZE_MB = 1,
                        READ_REPLICAS_SETTINGS = \"PER_AZ:1\"
                    )
                ) """
            session_pool.execute_with_retries(query)


    def test_statistics_followers(self):
        self.create_table()

        self.write_data()
        self.check_statistics()

        self.change_cluster_version()

        self.write_data()
        self.check_statistics()
