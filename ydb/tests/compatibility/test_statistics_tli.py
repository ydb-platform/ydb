# -*- coding: utf-8 -*-
import pytest
import time
import random
import threading
from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb

TABLE_NAME = "tli_table"

class TestRollingUpdateStatisticsTli(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        #
        # Setup cluster
        #
        yield from self.setup_cluster(
            # # Some feature flags can be passed. And other KikimrConfigGenerator options
            # extra_feature_flags={
            #     "some_feature_flag": True,
            # }
        )

    def write_data(session_pool):
        def operation(session):
            for _ in range(100):
                random_key = random.randint(1, 1000)
                session.transaction().execute(
                    f"""
                    UPSERT INTO {TABLE_NAME} (key, value) VALUES ({random_key}, 'Hello, YDB {random_key}!')
                    """,
                    commit_tx=True
                )

        session_pool.retry_operation_sync(operation)


    def read_data(session_pool):
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
                # Get newly created transaction id
                tx = session.transaction().begin()
                
                for query in queries:
                    result = session.transaction().execute(query, commit_tx=True)
                    # Process results if needed
                    for row in result[0].rows:
                        print(row)

                # Commit active transaction(tx)
                tx.commit()

        session_pool.retry_operation_sync(operation)        

    def test_example(self):

        with ydb.QuerySessionPool(self.driver) as session_pool:
            query = f"""
                    CREATE TABLE {TABLE_NAME} (
                    key Int64 NOT NULL,
                    value Int64 NOT NULL,
                    PRIMARY KEY (key)
                ) """
            session_pool.execute_with_retries(query)

        for _ in self.roll():  # every iteration is a step in rolling upgrade process

            with ydb.QuerySessionPool(self.driver) as session_pool:
                self.driver.wait(timeout=5)  # Ensure driver is initialized

                # Create threads for write and read operations
                threads = [
                    threading.Thread(target=self.write_data, args=(session_pool,)),
                    threading.Thread(target=self.read_data, args=(session_pool,))
                ]

                # Start threads
                for thread in threads:
                    thread.start()

                # Wait for all threads to complete
                for thread in threads:
                    thread.join()    


                # test statistics            

                query = f"""SELECT * FROM `.sys/partition_stats`;"""
                result_sets = session_pool.execute_with_retries(query)

                assert result_sets[0].rows.count > 0
