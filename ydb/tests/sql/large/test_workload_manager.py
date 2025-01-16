# -*- coding: utf-8 -*-
import concurrent.futures
from ydb.tests.sql.lib.test_base import TpchTestBaseH1
from ydb.tests.library.common import workload_manager
import ydb
import yatest.common
import os
import time


class TestWorkloadManager(TpchTestBaseH1):
    def get_pool(self, user, query):
        return workload_manager.get_pool(self, self.ydb_cli_path, self.get_endpoint(), self.get_database(), user, query)

    def test_crud(self):
        """
            Tests CRUD operations for workload manager resource pools and classifiers.
            Verifies creation of pools/classifiers and checks duplicate creation failures.
            Validates successful deletion of both classifier and resource pool.
        """

        pool_definition = """
                CREATE RESOURCE POOL test_pool WITH (
                CONCURRENT_QUERY_LIMIT=10,
                QUEUE_SIZE=1000,
                DATABASE_LOAD_CPU_THRESHOLD=80,
                RESOURCE_WEIGHT=100,
                QUERY_CPU_LIMIT_PERCENT_PER_NODE=50,
                TOTAL_CPU_LIMIT_PERCENT_PER_NODE=70
            ) """
        self.query(pool_definition)

        with pytest.raises(ydb.issues.GenericError):
            # create another pool with the same name
            self.query(pool_definition)

        pool_classifier_definition = """
            CREATE RESOURCE POOL CLASSIFIER test_classifier
            WITH (
                RESOURCE_POOL = 'test_pool',
                MEMBER_NAME = 'all-users@well-known'
            )"""
        self.query(pool_classifier_definition)

        with pytest.raises(ydb.issues.GenericError):
            # create another pool with the same name
            self.query(pool_classifier_definition)

        pool_definition = """
            DROP RESOURCE POOL CLASSIFIER test_classifier
            """
        self.query(pool_definition)

        pool_definition = """
            DROP RESOURCE POOL test_pool
            """
        self.query(pool_definition)

    def test_pool_classifier_with_init_timeout(self):
        """
            Verifies query execution in a specific pool based on classifier rules with initialization timeout.
            Creates resource pool with defined limits and assigns user to it via classifier.
            Tests query routing after allowing time for resource pool to fetch classifier list.
        """

        table_name = f"{self.tpch_default_path()}/lineitem"

        self.query(f"CREATE USER testuser PASSWORD NULL")
        self.query(f"GRANT ALL ON `{self.database}` TO testuser ")

        test_user_connection = self.create_connection("testuser")
        test_user_connection.query("select 1")

        resource_pool = "test_pool"
        pool_definition = f"""
        CREATE RESOURCE POOL {resource_pool} WITH (
            CONCURRENT_QUERY_LIMIT=1,
            QUEUE_SIZE=1,
            DATABASE_LOAD_CPU_THRESHOLD=80,
            RESOURCE_WEIGHT=100,
            QUERY_CPU_LIMIT_PERCENT_PER_NODE=50,
            TOTAL_CPU_LIMIT_PERCENT_PER_NODE=70
            )
        """
        self.query(pool_definition)

        pool_classifier_definition = f"""
            CREATE RESOURCE POOL CLASSIFIER test_classifier
            WITH (
                RESOURCE_POOL = '{resource_pool}',
                MEMBER_NAME = 'testuser'
            )"""
        self.query(pool_classifier_definition)

        # Wait until resource pool fetches resource classifiers list
        time.sleep(12)

        assert self.get_pool("testuser", f'select count(*) from `{table_name}`') == resource_pool

    def test_pool_classifier_without_init_timeout(self):
        """
            Verifies query execution in a specific pool based on classifier rules.
            Creates resource pool with defined limits and assigns user to it via classifier.
            Validates that query is correctly routed to the specified pool without initialization timeout.
        """

        table_name = f"{self.tpch_default_path()}/lineitem"

        self.query(f"CREATE USER testuser PASSWORD NULL")
        self.query(f"GRANT ALL ON `{self.database}` TO testuser ")

        test_user_connection = self.create_connection("testuser")
        test_user_connection.query("select 1")

        resource_pool = "test_pool"
        pool_definition = f"""
        CREATE RESOURCE POOL {resource_pool} WITH (
            CONCURRENT_QUERY_LIMIT=1,
            QUEUE_SIZE=1,
            DATABASE_LOAD_CPU_THRESHOLD=80,
            RESOURCE_WEIGHT=100,
            QUERY_CPU_LIMIT_PERCENT_PER_NODE=50,
            TOTAL_CPU_LIMIT_PERCENT_PER_NODE=70
            )
        """
        self.query(pool_definition)

        pool_classifier_definition = f"""
            CREATE RESOURCE POOL CLASSIFIER test_classifier
            WITH (
                RESOURCE_POOL = '{resource_pool}',
                MEMBER_NAME = 'testuser'
            )"""
        self.query(pool_classifier_definition)

        self.verify_pool("testuser", resource_pool, f'select count(*) from `{table_name}`')


    def test_pool_classifier_without_init_timeout(self):
        """
        Test queue size
        """

        table_name = f"{self.tpch_default_path()}/lineitem"

        self.query(f"CREATE USER testuser PASSWORD NULL")
        self.query(f"GRANT ALL ON `{self.database}` TO testuser ")

        test_user_connection = self.create_connection("testuser")
        test_user_connection.query("select 1")

        resource_pool = "test_pool"
        pool_definition = f"""
        CREATE RESOURCE POOL {resource_pool} WITH (
            CONCURRENT_QUERY_LIMIT=1,
            QUEUE_SIZE=1,
            DATABASE_LOAD_CPU_THRESHOLD=80,
            RESOURCE_WEIGHT=100,
            QUERY_CPU_LIMIT_PERCENT_PER_NODE=50,
            TOTAL_CPU_LIMIT_PERCENT_PER_NODE=70
            )
        """
        self.query(pool_definition)

        pool_classifier_definition = f"""
            CREATE RESOURCE POOL CLASSIFIER test_classifier
            WITH (
                RESOURCE_POOL = '{resource_pool}',
                MEMBER_NAME = 'testuser'
            )"""
        self.query(pool_classifier_definition)

        assert self.get_pool("testuser", f'select count(*) from `{table_name}`') == resource_pool

    def test_resource_pool_queue_size_limit(self):
        """
            Tests resource pool queue size limit functionality by creating a pool with specific limits.
            Executes multiple concurrent queries to verify proper queue management.
            Validates that successful queries match the configured queue size.
        """

        table_name = f"{self.tpch_default_path()}/lineitem"
        self.query(f"CREATE USER testuser PASSWORD NULL")
        self.query(f"GRANT ALL ON `{self.database}` TO testuser ")

        queue_size = 3

        resource_pool = "test_pool"
        pool_definition = f"""
        CREATE RESOURCE POOL `{resource_pool}` WITH (
            CONCURRENT_QUERY_LIMIT=1,
            QUEUE_SIZE={queue_size},
            DATABASE_LOAD_CPU_THRESHOLD=80,
            RESOURCE_WEIGHT=100,
            QUERY_CPU_LIMIT_PERCENT_PER_NODE=50,
            TOTAL_CPU_LIMIT_PERCENT_PER_NODE=70
            )
        """
        self.query(pool_definition)

        pool_classifier_definition = f"""
            CREATE RESOURCE POOL CLASSIFIER test_classifier
            WITH (
                RESOURCE_POOL = '{resource_pool}',
                MEMBER_NAME = 'testuser'
            )"""
        self.query(pool_classifier_definition)

        # Wait until resource pool fetches resource classifiers list
        time.sleep(12)

        self.verify_pool("testuser", resource_pool, f'select count(*) from `{table_name}`')

        test_user_connection = self.create_connection("testuser")

        success_count = 0
        error_count = 0
        num_threads = 3*queue_size+10

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            def execute_query(query):
                test_user_connection.query(query)

            query_futures = [executor.submit(execute_query,
                                             f"select sum(l_linenumber) from `{table_name}`") for _ in range(num_threads)]

            # Wait for the insert operation to complete, then allow reading to complete
            concurrent.futures.wait(query_futures)

            for future in query_futures:
                try:
                    future.result()
                    success_count += 1
                except Exception:
                    error_count += 1

        assert success_count == queue_size, f"Expected {queue_size} successful queries, got {success_count}"
        assert error_count == 0, f"Expected 0 failed queries, got {error_count}"


    def test_resource_pool_queue_resource_weight(self):
        """
        Test queue size limit for resource pool.
        """

        table_name = f"{self.tpch_default_path()}/lineitem"
        self.query(f"CREATE USER testuser1 PASSWORD NULL")
        self.query(f"GRANT ALL ON `{self.database}` TO testuser1 ")

        self.query(f"CREATE USER testuser2 PASSWORD NULL")
        self.query(f"GRANT ALL ON `{self.database}` TO testuser2 ")

        queue_size = 2

        resource_pool1 = "test_pool1"
        pool_definition = f"""
        CREATE RESOURCE POOL `{resource_pool1}` WITH (
            CONCURRENT_QUERY_LIMIT=1,
            QUEUE_SIZE={queue_size},
            DATABASE_LOAD_CPU_THRESHOLD=80,
            RESOURCE_WEIGHT=100,
            QUERY_CPU_LIMIT_PERCENT_PER_NODE=100,
            TOTAL_CPU_LIMIT_PERCENT_PER_NODE=70
            )
        """
        self.query(pool_definition)

        resource_pool2 = "test_pool2"
        pool_definition = f"""
        CREATE RESOURCE POOL `{resource_pool2}` WITH (
            CONCURRENT_QUERY_LIMIT=1,
            QUEUE_SIZE={queue_size},
            DATABASE_LOAD_CPU_THRESHOLD=80,
            RESOURCE_WEIGHT=0,
            QUERY_CPU_LIMIT_PERCENT_PER_NODE=40,
            TOTAL_CPU_LIMIT_PERCENT_PER_NODE=40
            )
        """
        self.query(pool_definition)

        pool_classifier1_definition = f"""
            CREATE RESOURCE POOL CLASSIFIER test_classifier1
            WITH (
                RESOURCE_POOL = '{resource_pool1}',
                MEMBER_NAME = 'testuser1'
            )"""
        self.query(pool_classifier1_definition)

        pool_classifier2_definition = f"""
            CREATE RESOURCE POOL CLASSIFIER test_classifier2
            WITH (
                RESOURCE_POOL = '{resource_pool2}',
                MEMBER_NAME = 'testuser2'
            )"""
        self.query(pool_classifier2_definition)

        # Wait until resource pool fetches resource classifiers list
        time.sleep(12)

        self.verify_pool("testuser1", resource_pool1, f'select count(*) from `{table_name}`')
        self.verify_pool("testuser2", resource_pool2, f'select count(*) from `{table_name}`')

        test_user1_connection = self.create_connection("testuser1")
        test_user2_connection = self.create_connection("testuser2")

        num_threads = 2
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            def execute_query(connection, query):
                start_time = time.time()
                for _ in range(3):
                    connection.query(query)
                end_time = time.time()
                return end_time - start_time

            query = f"select sum(l_linenumber) from `{table_name}`"
            query_testuser1_future = executor.submit(execute_query,test_user1_connection, query)
            query_testuser2_future = executor.submit(execute_query,test_user2_connection, query)

            # Wait for the insert operation to complete, then allow reading to complete
            concurrent.futures.wait([query_testuser1_future, query_testuser2_future])

            q1_time = query_testuser1_future.result()
            q2_time = query_testuser2_future.result()

            print(f"q1={q1_time} q2={q2_time}")

            assert q1_time < q2_time/2, "Low CPU user2 task works faster than high cpu user 1"
