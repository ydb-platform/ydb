# -*- coding: utf-8 -*-
import concurrent.futures
from ydb.tests.sql.lib.test_base import TpchTestBaseH1
from ydb.tests.library.common import workload_manager
from ydb.tests.library.test_meta import link_test_case
import ydb
import time
import pytest


class TestWorkloadManager(TpchTestBaseH1):
    def get_pool(self, user, query):
        return workload_manager.get_pool(self, self.ydb_cli_path, self.get_endpoint(), self.get_database(), user, query)

    def test_crud(self):
        """
            Tests CRUD operations for workload manager resource pools and classifiers.
            Verifies creation of pools/classifiers and checks duplicate creation failures.
            Validates successful deletion of both classifier and resource pool.
        """

        pool_name = f"test_pool{self.hash_short}"
        pool_definition = f"""
                CREATE RESOURCE POOL {pool_name} WITH (
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

        classifier_name = f"test_classifier{self.hash_short}"
        pool_classifier_definition = f"""
            CREATE RESOURCE POOL CLASSIFIER {classifier_name}
            WITH (
                RESOURCE_POOL = 'test_pool',
                MEMBER_NAME = 'all-users@well-known'
            )"""
        self.query(pool_classifier_definition)

        with pytest.raises(ydb.issues.GenericError):
            # create another pool with the same name
            self.query(pool_classifier_definition)

        pool_definition = f"""
            DROP RESOURCE POOL CLASSIFIER {classifier_name}
            """
        self.query(pool_definition)

        pool_definition = f"""
            DROP RESOURCE POOL {pool_name}
            """
        self.query(pool_definition)

    @link_test_case("#14602")
    @pytest.mark.parametrize("wait_for_timeout", [(False), (True)])
    def test_pool_classifier_init(self, wait_for_timeout):
        """
            Verifies query execution in a specific pool based on classifier rules with initialization timeout.
            Creates resource pool with defined limits and assigns user to it via classifier.
            Tests query routing after allowing time for resource pool to fetch classifier list.
        """

        table_name = f"{self.tpch_default_path()}/lineitem"

        user_name = "testuser" + self.hash_short
        self.query(f"CREATE USER {user_name} PASSWORD NULL")
        self.query(f"GRANT ALL ON `{self.database}` TO {user_name} ")

        test_user_connection = self.create_connection(user_name)
        test_user_connection.query("select 1")

        resource_pool = "test_pool"+self.hash_short
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
            CREATE RESOURCE POOL CLASSIFIER test_classifier{self.hash_short}
            WITH (
                RESOURCE_POOL = '{resource_pool}',
                MEMBER_NAME = '{user_name}'
            )"""
        self.query(pool_classifier_definition)

        if wait_for_timeout:
            # Wait until resource pool fetches resource classifiers list
            time.sleep(12)

        assert self.get_pool(user_name, f'select count(*) from `{table_name}`') == resource_pool

    def test_resource_pool_queue_size_limit(self):
        """
            Tests resource pool queue size limit functionality by creating a pool with specific limits.
            Executes multiple concurrent queries to verify proper queue management.
            Validates that successful queries match the configured queue size.
        """

        table_name = f"{self.tpch_default_path()}/lineitem"
        testuser_username = 'testuser' + self.hash_short
        self.query(f"CREATE USER {testuser_username} PASSWORD NULL")
        self.query(f"GRANT ALL ON `{self.database}` TO {testuser_username}")

        queue_size = 3

        resource_pool = "test_pool" + self.hash_short
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
            CREATE RESOURCE POOL CLASSIFIER test_classifier{self.hash_short}
            WITH (
                RESOURCE_POOL = '{resource_pool}',
                MEMBER_NAME = '{testuser_username}'
            )"""
        self.query(pool_classifier_definition)

        # Wait until resource pool fetches resource classifiers list
        time.sleep(12)

        assert self.get_pool(testuser_username, f'select count(*) from `{table_name}`') == resource_pool

        test_user_connection = self.create_connection(testuser_username)

        success_count = 0
        error_count = 0
        num_threads = 3*queue_size+10

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            def execute_query(query):
                test_user_connection.query(query, retry_settings=ydb.RetrySettings(max_retries=0))

            query_futures = [executor.submit(execute_query, f"select sum(l_linenumber) from `{table_name}`")
                             for _ in range(num_threads)]

            # Wait for the insert operation to complete, then allow reading to complete
            concurrent.futures.wait(query_futures)

            for future in query_futures:
                try:
                    future.result()
                    success_count += 1
                except Exception:
                    error_count += 1

        # CONCURRENT_QUERY_LIMIT (1) queries are executing, 2 in queue
        assert success_count == queue_size + 1, f"Expected {queue_size+1} successful queries, got {success_count}"

    @pytest.mark.parametrize("run_count,use_classifiers", [(1, False), (1, True)])
    def test_resource_pool_queue_resource_weight(self, run_count, use_classifiers):
        """
        Tests resource pool performance based on different resource weights and priorities.
        Creates two pools with different resource weights and CPU limits for high and low priority users.
        If resource pool classifiers are set, verifies that high priority pool executes queries significantly faster than low priority pool.
        """

        table_name = f"{self.tpch_default_path()}/lineitem"
        high_priority_user = "userhighpriority" + self.hash_short
        self.query(f"CREATE USER {high_priority_user} PASSWORD NULL")
        self.query(f"GRANT ALL ON `{self.database}` TO {high_priority_user} ")

        low_priority_user = "userlowpriority" + self.hash_short
        self.query(f"CREATE USER {low_priority_user} PASSWORD NULL")
        self.query(f"GRANT ALL ON `{self.database}` TO {low_priority_user} ")

        queue_size = 2

        high_resource_pool = "high_resource_pool" + self.hash_short
        pool_definition = f"""
        CREATE RESOURCE POOL `{high_resource_pool}` WITH (
            CONCURRENT_QUERY_LIMIT=1,
            QUEUE_SIZE={queue_size},
            DATABASE_LOAD_CPU_THRESHOLD=80,
            RESOURCE_WEIGHT=100,
            QUERY_CPU_LIMIT_PERCENT_PER_NODE=100,
            TOTAL_CPU_LIMIT_PERCENT_PER_NODE=100
            )
        """
        self.query(pool_definition)

        low_resource_pool = "low_resource_pool" + self.hash_short
        pool_definition = f"""
        CREATE RESOURCE POOL `{low_resource_pool}` WITH (
            CONCURRENT_QUERY_LIMIT=1,
            QUEUE_SIZE={queue_size},
            DATABASE_LOAD_CPU_THRESHOLD=80,
            RESOURCE_WEIGHT=0,
            QUERY_CPU_LIMIT_PERCENT_PER_NODE=100,
            TOTAL_CPU_LIMIT_PERCENT_PER_NODE=30
            )
        """
        self.query(pool_definition)

        if use_classifiers:
            pool_classifier1_definition = f"""
                CREATE RESOURCE POOL CLASSIFIER test_classifier1{self.hash_short}
                WITH (
                    RESOURCE_POOL = '{high_resource_pool}',
                    MEMBER_NAME = '{high_priority_user}'
                )"""
            self.query(pool_classifier1_definition)

            pool_classifier2_definition = f"""
                CREATE RESOURCE POOL CLASSIFIER test_classifier2{self.hash_short}
                WITH (
                    RESOURCE_POOL = '{low_resource_pool}',
                    MEMBER_NAME = '{low_priority_user}'
                )"""
            self.query(pool_classifier2_definition)

            # Wait until resource pool fetches resource classifiers list
            time.sleep(12)

            assert self.get_pool(high_priority_user, f'select count(*) from `{table_name}`') == high_resource_pool
            assert self.get_pool(low_priority_user, f'select count(*) from `{table_name}`') == low_resource_pool

        highpriority_user_connection = self.create_connection(high_priority_user)
        lowpriority_user_connection = self.create_connection(low_priority_user)

        num_threads = 2
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            def execute_query(connection, query):
                start_time = time.time()
                for _ in range(run_count):
                    connection.query(query)
                end_time = time.time()
                return end_time - start_time

            query = f"select sum(l_linenumber) from `{table_name}`"
            highpriorityuser_future = executor.submit(execute_query, highpriority_user_connection, query)
            lowpriorityuser_future = executor.submit(execute_query, lowpriority_user_connection, query)

            # Wait for the insert operation to complete, then allow reading to complete
            concurrent.futures.wait([highpriorityuser_future, lowpriorityuser_future])

            highpriority_time = highpriorityuser_future.result()
            lowpriority_time = lowpriorityuser_future.result()

            assert highpriority_time < lowpriority_time/2, "Low CPU user2 task works faster than high cpu user 1"
