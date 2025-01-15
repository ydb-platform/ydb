# -*- coding: utf-8 -*-
import pytest
from ydb.tests.sql.lib.test_base import TpchTestBaseH1
from ydb.tests.sql.lib.test_wm import WorkloadManager
import ydb
import yatest.common
import os
from time import sleep
import threading
import queue


class TestWorkloadManager(TpchTestBaseH1, WorkloadManager):

    def test_crud(self):
        """
        Test  verifies CRUD operations for workload manager
        """

        pool_definition = f"""
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

        pool_classifier_definition = f"""
            CREATE RESOURCE POOL CLASSIFIER test_classifier
            WITH (
                RESOURCE_POOL = 'test_pool',
                MEMBER_NAME = 'all-users@well-known'
            )"""
        self.query(pool_classifier_definition)

        with pytest.raises(ydb.issues.GenericError):
            # create another pool with the same name
            self.query(pool_classifier_definition)

        pool_definition = f"""
            DROP RESOURCE POOL CLASSIFIER test_classifier
            """
        self.query(pool_definition)

        pool_definition = f"""
            DROP RESOURCE POOL test_pool
            """
        self.query(pool_definition)


    def test_pool_classifier_with_init_timeout(self):
        """
        Tests if simple query executes in specific pool by classifier rule.
        Wait timeout after classifier creation to let Resource pool fetch classifier list
        """

        table_name = f"{self.tpch_default_path()}/lineitem"

        user_definition = f"""
        CREATE USER testuser PASSWORD NULL
        """
        self.query(user_definition)
        self.query(f"GRANT ALL ON `/Root` TO testuser ")
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
        sleep(12)

        self.verify_pool("testuser", resource_pool, f'select count(*) from `{table_name}`')


    def test_pool_classifier_without_init_timeout(self):
        """
        Tests if simple query executes in specific pool by classifier rule.
        Do not wait timeout after classifier creation to let Resource pool fetch classifier list
        """

        table_name = f"{self.tpch_default_path()}/lineitem"

        user_definition = f"""
        CREATE USER testuser PASSWORD NULL
        """
        self.query(user_definition)
        self.query(f"GRANT ALL ON `/Root` TO testuser ")
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

        user_definition = f"""
        CREATE USER testuser PASSWORD NULL
        """
        self.query(user_definition)
        self.query(f"GRANT ALL ON `/Root` TO testuser ")
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


    def test_resource_pool_queue_size_limit(self):
        """
        Test queue size limit for resource pool.
        """
        print("test_resource_pool_queue_size_limit")

        table_name = f"{self.tpch_default_path()}/lineitem"
        user_definition = f"""
        CREATE USER testuser PASSWORD NULL
        """
        self.query(user_definition)
        self.query(f"GRANT ALL ON `/Root` TO testuser ")
        test_user_connection = self.create_connection("testuser")
        test_user_connection.query("select 1")

        print("before creating pool")

        queue_size = 2

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
        sleep(12)

        self.verify_pool("testuser", resource_pool, f'select count(*) from `{table_name}`')

        def execute_query(query_queue, result_queue):
            while not query_queue.empty():
                try:
                    query_text = query_queue.get()
                    # Execute a query
                    test_user_connection.query(query_text)
                    result_queue.put("success", block=False)
                    print("success")
                except Exception as e:
                    result_queue.put(f"error: {str(e)}", block=False)
                    print("error")
                finally:
                    query_queue.task_done()

        query_queue = queue.Queue()
        result_queue = queue.Queue()

        for _ in range(queue_size+20):
            query_queue.put(f"select count(*) from `{table_name}`", block=False)

        print("before")
        threads = []
        for _ in range(queue_size+20):
            print(f"sending command {_}")
            t = threading.Thread(target=execute_query, args=(query_queue, result_queue))
            threads.append(t)
            t.start()

        sleep(1)
        print("waiting for the queue")
        # Wait for all queries to complete
        query_queue.join()

        for t in threads:
            t.join()

        # Collect results
        success_count = 0
        error_count = 0
        while not result_queue.empty():
            result = result_queue.get()
            if "success" in result:
                success_count += 1
            else:
                error_count += 1

        assert success_count == queue_size, f"Expected {queue_size} successful queries, got {success_count}"
        assert error_count == 0, f"Expected 0 failed queries, got {error_count}"
