# -*- coding: utf-8 -*-
import pytest
from ydb.tests.sql.lib.test_base import TpchTestBaseH1
import ydb
import yatest.common
import os


class TestWorkloadManager(TpchTestBaseH1):

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

    def test_queue_size(self):
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


        pool_definition = f"""
        CREATE RESOURCE POOL test_pool WITH (
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
                RESOURCE_POOL = 'test_pool',
                MEMBER_NAME = 'testuser'
            )"""
        self.query(pool_classifier_definition)

        result = self.run_cli(["--user", "testuser", '--no-password',"discovery", "whoami"]).stdout.decode("ascii")
        result = self.run_cli(["--user", "testuser", '--no-password', "sql", "-s", f'select count(*) from `{table_name}`', '--stats', 'full', '--format', 'json-unicode']).stdout.decode("utf-8")

        # result = self.run_cli(["sql", "-s", 'select 1', '--stats', 'full', '--format', 'json-unicode']).stdout.decode("ascii")
        # result = self.run_cli(['workload', 'tpch', '-p', '/', 'init', '--store=column', '--datetime']).stdout.decode("ascii")
        assert False, result
        # result = self.run_cli(["--user", "testuser",  "discovery", "whoami"]).stdout.decode("ascii")
        result = self.run_cli(["discovery", "whoami"]).stdout.decode("ascii")
        # assert False, result

        for _ in range(1,200):
            result = test_user_connection.query(f"select sum(l_orderkey) from `{table_name}`", stats=True)
            # print(result)
            # assert False, result

