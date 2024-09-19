import os

from ydb.tests.oss.ydb_sdk_import import ydb


class TestKqprunRecipe(object):
    def test_query_execution(self):
        with ydb.Driver(
            endpoint=os.getenv("KQPRUN_ENDPOINT"),
            database="/Root"
        ) as driver:
            driver.wait(timeout=5, fail_fast=True)

            with ydb.QuerySessionPool(driver) as pool:
                result_sets = pool.execute_with_retries("SELECT * FROM test_table")
                rows = result_sets[0].rows
                assert len(rows) == 1
                assert rows[0].Key == 42
