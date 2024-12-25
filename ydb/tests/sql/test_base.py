import ydb
import os

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from typing import Callable, Any, List


class TestBase:

    @classmethod
    def setup_class(cls):
        cls.database = "/Root"
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=Erasure.NONE))
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database=cls.database,
                endpoint="%s:%s" % (
                    cls.cluster.nodes[1].host, cls.cluster.nodes[1].port
                )
            )
        )
        cls.driver.wait()
        cls.pool = ydb.QuerySessionPool(cls.driver)

    @classmethod
    def teardown_class(cls):
        cls.pool.stop()
        cls.driver.stop()
        cls.cluster.stop()

    def setup_method(self):
        current_test_full_name = os.environ.get("PYTEST_CURRENT_TEST")
        self.table_path = "insert_table_" + current_test_full_name.replace("::", ".").removesuffix(" (setup)")
        print(self.table_path)

    def query(self, text, tx: None|ydb.QueryTxContext = None):
        results = []
        if tx is None:
            result_sets = self.pool.execute_with_retries(text)
            for result_set in result_sets:
                results.extend(result_set.rows)
        else:
            with tx.execute(text) as result_sets:
                for result_set in result_sets:
                    results.extend(result_set.rows)

        return results

    def transactional(self, fn: Callable[[ydb.QuerySession], List[Any]] ):
        return self.pool.retry_operation_sync(lambda session: fn(session))

    # class Transactional(object):
    #     def __init__(self, test_base):
    #         self.test_base = test_base

    #     def __enter__(self):
    #         return self

    #     def __exit__(self):

