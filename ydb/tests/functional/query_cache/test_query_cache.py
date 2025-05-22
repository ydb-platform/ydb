# -*- coding: utf-8 -*-
from ydb.tests.oss.ydb_sdk_import import ydb

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


QUERY_CACHE_SIZE = 10
QUERY_COUNT = 20


def execute_some_queries(pool):
    for i in range(QUERY_COUNT):
        with pool.checkout() as session:
            with session.transaction().execute(
                f"declare $param as Uint64;select {i} + $param as result;",
                {"$param": ydb.TypedValue(value=i, value_type=ydb.PrimitiveType.Uint64)},
                commit_tx=True,
            ) as result_sets:
                for result_set in result_sets:
                    for row in result_set.rows:
                        assert row.result == i + i


class TestQueryCache(object):
    @classmethod
    def setup_class(cls):
        cls.config = KikimrConfigGenerator(use_in_memory_pdisks=True)
        cls.config.yaml_config["table_service_config"] = {"compile_query_cache_size": QUERY_CACHE_SIZE}
        cls.cluster = KiKiMR(configurator=cls.config)
        cls.cluster.start()
        cls.discovery_endpoint = "%s:%s" % (cls.cluster.nodes[1].host, cls.cluster.nodes[1].grpc_port)
        cls.driver = ydb.Driver(endpoint=cls.discovery_endpoint, database="/Root", credentials=ydb.AnonymousCredentials())
        cls.driver.wait(timeout=5)
        cls.pool = ydb.QuerySessionPool(cls.driver)

    def test(self):
        execute_some_queries(self.pool)

    @classmethod
    def teardown_class(cls):
        cls.pool.stop()
        cls.driver.stop()
        cls.cluster.stop()
