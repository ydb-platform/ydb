# -*- coding: utf-8 -*-
from ydb.tests.oss.ydb_sdk_import import ydb

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


QUERY_CACHE_SIZE = 10
QUERY_COUNT = 20


def execute_some_queries(pool):
    for i in range(QUERY_COUNT):
        with pool.checkout() as session:
            query_result = session.transaction().execute(
                ydb.DataQuery("declare $param as Uint64;\nselect %d + $param as result;" % i, {"$param": ydb.PrimitiveType.Uint64}),
                {"$param": i},
                commit_tx=True,
            )

            assert query_result[0].rows[0].result == i + i


class TestQueryCache(object):
    @classmethod
    def setup_class(cls):
        cls.config = KikimrConfigGenerator(use_in_memory_pdisks=True)
        cls.config.yaml_config["table_service_config"] = {"compile_query_cache_size": QUERY_CACHE_SIZE}
        cls.cluster = kikimr_cluster_factory(configurator=cls.config)
        cls.cluster.start()
        cls.discovery_endpoint = "%s:%s" % (cls.cluster.nodes[1].hostname, cls.cluster.nodes[1].grpc_port)
        cls.driver = ydb.Driver(endpoint=cls.discovery_endpoint, database="/Root", credentials=ydb.AnonymousCredentials())
        cls.driver.wait(timeout=5)
        cls.pool = ydb.SessionPool(cls.driver)

    def test(self):
        execute_some_queries(self.pool)

    @classmethod
    def teardown_class(cls):
        cls.pool.stop()
        cls.driver.stop()
        cls.cluster.stop()
