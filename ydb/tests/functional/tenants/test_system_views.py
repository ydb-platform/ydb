# -*- coding: utf-8 -*-
import logging
import os
import time

from hamcrest import (
    anything,
    assert_that,
    greater_than,
    has_length,
    has_properties,
)

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


class BaseSystemViews(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory(
            KikimrConfigGenerator(
                additional_log_configs={
                    'SYSTEM_VIEWS': LogLevels.DEBUG
                }
            )
        )
        cls.cluster.config.yaml_config['feature_flags']['enable_column_statistics'] = False
        cls.cluster.start()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def setup_method(self, method=None):
        self.database = "/Root/users/{class_name}_{method_name}".format(
            class_name=self.__class__.__name__,
            method_name=method.__name__,
        )
        logger.debug("Create database %s" % self.database)
        self.cluster.create_database(
            self.database,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        self.cluster.register_and_start_slots(self.database, count=1)
        self.cluster.wait_tenant_up(self.database)

    def teardown_method(self, method=None):
        logger.debug("Remove database %s" % self.database)
        self.cluster.remove_database(self.database)
        self.database = None

    def create_table(self, driver, table):
        with ydb.SessionPool(driver, size=1) as pool:
            with pool.checkout() as session:
                session.execute_scheme(
                    "create table `{}` (key Int32, value String, primary key(key));".format(
                        table
                    )
                )

    def read_partition_stats(self, driver, database):
        table = os.path.join(database, '.sys/partition_stats')
        return self._stream_query_result(
            driver,
            "select PathId, PartIdx, Path from `{}`;".format(table)
        )

    def read_query_metrics(self, driver, database):
        table = os.path.join(database, '.sys/query_metrics_one_minute')
        return self._stream_query_result(
            driver,
            "select Count, SumReadBytes, SumRequestUnits from `{}`;".format(table)
        )

    def read_query_stats(self, driver, database, table_name):
        table = os.path.join(database, '.sys', table_name)
        return self._stream_query_result(
            driver,
            "select Duration, ReadBytes, CPUTime, RequestUnits from `{}`;".format(table)
        )

    def _stream_query_result(self, driver, query):
        it = driver.table_client.scan_query(query)
        result = []

        while True:
            try:
                response = next(it)
            except StopIteration:
                break

            for row in response.result_set.rows:
                result.append(row)

        return result

    def check_query_metrics_and_stats(self, query_count):
        table = os.path.join(self.database, 'table')

        driver_config = ydb.DriverConfig(
            "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
            self.database
        )

        with ydb.Driver(driver_config) as driver:
            driver.wait(timeout=10)

            self.create_table(driver, table)

            with ydb.SessionPool(driver, size=1) as pool:
                with pool.checkout() as session:
                    for i in range(query_count):
                        query = "select * from `{}` -- {}".format(table, i)
                        session.transaction().execute(query, commit_tx=True)

            time.sleep(70)

            for i in range(60):
                metrics = self.read_query_metrics(driver, self.database)
                if len(metrics) == query_count:
                    break
                time.sleep(5)

            assert_that(metrics, has_length(query_count))
            assert_that(metrics[0], has_properties({
                'Count': 1,
                'SumReadBytes': 0,
                'SumRequestUnits': greater_than(0),
            }))

            for table_name in [
                'top_queries_by_duration_one_minute',
                'top_queries_by_duration_one_hour',
                'top_queries_by_read_bytes_one_minute',
                'top_queries_by_read_bytes_one_hour',
                'top_queries_by_cpu_time_one_minute',
                'top_queries_by_cpu_time_one_hour',
                'top_queries_by_request_units_one_minute',
                'top_queries_by_request_units_one_hour'
            ]:
                stats = self.read_query_stats(driver, self.database, table_name)

                assert_that(stats, has_length(min(query_count, 5)))
                assert_that(stats[0], has_properties({
                    'Duration': anything(),
                    'ReadBytes': 0,
                    'CPUTime': anything(),
                    'RequestUnits': greater_than(0)
                }))


class TestPartitionStats(BaseSystemViews):
    def test_case(self):
        for database in ('/Root', self.database):
            table = os.path.join(database, 'table')

            driver_config = ydb.DriverConfig(
                "%s:%s" % (self.cluster.nodes[1].host, self.cluster.nodes[1].port),
                database
            )

            with ydb.Driver(driver_config) as driver:
                driver.wait(timeout=10)

                self.create_table(driver, table)
                stats = self.read_partition_stats(driver, database)

                assert_that(stats, has_length(1))
                assert_that(stats[0], has_properties({
                    'Path': table,
                    'PathId': anything(),
                    'PartIdx': 0,
                }))


class TestQueryMetrics(BaseSystemViews):
    def test_case(self):
        self.check_query_metrics_and_stats(1)


class TestQueryMetricsUniqueQueries(BaseSystemViews):
    def test_case(self):
        self.check_query_metrics_and_stats(10)
