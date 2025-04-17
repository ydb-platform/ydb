import ydb
import os
import yatest.common
import logging
import hashlib


from typing import Any, List, Optional
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels

logger = logging.getLogger(__name__)


class MulticlusterTestBase():
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get(
            "YDB_DRIVER_BINARY", "ydb/apps/ydbd/ydbd"))
        logger.error(yatest.common.execute(
            [ydb_path, "-V"], wait=True).stdout.decode("utf-8"))

        cls.ydb_cli_path = yatest.common.build_path("ydb/apps/ydb/ydb")

        cls.database = "/Root"
        cls.clusters = [cls.build_cluster(), cls.build_cluster()]

    @classmethod
    def build_cluster(self):
        cluster = KiKiMR(KikimrConfigGenerator(erasure=self.get_cluster_configuration(),
                                               extra_feature_flags=["enable_resource_pools",
                                                                    "enable_external_data_sources",
                                                                    "enable_tiering_in_column_shard"],
                                               column_shard_config={
            'disabled_on_scheme_shard': False,
            'lag_for_compaction_before_tierings_ms': 0,
            'compaction_actualization_lag_ms': 0,
            'optimizer_freshness_check_duration_ms': 0,
            'small_portion_detect_size_limit': 0,
        },
            additional_log_configs={
            'TX_TIERING': LogLevels.DEBUG}))
        cluster.start()
        driver = ydb.Driver(
            ydb.DriverConfig(
                database=self.get_database(),
                endpoint=self.get_endpoint(cluster)
            )
        )
        driver.wait()
        pool = ydb.QuerySessionPool(driver)
        return {
            "pool": pool,
            "driver": driver,
            "cluster": cluster
        }

    @classmethod
    def get_cluster_configuration(self):
        return Erasure.NONE

    @classmethod
    def get_database(self):
        return self.database

    @classmethod
    def get_endpoint(self, cluster):
        return "%s:%s" % (
            cluster.nodes[1].host, cluster.nodes[1].port
        )

    @classmethod
    def teardown_class(cls):
        for cluster in cls.clusters:
            for element in cluster.values():
                element.stop()

    def setup_method(self):
        current_test_full_name = os.environ.get("PYTEST_CURRENT_TEST")
        self.table_path = "insert_table_" + \
            current_test_full_name.replace("::", ".").removesuffix(" (setup)")
        self.hash = hashlib.md5(self.table_path.encode()).hexdigest()
        self.hash_short = self.hash[:8]

    def query(self, text, pool, driver,
              tx: ydb.QueryTxContext | None = None,
              stats: bool | None = None,
              parameters: Optional[dict] = None,
              retry_settings=None) -> List[Any]:
        results = []
        if tx is None:
            if not stats:
                result_sets = pool.execute_with_retries(
                    text, parameters=parameters, retry_settings=retry_settings)
                for result_set in result_sets:
                    results.extend(result_set.rows)
            else:
                settings = ydb.ScanQuerySettings()
                settings = settings.with_collect_stats(
                    ydb.QueryStatsCollectionMode.FULL)
                for response in driver.table_client.scan_query(text,
                                                               settings=settings,
                                                               parameters=parameters,
                                                               retry_settings=retry_settings):
                    last_response = response
                    for row in response.result_set.rows:
                        results.append(row)

                return (results, last_response.query_stats)
        else:
            with tx.execute(text) as result_sets:
                for result_set in result_sets:
                    results.extend(result_set.rows)

        return results

    def create_query(pool, driver):
        def query(self, text,
                  tx: ydb.QueryTxContext | None = None,
                  stats: bool | None = None,
                  parameters: Optional[dict] = None,
                  retry_settings=None) -> List[Any]:
            results = []
            if tx is None:
                if not stats:
                    result_sets = pool.execute_with_retries(
                        text, parameters=parameters, retry_settings=retry_settings)
                    for result_set in result_sets:
                        results.extend(result_set.rows)
                else:
                    settings = ydb.ScanQuerySettings()
                    settings = settings.with_collect_stats(
                        ydb.QueryStatsCollectionMode.FULL)
                    for response in driver.table_client.scan_query(text,
                                                                   settings=settings,
                                                                   parameters=parameters,
                                                                   retry_settings=retry_settings):
                        last_response = response
                        for row in response.result_set.rows:
                            results.append(row)

                    return (results, last_response.query_stats)
            else:
                with tx.execute(text) as result_sets:
                    for result_set in result_sets:
                        results.extend(result_set.rows)

            return results
        return query
