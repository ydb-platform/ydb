import os
import yatest.common
import logging
import ydb

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels

logger = logging.getLogger(__name__)


class VectoreBase(TestBase):
    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get(
            "YDB_DRIVER_BINARY", "ydb/apps/ydbd/ydbd"))
        logger.error(yatest.common.execute(
            [ydb_path, "-V"], wait=True).stdout.decode("utf-8"))

        cls.ydb_cli_path = yatest.common.build_path("ydb/apps/ydb/ydb")

        cls.database = "/Root"
        cls.cluster = KiKiMR(KikimrConfigGenerator(erasure=cls.get_cluster_configuration(),
                                                   extra_feature_flags=["enable_resource_pools",
                                                                        "enable_external_data_sources",
                                                                        "enable_tiering_in_column_shard",
                                                                        "enable_vector_index"
                                                                        ],
                                                   column_shard_config={
                                                       'disabled_on_scheme_shard': False,
                                                       'lag_for_compaction_before_tierings_ms': 0,
                                                       'compaction_actualization_lag_ms': 0,
                                                       'optimizer_freshness_check_duration_ms': 0,
                                                       'small_portion_detect_size_limit': 0,
        },
            additional_log_configs={
                                                       'TX_TIERING': LogLevels.DEBUG}))
        cls.cluster.start()
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database=cls.get_database(),
                endpoint=cls.get_endpoint()
            )
        )
        cls.driver.wait()
        cls.pool = ydb.QuerySessionPool(cls.driver)
