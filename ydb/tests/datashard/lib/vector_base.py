import os
import yatest.common
import logging
import ydb

from ydb.tests.sql.lib.test_base import TestBase
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.util import LogLevels

logger = logging.getLogger(__name__)


class VectorBase(TestBase):
    @classmethod
    def setup_class(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY", "ydb/apps/ydbd/ydbd"))
        logger.error(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))

        cls.ydb_cli_path = yatest.common.build_path("ydb/apps/ydb/ydb")

        cls.database = "/Root"
        cls.cluster = KiKiMR(
            KikimrConfigGenerator(
                erasure=cls.get_cluster_configuration(),
                extra_feature_flags=["enable_vector_index"],
                additional_log_configs={'TX_TIERING': LogLevels.DEBUG},
            )
        )
        cls.cluster.start()
        cls.driver = ydb.Driver(ydb.DriverConfig(database=cls.get_database(), endpoint=cls.get_endpoint()))
        cls.driver.wait()
        cls.pool = ydb.QuerySessionPool(cls.driver)
