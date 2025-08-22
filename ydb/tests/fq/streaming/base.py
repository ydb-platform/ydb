import os
import logging

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.olap.common.ydb_client import YdbClient

import yatest.common

logger = logging.getLogger(__name__)


class StreamingImportTestBase(object):
    @classmethod
    def setup_class(cls):
        cls._setup_ydb()

    @classmethod
    def teardown_class(cls):
        cls.ydb_client.stop()
        cls.cluster.stop()

    @classmethod
    def _get_ydb_config(cls):
        config = KikimrConfigGenerator(
            extra_feature_flags={
                "enable_external_data_sources": True,
                "enable_move_column_table": True
            },
            query_service_config={"available_external_data_sources": ["Ydb"]},
            table_service_config={}
        )
        config.yaml_config["query_service_config"] = {}
        config.yaml_config["query_service_config"]["available_external_data_sources"] = ["ObjectStorage", "Ydb"]
        config.yaml_config["log_config"]["default_level"] = 7

        return config

    @classmethod
    def _setup_ydb(cls):
        ydb_path = yatest.common.build_path(os.environ.get("YDB_DRIVER_BINARY"))
        logger.info(yatest.common.execute([ydb_path, "-V"], wait=True).stdout.decode("utf-8"))

        config = cls._get_ydb_config()
        cls.cluster = KiKiMR(config)
        cls.cluster.start()

        node = cls.cluster.nodes[1]
        cls.ydb_client = YdbClient(
            database=f"/{config.domain_name}",
            endpoint=f"grpc://{node.host}:{node.port}",
            ydb_cli_path=yatest.common.build_path(os.environ.get("YDB_CLI_BINARY"))
        )
        cls.ydb_client.wait_connection()
