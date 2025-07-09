import os
import logging

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.olap.common.s3_client import S3Mock, S3Client
from ydb.tests.olap.common.ydb_client import YdbClient

import yatest.common

logger = logging.getLogger(__name__)


class S3ImportTestBase(object):
    @classmethod
    def setup_class(cls):
        cls._setup_ydb()
        cls._setup_s3()

    @classmethod
    def teardown_class(cls):
        cls.s3_mock.stop()
        cls.ydb_client.stop()
        cls.cluster.stop()

    @classmethod
    def _get_ydb_config(cls):
        config = KikimrConfigGenerator(
            extra_feature_flags={"enable_external_data_sources": True}
        )
        config.yaml_config["query_service_config"] = {}
        config.yaml_config["query_service_config"]["available_external_data_sources"] = ["ObjectStorage"]
        config.yaml_config["table_service_config"]["resource_manager"] = {"query_memory_limit": 64424509440}
        config.yaml_config["resource_broker_config"] = {}
        config.yaml_config["resource_broker_config"]["queues"] = [{"limit": {"memory": 64424509440}, "weight": 30, "name": "queue_kqp_resource_manager"}]
        config.yaml_config["resource_broker_config"]["resource_limit"] = {"memory": 64424509440}
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

    @classmethod
    def _setup_s3(cls):
        cls.s3_mock = S3Mock(yatest.common.binary_path(os.environ["MOTO_SERVER_PATH"]))
        cls.s3_mock.start()
        cls.s3_client = S3Client(cls.s3_mock.endpoint)
