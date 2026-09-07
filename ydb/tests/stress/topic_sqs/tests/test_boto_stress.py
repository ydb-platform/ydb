# -*- coding: utf-8 -*-
import pytest
import yatest.common

from ydb.tests.library.fixtures import ydb_database_ctx
from ydb.tests.library.harness.kikimr_config import Erasure, KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.param_constants import kikimr_driver_path
from ydb.tests.library.stress.fixtures import StressFixture
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.stress.topic_sqs.workload.boto_stress import (
    BotoStressWorkload,
    DEFAULT_DURATION_SECONDS,
    DEFAULT_WORKERS,
)


class TestSqsTopicBotoStress(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        erasure = Erasure.from_string(yatest.common.get_param('stress_default_erasure', default='NONE'))
        self.config = KikimrConfigGenerator(
            binary_paths=[kikimr_driver_path()],
            erasure=erasure,
            extra_feature_flags={
                "enable_topic_message_level_parallelism": True,
            },
            http_proxy_config={
                "enabled": True,
                "sqs_topic_enabled": True,
                "ymq_enabled": False,
                "yandex_cloud_service_region": ["ru-central1", "ru-test"],
            },
        )
        self.cluster = KiKiMR(self.config)
        self.cluster.start()
        with ydb_database_ctx(self.cluster, "/Root/SqsTopic", node_count=1) as db_path:
            self.database = db_path
            self.endpoint = "grpc://%s:%s" % ("localhost", self.cluster.nodes[1].port)
            self.http_proxy_endpoint = f"http://localhost:{self.cluster.nodes[1].http_proxy_port}"
            self.driver = ydb.Driver(ydb.DriverConfig(self.endpoint, self.database))
            self.driver.wait(timeout=60)
            yield
            self.driver.stop()
        self.cluster.stop()

    def test_boto_write_read_commit_stress(self):
        with BotoStressWorkload(
            endpoint=self.endpoint,
            database=self.database,
            duration=DEFAULT_DURATION_SECONDS,
            sqs_endpoint=self.http_proxy_endpoint + self.database,
            workers=DEFAULT_WORKERS,
        ) as workload:
            workload.run()
