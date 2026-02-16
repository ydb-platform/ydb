# -*- coding: utf-8 -*-
import pytest
import yatest.common
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import Erasure, KikimrConfigGenerator
from ydb.tests.library.harness.param_constants import kikimr_driver_path

from ydb.tests.oss.ydb_sdk_import import ydb


class StressFixture:
    @pytest.fixture(autouse=True)
    def base_setup(self):
        self.all_binary_paths = [kikimr_driver_path()]
        self.base_erasure = yatest.common.get_param('stress_default_erasure', default='NONE')
        self.base_erasure = Erasure.from_string(self.base_erasure)

        self.base_duration = yatest.common.get_param('stress_default_duration', default='60')

    def setup_cluster(self, erasure=None, **kwargs):
        erasure = erasure or self.base_erasure
        self.config = KikimrConfigGenerator(
            binary_paths=self.all_binary_paths,
            erasure=erasure,
            **kwargs,
        )

        self.cluster = KiKiMR(self.config)
        self.cluster.start()
        self.database = "/Root"
        self.endpoint = "grpc://%s:%s" % ('localhost', self.cluster.nodes[1].port)
        self.mon_endpoint = f"http://localhost:{self.cluster.nodes[1].mon_port}"

        self.driver = ydb.Driver(
            ydb.DriverConfig(
                database=self.database,
                endpoint=self.endpoint
            )
        )
        self.driver.wait(timeout=60)
        yield
        self.driver.stop()
        self.cluster.stop()
