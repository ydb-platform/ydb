# -*- coding: utf-8 -*-
import pytest
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.param_constants import kikimr_driver_path

from ydb.tests.library.common.types import Erasure
from ydb.tests.oss.ydb_sdk_import import ydb


class StressFixture:
    @pytest.fixture(autouse=True)
    def base_setup(self):

        self.all_binary_paths = [kikimr_driver_path()]
        pass

    def setup_cluster(self, **kwargs):
        self.config = KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            binary_paths=self.all_binary_paths,
            **kwargs,
        )

        self.cluster = KiKiMR(self.config)
        self.cluster.start()
        self.endpoint = "grpc://%s:%s" % ('localhost', self.cluster.nodes[1].port)

        self.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint=self.endpoint
            )
        )
        self.driver.wait()
        yield
        self.cluster.stop()
