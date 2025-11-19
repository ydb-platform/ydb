# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            erasure=Erasure.MIRROR_3_DC,
            extra_feature_flags={
                "enable_external_data_sources": True,
                "enable_streaming_queries": True
            }
        )

    def test(self):
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint",  f"localhost:{self.cluster.nodes[1].port}",
            "--database", self.database,
            "--duration", "30"   # TODO 60
        ]
        yatest.common.execute(cmd, wait=True)
