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
                "enable_topic_transfer": True,
            }
        )

    @pytest.mark.parametrize("store_type, topic", [
        ("row", "local"),
        ("row", "remote"),
        ("column", "local"),
        ("column", "remote")
    ])
    def test(self, store_type, topic):
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", "60",
            "--mode", store_type,
            "--topic", topic
        ]
        yatest.common.execute(cmd, wait=True)
