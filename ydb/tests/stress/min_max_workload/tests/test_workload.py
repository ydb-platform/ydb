# -*- coding: utf-8 -*-
import os
import pytest
import yatest
from ydb.tests.library.common.types import Erasure

from ydb.tests.library.stress.fixtures import StressFixture


class TestMinMaxWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            erasure=Erasure.NONE,
            extra_feature_flags={
                "enable_columnshard_bool": True,
                "enable_local_min_max_index": True,
            },
        )

    def test(self):
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", self.base_duration,
        ])
