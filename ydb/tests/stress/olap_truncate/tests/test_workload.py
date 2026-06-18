# -*- coding: utf-8 -*-
import os
import pytest
import yatest
from ydb.tests.library.common.types import Erasure

from ydb.tests.library.stress.fixtures import StressFixture


class TestOlapTruncateWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            erasure=Erasure.NONE,
            extra_feature_flags=[
                "enable_truncate_table",
                "enable_truncate_column_table",
            ],
            column_shard_config={
                "generate_internal_path_id": True
            }
        )

    def test(self):
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", self.base_duration,
        ])
