# -*- coding: utf-8 -*-
import os
import pytest
import yatest
from ydb.tests.library.common.types import Erasure

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbTestShardWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            erasure=Erasure.MIRROR_3_DC,
            extra_grpc_services=['test_shard'],
        )

    def test(self):
        pool_name = list(self.cluster.default_channel_bindings.values())[0]
        channels = f"{pool_name},{pool_name},{pool_name}"

        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", "120",
            "--channels", channels,
        ])
