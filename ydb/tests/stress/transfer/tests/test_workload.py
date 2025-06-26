# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture

class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            column_shard_config={
                "allow_nullable_columns_in_pk": True,
            }
        )

    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test(self, store_type):
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", f'grpc://localhost:{self.cluster.nodes[1].grpc_port}',
            "--database", "/Root",
            "--duration", "60",
            "--mode", store_type
        ]
        yatest.common.execute(cmd, wait=True)
