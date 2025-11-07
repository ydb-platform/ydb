# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbKvWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    @pytest.mark.parametrize("store_type", ["row", "column"])
    def test(self, store_type):
        oom = []
        while True:
            oom.append("x" * 1024 * 1024 * 100)
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", "10",
            "--store_type", store_type,
            "--kv_prefix", store_type,
        ])
