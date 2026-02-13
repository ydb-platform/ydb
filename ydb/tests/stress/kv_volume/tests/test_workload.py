# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbKvVolumeWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    @pytest.mark.parametrize("config_name", ["common_channel_read", "inline_channel_read", "write_read_delete"])
    def test(self, config_name):
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", "1",
            "--in-flight", "1",
            "--version", "v1",
            "--config-name", config_name,
        ])
