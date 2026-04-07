# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbKvVolumeWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    @pytest.mark.parametrize(
        "config",
        [
            "ydb/tests/stress/kv_volume/tests/configs/common_channel_read.textproto",
            "ydb/tests/stress/kv_volume/tests/configs/inline_channel_read.textproto",
            "ydb/tests/stress/kv_volume/tests/configs/write_read_delete.textproto",
        ],
    )
    @pytest.mark.parametrize("version", ["v1", "v2"])
    def test(self, config, version):
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", "1",
            "--in-flight", "1",
            "--version", version,
            "--config", yatest.common.source_path(config),
        ])
