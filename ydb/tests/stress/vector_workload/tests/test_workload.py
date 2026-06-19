# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbVectorWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test(self):
        mode = yatest.common.get_param('vector_mode', default='standalone')
        data_dir = yatest.common.get_param('vector_data_dir', default=None)

        cmd = [
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", self.base_duration,
            "--mode", mode,
        ]
        if data_dir:
            cmd.extend(["--data-dir", data_dir])

        yatest.common.execute(cmd)
