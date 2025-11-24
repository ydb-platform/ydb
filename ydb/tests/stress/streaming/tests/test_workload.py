# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.stress.fixtures import StressFixture
from ydb.tests.library.harness.util import LogLevels


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            erasure=Erasure.MIRROR_3_DC,
            extra_feature_flags={
                "enable_external_data_sources": True,
                "enable_streaming_queries": True
            },
            additional_log_configs={
                'KQP_COMPUTE': LogLevels.TRACE,
                'STREAMS_CHECKPOINT_COORDINATOR': LogLevels.TRACE,
                'STREAMS_STORAGE_SERVICE': LogLevels.TRACE,
                'FQ_ROW_DISPATCHER': LogLevels.TRACE,
                'KQP_PROXY': LogLevels.DEBUG,
                'KQP_EXECUTOR': LogLevels.DEBUG},
        )

    def test(self):
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint",  f"localhost:{self.cluster.nodes[1].port}",
            "--database", self.database,
            "--duration", "60",
            "--partitions-count", "10"
        ]
        yatest.common.execute(cmd, wait=True)
