# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.stress.fixtures import StressFixture
from ydb.tests.library.harness.util import LogLevels
import logging

logger = logging.getLogger(__name__)


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            erasure=Erasure.MIRROR_3_DC,
            extra_feature_flags={
                "enable_external_data_sources": True,
                "enable_streaming_queries": True,
            },
            additional_log_configs={
                'KQP_COMPUTE': LogLevels.DEBUG,
                'STREAMS_CHECKPOINT_COORDINATOR': LogLevels.DEBUG,
                'STREAMS_STORAGE_SERVICE': LogLevels.DEBUG,
                'FQ_ROW_DISPATCHER': LogLevels.DEBUG,
                'KQP_PROXY': LogLevels.DEBUG,
                'KQP_EXECUTOR': LogLevels.DEBUG,
            },
            table_service_config={
                "enable_watermarks": True,
                "dq_channel_version": 1,
            },
        )

    def test(self):
        logger.info("TestYdbWorkload::start test")
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint",  f"localhost:{self.cluster.nodes[1].port}",
            "--database", self.database,
            "--duration", self.base_duration,
            "--partitions-count", "10",
            "--prefix", "streaming_stress"
        ]
        yatest.common.execute(cmd, wait=True)
