# -*- coding: utf-8 -*-
import logging
import os

import pytest
import yatest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.stress.fixtures import StressFixture

logger = logging.getLogger(__name__)


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            erasure=Erasure.MIRROR_3_DC,
            extra_feature_flags={
                "enable_external_data_sources": True,
            },
            query_service_config={
                "available_external_data_sources": ["Solomon"],
            },
            additional_log_configs={
                "KQP_COMPUTE": LogLevels.DEBUG,
                "KQP_PROXY": LogLevels.DEBUG,
                "KQP_EXECUTOR": LogLevels.DEBUG,
            },
            table_service_config={
                "dq_channel_version": 1,
            },
        )

    def test(self):
        logger.info("TestYdbWorkload::start test")
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", f"localhost:{self.cluster.nodes[1].port}",
            "--database", self.database,
            "--duration", self.base_duration,
            "--prefix", "federated_queries_stress",
        ]
        yatest.common.execute(cmd, wait=True)
