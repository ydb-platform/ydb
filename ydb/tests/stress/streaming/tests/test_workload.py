# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_port_allocator import KikimrPortManagerPortAllocator
from ydb.tests.library.stress.fixtures import StressFixture
from ydb.tests.library.harness.util import LogLevels
import logging

logger = logging.getLogger(__name__)


@pytest.fixture
def enable_watermarks(request):
    return getattr(request, 'param', True)


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self, enable_watermarks: bool):
        erasure = Erasure.MIRROR_3_DC
        port_allocator = KikimrPortManagerPortAllocator()
        query_service_config = {
            "streaming_queries": {
                "external_storage": {
                    "database_connection": {
                        "endpoint": f"localhost:{port_allocator.get_node_port_allocator(1).grpc_port}",
                        "database": "/Root",
                    },
                },
            },
        } if enable_watermarks else None

        yield from self.setup_cluster(
            erasure=erasure,
            extra_feature_flags={
                "enable_external_data_sources": True,
                "enable_streaming_queries": True,
            },
            port_allocator=port_allocator,
            query_service_config=query_service_config,
            additional_log_configs={
                'KQP_COMPUTE': LogLevels.DEBUG,
                'STREAMS_CHECKPOINT_COORDINATOR': LogLevels.DEBUG,
                'STREAMS_STORAGE_SERVICE': LogLevels.DEBUG,
                'FQ_ROW_DISPATCHER': LogLevels.DEBUG,
                'KQP_PROXY': LogLevels.DEBUG,
                'KQP_EXECUTOR': LogLevels.DEBUG,
            },
            table_service_config={
                "enable_watermarks": enable_watermarks,
                "dq_channel_version": 1,
            },
        )

    @pytest.mark.parametrize('enable_watermarks', [True, False], indirect=True)
    def test(self, enable_watermarks: bool):
        logger.info("TestYdbWorkload::start test")
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", f"localhost:{self.cluster.nodes[1].port}",
            "--database", self.database,
            "--duration", self.base_duration,
            "--partitions-count", "10",
            "--prefix", "streaming_stress",
            "--enable_watermarks", str(enable_watermarks).lower(),
        ]
        yatest.common.execute(cmd, wait=True)
