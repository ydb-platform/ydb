# -*- coding: utf-8 -*-
import logging
import os
import pytest
import yatest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_port_allocator import KikimrPortManagerPortAllocator
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.stress.fixtures import StressFixture

logger = logging.getLogger(__name__)


@pytest.fixture
def enable_watermarks(request):
    return getattr(request, 'param', True)


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self, enable_watermarks: bool):
        erasure = None if enable_watermarks else Erasure.MIRROR_3_DC  # TODO: Erasure.MIRROR_3_DC
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
                'KQP_COMPUTE': LogLevels.TRACE,
                'STREAMS_CHECKPOINT_COORDINATOR': LogLevels.TRACE,
                'STREAMS_STORAGE_SERVICE': LogLevels.TRACE,
                'FQ_ROW_DISPATCHER': LogLevels.TRACE,
                'KQP_PROXY': LogLevels.DEBUG,
                'KQP_EXECUTOR': LogLevels.DEBUG,
            },
            table_service_config={
                "enable_watermarks": enable_watermarks,
            },
        )

    @pytest.mark.parametrize('enable_watermarks', [True, False], indirect=True)
    def test(self, enable_watermarks: bool):
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", f"localhost:{self.cluster.nodes[1].port}",
            "--database", self.database,
            "--duration", "60",
            "--partitions-count", "10",
            "--prefix", "streaming_stress",
            "--enable_watermarks", str(enable_watermarks).lower(),
        ]
        yatest.common.execute(cmd, wait=True)
