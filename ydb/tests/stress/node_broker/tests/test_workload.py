# -*- coding: utf-8 -*-
import os
import yatest
import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels


from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            erasure=Erasure.MIRROR_3_DC,
            additional_log_configs={
                "NODE_BROKER": LogLevels.TRACE,
                "NAMESERVICE": LogLevels.TRACE,
            },
        )

    def test(self):
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", self.endpoint,
            "--mon-endpoint", self.mon_endpoint,
            "--database", self.database,
            "--duration", "120",
        ]
        yatest.common.execute(cmd, wait=True)


class TestDeltaProtocol(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            erasure=Erasure.MIRROR_3_DC,
            additional_log_configs={
                "NODE_BROKER": LogLevels.TRACE,
                "NAMESERVICE": LogLevels.TRACE,
            },
            extra_feature_flags={
                "enable_node_broker_delta_protocol": True
            }
        )

    def test(self):
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", f"grpc://localhost:{self.cluster.nodes[1].grpc_port}",
            "--mon-endpoint", f"http://localhost:{self.cluster.nodes[1].mon_port}",
            "--database", "/Root",
            "--duration", "120",
        ]
        yatest.common.execute(cmd, wait=True)
