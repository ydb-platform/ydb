# -*- coding: utf-8 -*-
import os
import pytest
import yatest
import library.python.port_manager
from ydb.tests.library.common.types import Erasure

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbTestShardWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            erasure=Erasure.MIRROR_3_DC,
            extra_grpc_services=['test_shard'],
            default_users={},
            enforce_user_token_requirement=False,
        )

    def test(self):
        port_manager = library.python.port_manager.PortManager()
        tsserver_port = str(port_manager.get_port())
        mon_port = str(self.cluster.nodes[1].mon_port)

        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--path", "testshard_workload",
            "--duration", self.base_duration,
            "--monitoring-port", mon_port,
            "--tsserver-port", tsserver_port,
        ])
