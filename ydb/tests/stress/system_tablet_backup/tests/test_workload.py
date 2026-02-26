# -*- coding: utf-8 -*-
import os
import yatest
import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.stress.fixtures import StressFixture


class TestSystemTabletBackup(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            erasure=Erasure.MIRROR_3_DC,
            additional_log_configs={
                "LOCAL_DB_BACKUP": LogLevels.TRACE,
                "NODE_BROKER": LogLevels.TRACE,
                "BOOTSTRAPPER": LogLevels.TRACE,
            },
            system_tablet_backup_config={
                "filesystem": {
                    "path": yatest.common.output_path("system_tablet_backup"),
                },
            },
        )

    def test_workload(self):
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", f"grpc://localhost:{self.cluster.nodes[1].grpc_port}",
            "--mon-endpoint", f"http://localhost:{self.cluster.nodes[1].mon_port}",
            "--database", self.database,
            "--duration", self.base_duration,
            "--backup-path", yatest.common.output_path("system_tablet_backup"),
        ]
        yatest.common.execute(cmd, wait=True)
