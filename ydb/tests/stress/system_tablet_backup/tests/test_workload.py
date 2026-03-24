# -*- coding: utf-8 -*-
import os
import yatest
import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.stress.fixtures import StressFixture


class TestSystemTabletBackup(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self, request):
        self._backup_path = yatest.common.output_path(
            f"system_tablet_backup_{request.node.name}")

        yield from self.setup_cluster(
            erasure=Erasure.MIRROR_3_DC,
            additional_log_configs={
                "LOCAL_DB_BACKUP": LogLevels.TRACE,
                "NODE_BROKER": LogLevels.TRACE,
                "BOOTSTRAPPER": LogLevels.TRACE,
            },
            system_tablet_backup_config={
                "filesystem": {
                    "path": self._backup_path,
                },
            },
        )

    def _run_workload(self):
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", f"grpc://localhost:{self.cluster.nodes[1].grpc_port}",
            "--mon-endpoint", f"http://localhost:{self.cluster.nodes[1].mon_port}",
            "--database", self.database,
            "--duration", self.base_duration,
            "--backup-path", self._backup_path,
        ]
        yatest.common.execute(cmd, wait=True)

    def test_workload(self):
        self._run_workload()

    def test_workload_with_corrupted_backup(self):
        tablet_id = 72057594037936129  # NODE_BROKER
        corrupted_backup_dir = os.path.join(
            self._backup_path, "node_broker", str(tablet_id),
            "backup_corrupted", "snapshot")
        os.makedirs(corrupted_backup_dir)

        with pytest.raises(yatest.common.process.ExecutionError):
            self._run_workload()
