import os
import pytest
import yatest

from ydb.tests.library.harness.util import LogLevels
from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbBackupWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_backup_service": True
            },
            additional_log_configs={
                "BACKUP_SERVICE": LogLevels.DEBUG,
            },
        )

    def test(self):
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", self.base_duration,
            "--backup-interval", "20",
        ]
        yatest.common.execute(cmd, wait=True)
