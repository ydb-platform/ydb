import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestKvVolumeTool(StressFixture):
    VOLUME_PATH = "kv_volume_tool_stress"

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def _tool(self):
        return yatest.common.binary_path(os.environ["YDB_KV_VOLUME_TOOL_PATH"])

    def _base_args(self):
        return [
            "-e", self.endpoint,
            "-d", self.database,
            "-p", self.VOLUME_PATH,
        ]

    def _run(self, command, *args):
        yatest.common.execute([self._tool(), command, *self._base_args(), *args])

    def _create_volume(self):
        self._run(
            "create",
            "--partition-count", "4",
            "--channel-media", "hdd",
            "--channel-media", "hdd",
            "--channel-media", "hdd",
        )

    def test_load(self):
        self._create_volume()
        self._run(
            "load",
            "--partition-count", "4",
            "--seconds", "2",
            "--threads", "2",
            "--value-size", "1024",
            "--read-percent", "50",
            "--delete-percent", "10",
            "--report-period", "1",
        )
        self._run("describe")
        self._run("remove")

    def test_write_read(self):
        self._create_volume()
        self._run(
            "write",
            "--partition-id", "0",
            "--key", "stress_key",
            "--value", "stress_value",
        )
        self._run(
            "read",
            "--partition-id", "0",
            "--key", "stress_key",
        )
        self._run("remove")
