import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestResultSetArrowWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        self.log_level = "INFO"
        self.format = "arrow"
        self.duration = 120
        self.channel_buffer_size = 32 * 1024

        yield from self.setup_cluster(
            extra_feature_flags={"enable_arrow_result_set_format": True},
            table_service_config={"resource_manager": {"channel_buffer_size": self.channel_buffer_size}},
        )

    def test(self):
        yatest.common.execute(
            [
                yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
                "--endpoint",
                self.endpoint,
                "--database",
                self.database,
                "--format",
                self.format,
                "--duration",
                str(self.duration),
                "--log-level",
                self.log_level,
            ]
        )
