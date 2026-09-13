# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture
from ydb.tests.stress.topic_reset_offset.workload import MEGABYTE


class TestYdbTopicResetOffsetWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags=[
                "enable_topic_write_offset_delta_in_keys",
                "enable_topic_messages_batching",
            ]
        )

    def test(self):
        limit_memory = os.environ.get("YDB_STRESS_TEST_LIMIT_MEMORY", "0").lower() in ("true", "1", "y", "yes")
        duration = yatest.common.get_param("stress_default_duration", default="30")
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", duration,
            "--partitions", "10",
            "--messages-per-partition", "1000",
            "--large-message-bytes", str(10 * MEGABYTE),
            "--writers", "1",
            "--consumers", "2",
            "--readers-per-consumer", "1",
        ]
        if limit_memory:
            cmd = [
                yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
                "--endpoint", self.endpoint,
                "--database", self.database,
                "--duration", duration,
                "--partitions", "2",
                "--messages-per-partition", "100",
                "--large-message-bytes", str(256 * 1024),
                "--writers", "0",
                "--consumers", "1",
                "--readers-per-consumer", "1",
            ]
        yatest.common.execute(cmd)
