# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbTopicSetOffsetsWorkload(StressFixture):
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
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_TEST_PATH")),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", self.base_duration,
        ]
        if limit_memory:
            cmd.extend(["--writers", "2", "--consumers", "2", "--readers-per-consumer", "1"])
        yatest.common.execute(cmd)
