# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbTopicWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags=[
                "enable_topic_compactification_by_key",
            ],
        )

    def test(self):
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", "120",
            "--consumers", "2",
            "--consumer-threads", "2",
            "--restart-interval", "15s",
            "--partitions", "4",
            "--write-workload", "0.01", "9000000", "2", "big_record", "1",
            "--write-workload", "8000", "45",   "1000", "small_record", "10",
            "--write-workload", "800", "4096",       "1", "medium_record", "10",
            # MESSAGE_RATE:float MESSAGE_SIZE:int KEYS_COUNT:int KEY_PREFIX:str PRODUCERS:int
        ])
