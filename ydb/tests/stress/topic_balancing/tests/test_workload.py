# -*- coding: utf-8 -*-
import os
import pytest
import yatest

from ydb.tests.library.stress.fixtures import StressFixture


class TestTopicBalancingWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            pq_config={
                'quoting_config': {
                    'enable_quoting': True,
                },
            },
        )

    def test(self):
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--path", "topic_balancing",
            "--duration", self.base_duration,
            "--partitions", "1024",
            "--max-sessions", "2048",
            "--threads", "8",
            "--max-lag-ms", "5000",
        ])


class TestTopicBalancingAutopartWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            extra_feature_flags=[
                "enable_topic_partition_split_based_on_kll_sketch",
            ],
            pq_config={
                'quoting_config': {
                    'enable_quoting': True,
                },
            },
        )

    def _duration(self):
        return str(max(int(self.base_duration), 240))

    def _run(self, extra_args):
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_AUTOPART_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", self._duration(),
            "--min-partitions", "7",
            "--wait-partitions", "40",
            "--max-partitions", "80",
            "--max-sessions", "400",
            "--writers", "16",
            "--threads", "8",
            *extra_args,
        ])

    def test_read(self):
        self._run([
            "--path", "topic_balancing_autopart_read",
            "--min-sessions", "32",
            "--max-lag-ms", "10000",
        ])

    def test_commit_roots(self):
        self._run([
            "--path", "topic_balancing_autopart_commit",
            "--commit-data",
            "--rewind-rps", "2",
            "--rewind-target", "assigned",
            "--min-sessions", "32",
            "--max-lag-ms", "15000",
        ])
