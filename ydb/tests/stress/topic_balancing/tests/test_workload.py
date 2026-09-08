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


class TestTopicBalancingAutoPartitioningWorkload(StressFixture):
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
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--auto-partitioning",
            "--duration", self._duration(),
            "--min-partitions", "7",
            "--wait-partitions", "40",
            "--max-partitions", "80",
            "--max-sessions", "400",
            "--writers", "16",
            "--threads", "8",
            *extra_args,
        ])

    def _sdk_args(self, sdk):
        if sdk == "old_sdk":
            return ["--no-auto-partitioning-support"]
        return []

    @pytest.mark.parametrize("sdk", ["scale_aware_sdk", "old_sdk"])
    def test_read(self, sdk):
        # Finish-only: ScaleAware unlocks children on Finish; old SDK needs
        # from-end or the delay heuristic.
        self._run([
            "--path", f"topic_balancing_read_{sdk}",
            *self._sdk_args(sdk),
            "--min-sessions", "32",
            "--max-lag-ms", "10000",
        ])

    @pytest.mark.parametrize("sdk", ["scale_aware_sdk", "old_sdk"])
    def test_commit_reread(self, sdk):
        # Commit received data and rewind assigned partitions via CommitOffset.
        self._run([
            "--path", f"topic_balancing_commit_reread_{sdk}",
            *self._sdk_args(sdk),
            "--commit-data",
            "--rewind-rps", "2",
            "--rewind-target", "assigned",
            "--min-sessions", "32",
            "--max-lag-ms", "15000",
        ])


class TestTopicBalancingAutoPartitioningPreferredWorkload(TestTopicBalancingAutoPartitioningWorkload):
    """Same auto-partitioning churn, but every second read session lists 1-5 random partitions."""

    def _run(self, extra_args):
        super()._run(["--preferred-sessions", *extra_args])

    @pytest.mark.parametrize("sdk", ["scale_aware_sdk", "old_sdk"])
    def test_read(self, sdk):
        self._run([
            "--path", f"topic_balancing_preferred_read_{sdk}",
            *self._sdk_args(sdk),
            "--min-sessions", "32",
            "--max-lag-ms", "10000",
        ])

    @pytest.mark.parametrize("sdk", ["scale_aware_sdk", "old_sdk"])
    def test_commit_reread(self, sdk):
        self._run([
            "--path", f"topic_balancing_preferred_commit_reread_{sdk}",
            *self._sdk_args(sdk),
            "--commit-data",
            "--rewind-rps", "2",
            "--rewind-target", "assigned",
            "--min-sessions", "32",
            "--max-lag-ms", "15000",
        ])
