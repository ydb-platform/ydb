import logging
import unittest
from contextlib import ExitStack

import yatest.common

from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.snapshot_retention import (
    SnapshotRetentionMonitor,
    configure_retention_hive,
    pinned_driver,
    retention_cluster_config,
)
from ydb.tests.stress.snapshot_retention.workload import (
    HOLD_SECONDS,
    SnapshotRetentionWorkload,
)


logger = logging.getLogger(__name__)


class TestSnapshotRetentionWorkload(unittest.TestCase):
    read_kinds = ("point", "range", "index")

    def test_concurrent_writes_and_retained_reads(self):
        self._start_workload()
        readers = 4
        duration = float(yatest.common.get_param("stress_default_duration", default="60"))
        stats = self.workload.run(
            duration, HOLD_SECONDS, readers, seed=57975, before_read=self._wait_past_snapshot,
        )
        self.assertGreater(stats.writes, 1)
        self.monitor.wait_registry_empty()

    def test_tablet_restarts_and_retained_reads(self):
        self._start_workload()
        with self.workload.background_writer(retry_uncertain=True) as writer:
            with ExitStack() as stack:
                first = self._hold_snapshots(stack, writer)
                second = self._hold_snapshots(stack, writer)
                self._age_snapshots(first + second, writer)

                self._restart_target()
                self._place_target_tablets()
                self._check_snapshots(first, writer, phase="after first restart")

                # The second group has not accessed target, including its index.
                self._restart_target()
                self._place_target_tablets()
                self._check_snapshots(second, writer, phase="first target reads after second restart")
                self._check_snapshots(first, writer, phase="repeated reads after second restart")
                self._release_snapshots(first + second)
        self._check_writer_result(writer)
        self.monitor.wait_registry_empty()

    def test_split_merge_and_retained_reads(self):
        self._start_workload(split_merge=True)
        with self.workload.background_writer(retry_uncertain=True) as writer:
            with ExitStack() as stack:
                first = self._hold_snapshots(stack, writer)
                second = self._hold_snapshots(stack, writer)
                self._age_snapshots(first + second, writer)

                tablets = self.monitor.wait_tablets(self.workload.target_path)
                self.assertEqual(len(tablets), 1)
                split = self.monitor.split(self.workload.target_path, tablets[0], boundary=10)
                self.assertEqual(len(split), 2)
                self._place_target_tablets()
                self._check_snapshots(first, writer, phase="after split")

                # Keep the second group unread through both history transfers.
                tablets = self.monitor.wait_tablets(self.workload.target_path)
                self.assertEqual(len(tablets), 2)
                merged = self.monitor.merge(self.workload.target_path, tablets)
                self.assertEqual(len(merged), 1)
                self._place_target_tablets()
                self._check_snapshots(second, writer, phase="first target reads after merge")
                self._check_snapshots(first, writer, phase="repeated reads after merge")
                self._release_snapshots(first + second)
        self._check_writer_result(writer)
        self.monitor.wait_registry_empty()

    def _start_workload(self, *, split_merge=False):
        config = retention_cluster_config(
            # The default write-sequence protocol can fail during a split with
            # "Ancestor shard ... not found in lock" before retained reads run.
            # Isolate that unrelated protocol in this retention scenario; the
            # normal and restart cases keep the default protocol enabled.
            disabled_feature_flags=(
                ["enable_data_shard_uncommitted_write_seq_num"] if split_merge else []
            ),
        )
        self.cluster = KiKiMR(config)
        self.addCleanup(self.cluster.stop)
        self.cluster.start()
        self.driver = pinned_driver(self.cluster)
        self.addCleanup(self.driver.stop)
        configure_retention_hive(self.cluster)
        self.monitor = SnapshotRetentionMonitor(self.cluster, self.driver, "/Root")
        self.addCleanup(self.monitor.close)
        self.workload = SnapshotRetentionWorkload(self.driver, "/Root")
        self.addCleanup(self.workload.close)
        self.workload.create_tables()
        self.workload.write_generation(0)
        self._place_target_tablets()

    def _place_target_tablets(self):
        # Split/merge may put destination tablets on another node. Keep both
        # the main table and index remote from the session/retention owner.
        node_id = self.cluster.nodes[2].node_id
        for path in (self.workload.target_path, self.workload.index_path):
            for tablet in self.monitor.wait_tablets(path):
                self.monitor.move_tablet(tablet, node_id)
                self.assertEqual(self.monitor.tablet_node(tablet), node_id)

    def _wait_past_snapshot(self, held, writer=None):
        self.assertEqual(held.owner_node_id, self.cluster.nodes[1].node_id)
        advance = writer.check if writer is not None else None
        # Capture once per phase and share it across the main table and index.
        bound = self.monitor.snapshot_bound(held, advance=advance)
        for path in (self.workload.target_path, self.workload.index_path):
            self.monitor.wait_low_watermark(path, bound, advance=advance)

    def _hold_snapshots(self, stack, writer):
        snapshots = []
        for kind in self.read_kinds:
            writer.check()
            held = stack.enter_context(self.workload.begin_snapshot())
            self.assertEqual(held.owner_node_id, self.cluster.nodes[1].node_id)
            snapshots.append((kind, held))
            self.monitor._wait(
                f"writer advances beyond held generation {held.generation}",
                lambda: writer.generation > held.generation,
                advance=writer.check,
            )
        return snapshots

    def _age_snapshots(self, snapshots, writer):
        for _, held in snapshots:
            self._wait_past_snapshot(held, writer)

    def _restart_target(self):
        # The KQP owner remains alive while only the remote DataShard restarts.
        tablets = self.monitor.wait_tablets(self.workload.target_path)
        self.assertEqual(len(tablets), 1)
        self.assertEqual(self.monitor.tablet_node(tablets[0]), self.cluster.nodes[2].node_id)
        self.monitor.restart_tablet(tablets[0])

    def _check_snapshots(self, snapshots, writer, *, phase):
        for kind, held in snapshots:
            logger.info("%s: %s read in transaction %s, generation %s", phase, kind, held.tx_id, held.generation)
            self._wait_past_snapshot(held, writer)
            writer.check()
            held.assert_matches(kind=kind)

    @staticmethod
    def _release_snapshots(snapshots):
        modes = ("rollback", "commit", "session")
        for index, (_, held) in enumerate(snapshots):
            held.close(modes[index % len(modes)])

    def _check_writer_result(self, writer):
        writer.check()
        self.assertGreater(writer.writes, 1)
        self.assertGreater(writer.generation, writer.initial_generation)
        self.workload.assert_current(writer.generation)
        self.workload.assert_current(writer.generation, kind="index")
