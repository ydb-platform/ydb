import unittest
import uuid
from urllib.parse import parse_qs, urlparse

from ydb.tests.library.clients.kikimr_http_client import HiveClient
from ydb.tests.library.harness.kikimr_config import Erasure, KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.snapshot_retention import SnapshotRetentionMonitor
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.stress.snapshot_retention.workload import (
    KEEP_SNAPSHOT_TIMEOUT_MS,
    SnapshotRetentionWorkload,
    expected_rows,
)


class RetentionClusterTest(unittest.TestCase):
    data_shard_locking = True
    snapshots_locking = True

    @classmethod
    def setUpClass(cls):
        flags = ["enable_forced_compactions"]
        disabled = []
        for name, enabled in (
            ("enable_snapshots_locking", cls.snapshots_locking),
            ("enable_data_shard_snapshot_locking", cls.data_shard_locking),
        ):
            if enabled is not None:
                (flags if enabled else disabled).append(name)
        config = KikimrConfigGenerator(
            erasure=Erasure.NONE,
            nodes=3,
            use_in_memory_pdisks=False,
            extra_feature_flags=flags,
            disabled_feature_flags=disabled,
            table_service_config={
                "session_balancer_settings": {"enable_tier": "TIER_DISABLED"},
            },
            datashard_config={
                "keep_snapshot_timeout": KEEP_SNAPSHOT_TIMEOUT_MS,
                "cleanup_snapshot_period": 200,
                "stats_report_interval_seconds": 1,
                "idle_mem_compaction_interval_seconds": 1,
            },
        )
        config.yaml_config["long_tx_service_config"] = {
            "local_snapshot_promotion_time_seconds": 1,
            "max_clock_skew_ms": 1000,
            "snapshots_exchange_interval_seconds": 1,
            "snapshots_registry_update_interval_seconds": 1,
            "prefill_timeout_seconds": 1,
        }
        cls.cluster = KiKiMR(config)
        cls.addClassCleanup(cls.cluster.stop)
        cls.cluster.start()
        # Keep the KQP session on node 1 while its target DataShard is on node 2.
        cls.driver = ydb.Driver(ydb.DriverConfig(
            endpoint=cls.cluster.nodes[1].endpoint,
            database="/Root",
            disable_discovery=True,
        ))
        cls.addClassCleanup(cls.driver.stop)
        cls.driver.wait(timeout=60)
        HiveClient("localhost", cls.cluster.nodes[1].mon_port).set_min_scatter_to_balance(101)

    def setUp(self):
        self.workload = SnapshotRetentionWorkload(self.driver, "/Root", f"retention_{uuid.uuid4().hex[:12]}")
        self.addCleanup(self.workload.close)
        self.workload.create_tables()
        self.workload.write_generation(0)
        self.monitor = SnapshotRetentionMonitor(self.cluster, self.driver, "/Root")
        self._place_target_on_remote_node()

    def _place_target_on_remote_node(self):
        for path in (self.workload.target_path, self.workload.index_path):
            for tablet in self.monitor.wait_tablets(path):
                self.monitor.move_tablet(tablet, self.cluster.nodes[2].node_id)
                self.assertEqual(self.monitor.tablet_node(tablet), self.cluster.nodes[2].node_id)

    def _hold(self):
        snapshot = self.workload.begin_snapshot()
        self.addCleanup(snapshot.close, "session")
        owner = int(parse_qs(urlparse(snapshot.session_id).query)["node_id"][0])
        self.assertEqual(owner, self.cluster.nodes[1].node_id)
        return snapshot

    def _past(self, snapshot, generation=1, path=None, extra_advance=None, refresh_bound=False):
        def advance():
            # Index maintenance skips equal rows. Commit real changes, then
            # restore the deterministic current generation before observing it.
            self.workload.write_generation(generation + 1)
            self.workload.write_generation(generation)
            if extra_advance is not None:
                extra_advance()

        self.monitor.wait_past_snapshot(
            snapshot,
            path or self.workload.target_path,
            advance=advance,
            refresh_bound=refresh_bound,
        )

    def _compact(self):
        self.workload.execute_scheme(
            f"ALTER TABLE `{self.workload.target_path}` COMPACT WITH (CASCADE = true, PARALLEL = 2);"
        )


class TestSnapshotRetention(RetentionClusterTest):
    def _first_read(self, kind):
        snapshot = self._hold()
        self.assertEqual(snapshot.generation, 0)
        self.workload.write_generation(1)
        self._past(snapshot)
        if kind == "index":
            self._past(snapshot, path=self.workload.index_path)
        self._compact()
        snapshot.assert_matches(kind=kind)
        self.workload.assert_current(1)
        self.assertGreater(self.monitor.remote_snapshot_count(self.cluster.nodes[2].node_id), 0)
        snapshot.close()
        self.monitor.wait_registry_empty()

    def test_first_range_read_below_watermark(self):
        self._first_read("range")

    def test_first_point_read_below_watermark(self):
        self._first_read("point")

    def test_first_index_read_below_watermark(self):
        self._first_read("index")

    def test_independent_snapshots_and_release(self):
        snapshots = []
        for generation in range(3):
            self.workload.write_generation(generation)
            snapshot = self._hold()
            self.assertEqual(snapshot.generation, generation)
            snapshots.append(snapshot)
        self.workload.write_generation(3)
        for snapshot in snapshots:
            self._past(snapshot, generation=3)
            snapshot.assert_matches()
        registry_nodes = [self.cluster.nodes[2].node_id]
        # No SQL is in flight while polling: only these three held
        # transactions contribute promoted snapshots from the pinned owner.
        self.monitor.wait_registry_count(3, node_ids=registry_nodes)

        # Releasing one point must not release the other transactions' points.
        snapshots[1].close("rollback")
        self.monitor.wait_registry_count(2, node_ids=registry_nodes)
        # The previous bounds have already been crossed. Capture a new write
        # edge after release propagation and require cleanup on both tables.
        self._past(snapshots[0], generation=3, refresh_bound=True)
        self._past(snapshots[0], generation=3, path=self.workload.index_path)
        self._compact()
        for snapshot in (snapshots[0], snapshots[2]):
            snapshot.assert_matches()
            snapshot.assert_matches(kind="index")
        snapshots[0].close("commit")
        self.monitor.wait_registry_count(1, node_ids=registry_nodes)
        self._past(snapshots[2], generation=3, refresh_bound=True)
        self._past(snapshots[2], generation=3, path=self.workload.index_path)
        self._compact()
        snapshots[2].assert_matches()
        snapshots[2].assert_matches(kind="index")
        snapshots[2].close("session")
        self.monitor.wait_registry_empty()
        self.workload.assert_current(3)

    def test_compaction_and_tablet_restart(self):
        snapshot = self._hold()
        self.workload.write_generation(1)
        self._past(snapshot)
        self._compact()
        # Restart only the target tablets, keeping the KQP owner and handle.
        for path in (self.workload.target_path, self.workload.index_path):
            for tablet in self.monitor.tablets(path):
                self.monitor.restart_tablet(tablet)
        self._past(snapshot)
        snapshot.assert_matches()
        snapshot.assert_matches(kind="index")
        self.workload.assert_current(1)

    def test_split_merge_retains_unvisited_history(self):
        snapshot = self._hold()
        self.workload.write_generation(1)
        self._past(snapshot)
        self._compact()
        original = self.monitor.tablets(self.workload.target_path)
        self.assertEqual(len(original), 1)
        self.monitor.split(self.workload.target_path, original[0], 10)
        split = self.monitor.tablets(self.workload.target_path)
        self.assertEqual(len(split), 2)
        self.assertNotIn(original[0], split)
        self._past(snapshot)
        snapshot.assert_matches()

        self.monitor.merge(self.workload.target_path, split)
        merged = self.monitor.tablets(self.workload.target_path)
        self.assertEqual(len(merged), 1)
        self.assertNotIn(merged[0], split)
        self._past(snapshot)
        self._compact()
        snapshot.assert_matches(kind="point")
        self.workload.assert_current(1)

    def test_copy_has_its_own_retained_snapshot(self):
        source_snapshot = self._hold()
        self.workload.write_generation(1)
        self._past(source_snapshot)
        self._past(source_snapshot, path=self.workload.anchor_path)
        # The unindexed anchor uses the legacy SDK CopyTable operation.
        copy_path = f"{self.workload.anchor_path}_copy"
        self.workload.copy_table(self.workload.anchor_path, copy_path)
        self.addCleanup(self.workload.drop_table, copy_path)
        self.assertEqual(self.workload.read_current(table_path=copy_path), expected_rows(1))

        # Acquire after creation: a snapshot predating CopyTable is not a
        # supported way to read the newly created destination's history.
        copy_snapshot = self._hold()
        self.assertEqual(copy_snapshot.generation, 1)
        for tablet in self.monitor.wait_tablets(copy_path):
            self.monitor.move_tablet(tablet, self.cluster.nodes[2].node_id)
        self.workload.write_generation(2)
        self.workload.execute(f"UPDATE `{copy_path}` SET payload = 'changed';")
        self._past(source_snapshot, generation=2)

        def advance_copy():
            # The copy has its own maxWriteVersion. Advancing only its source
            # cannot move the copy's cleanup boundary past a later write edge.
            self.workload.execute(f"UPDATE `{copy_path}` SET payload = 'gc-pulse';")
            self.workload.execute(f"UPDATE `{copy_path}` SET payload = 'changed';")

        self._past(copy_snapshot, generation=2, path=copy_path, extra_advance=advance_copy)
        self.workload.execute_scheme(f"ALTER TABLE `{copy_path}` COMPACT;")
        source_snapshot.assert_matches()
        self.assertEqual(copy_snapshot.read(table_path=copy_path), expected_rows(1))
        expected_copy = [(key, generation, "changed") for key, generation, _ in expected_rows(1)]
        self.assertEqual(self.workload.read_current(table_path=copy_path), expected_copy)
        self.workload.assert_current(2)


class DisabledRetentionChecks:
    def test_unvisited_old_snapshot_is_rejected(self):
        snapshot = self._hold()
        self.workload.write_generation(1)
        self._past(snapshot)
        self._compact()
        try:
            # ExecuteDataQuery uses KQP read iterators: an unreadable snapshot
            # produces PRECONDITION_FAILED in DataShard, wrapped as ABORTED by
            # KQP. ReadTable and ReadColumns use different error mappings.
            with self.assertRaises((ydb.Aborted, ydb.PreconditionFailed)) as error:
                snapshot.read()
            # Require the snapshot diagnostic to reject unrelated query errors.
            self.assertIn("snapshot", str(error.exception).lower())
        finally:
            snapshot.close("session")
        self.workload.assert_current(1)


class TestSnapshotRetentionDefaultOff(DisabledRetentionChecks, RetentionClusterTest):
    data_shard_locking = None


class TestSnapshotRetentionExplicitOff(DisabledRetentionChecks, RetentionClusterTest):
    data_shard_locking = False


class TestSnapshotRetentionCommonOff(DisabledRetentionChecks, RetentionClusterTest):
    snapshots_locking = False
