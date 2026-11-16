import unittest
import uuid
from datetime import datetime

from ydb.public.api.protos import ydb_table_pb2
from ydb.tests.library.harness import daemon
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.snapshot_retention import (
    SnapshotRetentionMonitor,
    configure_retention_hive,
    pinned_driver,
    retention_cluster_config,
)
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.stress.snapshot_retention.workload import (
    KEY_COUNT,
    SnapshotRetentionWorkload,
    expected_rows,
    session_owner_node_id,
)


class _SnapshotReadWrite:
    # Table SDK has no builder for this mode yet, but its public wire API and
    # transaction builder support it through the same name/settings interface.
    name = "snapshot_read_write"

    @property
    def settings(self):
        return ydb_table_pb2.SnapshotRWModeSettings()


class RetentionClusterTest(unittest.TestCase):
    data_shard_locking = True
    snapshots_locking = True
    static_nodes = 3
    target_node_index = 2
    database = "/Root"

    @classmethod
    def _cluster_config(cls):
        return retention_cluster_config(
            nodes=cls.static_nodes,
            snapshots_locking=cls.snapshots_locking,
            data_shard_locking=cls.data_shard_locking,
            extra_feature_flags=["enable_forced_compactions"],
            table_service_config={"enable_snapshot_isolation_rw": True},
            datashard_config={"idle_mem_compaction_interval_seconds": 1},
        )

    @classmethod
    def setUpClass(cls):
        cls.cluster = KiKiMR(cls._cluster_config())
        cls.addClassCleanup(cls.cluster.stop)
        cls.cluster.start()
        # Pin the KQP session and explicitly select the target's process.
        cls.owner_node = cls.cluster.nodes[1]
        cls.target_node = cls.cluster.nodes[cls.target_node_index]
        cls.driver = pinned_driver(cls.cluster, node=cls.owner_node, database=cls.database)
        cls.addClassCleanup(cls.driver.stop)
        cls.hive_id = configure_retention_hive(cls.cluster)

    def setUp(self):
        self.table_prefix = f"retention_{uuid.uuid4().hex[:12]}"
        self.workload = SnapshotRetentionWorkload(self.driver, self.database, self.table_prefix)
        self.addCleanup(self.workload.close)
        self.workload.create_tables()
        self.workload.write_generation(0)
        self.monitor = SnapshotRetentionMonitor(self.cluster, self.driver, self.database, hive_id=self.hive_id)
        self.addCleanup(self.monitor.close)
        self._place_target_tablets()

    def _place_target_tablets(self):
        for path in (self.workload.target_path, self.workload.index_path):
            for tablet in self.monitor.wait_tablets(path):
                self.monitor.move_tablet(tablet, self.target_node.node_id)
                self.assertEqual(self.monitor.tablet_node(tablet), self.target_node.node_id)

    def _hold(self, tx_mode=None):
        snapshot = self.workload.begin_snapshot(tx_mode=tx_mode)
        self.addCleanup(snapshot.close, "session")
        self.assertEqual(snapshot.owner_node_id, self.owner_node.node_id)
        return snapshot

    def _past(self, snapshot, generation=1, paths=None, extra_advance=None):
        def advance():
            self.workload.pulse(generation)
            if extra_advance is not None:
                extra_advance()

        bound = self.monitor.snapshot_bound(snapshot, advance=advance)
        if paths is None:
            paths = (self.workload.target_path,)
        for path in paths:
            self.monitor.wait_low_watermark(path, bound, advance=advance)

    def _compact(self):
        self.workload.execute_scheme(
            f"ALTER TABLE `{self.workload.target_path}` COMPACT WITH (CASCADE = true, PARALLEL = 2);"
        )


class FirstSnapshotReadChecks:
    def _first_read(self, kind):
        snapshot = self._hold()
        self.assertEqual(snapshot.generation, 0)
        self.workload.write_generation(1)
        paths = [self.workload.target_path]
        if kind == "index":
            paths.append(self.workload.index_path)
        self._past(snapshot, paths=paths)

        def promoted_version():
            state = self.monitor.snapshot_state(self.owner_node.node_id)
            versions = state["local_snapshots"]
            return next(iter(versions)) if state["registry_ready"] and len(versions) == 1 else None

        version = self.monitor._wait("the owner's promoted held snapshot", promoted_version)
        local_target = self.target_node.node_id == self.owner_node.node_id

        def registered():
            state = self.monitor.snapshot_state(self.target_node.node_id)
            if not state["registry_ready"]:
                return None
            present = (
                version in state["local_snapshots"] if local_target
                else (self.owner_node.node_id, *version) in state["remote_snapshots"]
            )
            return state if present else None

        state = self.monitor._wait("held snapshot registered on the target node", registered)
        self.assertLess(version, state["border"])
        if local_target:
            # The one-node fixture exercises local promotion without receiving
            # this version through the remote exchange or a peer's registry.
            self.assertEqual(state["remote_nodes"], {})
            self.assertEqual(state["remote_snapshots"], set())
        for path in (self.workload.target_path, self.workload.index_path):
            for tablet in self.monitor.wait_tablets(path):
                self.assertEqual(self.monitor.tablet_node(tablet), self.target_node.node_id)
        self._compact()
        snapshot.assert_matches(kind=kind)
        self.workload.assert_current(1)
        snapshot.close()
        self.monitor._wait(
            "released snapshot leaves the owner's local storage",
            lambda: version not in self.monitor.snapshot_state(self.owner_node.node_id)["local_snapshots"],
        )
        self.monitor.wait_registry_empty()

    def test_first_range_read_below_watermark(self):
        self._first_read("range")

    def test_first_point_read_below_watermark(self):
        self._first_read("point")

    def test_first_index_read_below_watermark(self):
        self._first_read("index")


class TestSnapshotRetention(FirstSnapshotReadChecks, RetentionClusterTest):
    def _truncate_held_snapshot(self, kind):
        # Each read kind gets its own transaction: an earlier failed read must
        # not invalidate the transaction before the path being checked runs.
        snapshot = self._hold()
        self.assertEqual(snapshot.generation, 0)
        self.workload.write_generation(1)
        self._past(snapshot, paths=(self.workload.target_path, self.workload.index_path))
        self.workload.assert_current(1)
        self.workload.assert_current(1, kind="index")
        registry_nodes = [self.cluster.nodes[2].node_id]
        self.monitor.wait_registry_count(1, node_ids=registry_nodes)

        # Plain schema TRUNCATE destroys history in both the main table and
        # its index. Never call _past afterwards: it would refill the table.
        self.workload.execute_scheme(f"TRUNCATE TABLE `{self.workload.target_path}`;")
        self.assertEqual(self.workload.read_current(), [])
        self.assertEqual(self.workload.read_current(kind="index"), [])
        self.monitor.wait_registry_count(1, node_ids=registry_nodes)
        try:
            with self.assertRaises((ydb.Aborted, ydb.PreconditionFailed)) as error:
                snapshot.read(kind=kind)
            # A successful empty result would silently corrupt the historical
            # read. Require the snapshot error, not a session or schema error.
            self.assertIn("snapshot", str(error.exception).lower())
        finally:
            snapshot.close("session")
        self.monitor.wait_registry_empty()

    def test_truncate_rejects_held_range_snapshot(self):
        self._truncate_held_snapshot("range")

    def test_truncate_rejects_held_point_snapshot(self):
        self._truncate_held_snapshot("point")

    def test_truncate_rejects_held_index_snapshot(self):
        self._truncate_held_snapshot("index")

    def _past_without_read_conflicts(self, snapshot):
        # Neither the anchor's key 0 nor the target's checked range [0, KEY_COUNT)
        # may change in the successful Serializable case. Use an unrelated key
        # to acknowledge a newer write and advance both shards' MVCC edges.
        pulse_key = KEY_COUNT + 2
        pulse = 0

        def advance():
            nonlocal pulse
            pulse += 1
            self.workload.execute("\n".join(
                f"UPSERT INTO `{path}` (key, generation, payload) "
                f"VALUES ({pulse_key}u, {snapshot.generation + 1}u, 'gc-pulse-{pulse}');"
                for path in (self.workload.anchor_path, self.workload.target_path)
            ))

        bound = self.monitor.snapshot_bound(snapshot, advance=advance, anchor_key=pulse_key)
        self.monitor.wait_low_watermark(self.workload.target_path, bound, advance=advance)

    def _read_write_after_watermark(self, tx_mode, *, conflict, write_key):
        snapshot = self._hold(tx_mode)
        self.assertEqual(snapshot.generation, 0)
        if conflict:
            self.workload.execute(
                f"UPDATE `{self.workload.target_path}` SET payload = 'concurrent-write' WHERE key = 0u;"
            )
        self._past_without_read_conflicts(snapshot)
        self._compact()

        # The first target read must return the retained data. An unreadable
        # snapshot must fail here, rather than satisfy the conflict assertion.
        snapshot.assert_matches()
        marker_key = KEY_COUNT + 1
        write_query = (
            f"UPSERT INTO `{self.workload.target_path}` (key, generation, payload) VALUES "
            f"({write_key}u, 2u, 'retained-write'), ({marker_key}u, 2u, 'commit-marker');"
        )
        if conflict:
            with self.assertRaises(ydb.Aborted) as error:
                snapshot.execute(write_query)
                snapshot.close("commit")
            # Stale-snapshot errors are also ABORTED in KQP. Require an actual
            # lock conflict, whether it is detected on the write or at commit.
            self.assertIn("locks invalidated", str(error.exception).lower())
            snapshot.close("session")
        else:
            snapshot.execute(write_query)
            snapshot.close("commit")

        expected = {row[0]: row for row in expected_rows(0)}
        if conflict:
            expected[0] = (0, 0, "concurrent-write")
        else:
            expected[write_key] = (write_key, 2, "retained-write")
            expected[marker_key] = (marker_key, 2, "commit-marker")
        result = self.workload.execute(
            f"SELECT key, generation, payload FROM `{self.workload.target_path}` "
            f"WHERE key < {KEY_COUNT + 2}u ORDER BY key;"
        )
        actual = [(int(row["key"]), int(row["generation"]), row["payload"]) for row in result[0].rows]
        # Check the fresh committed state, including absence of partial writes
        # and preservation of the competing writer's value after ABORTED.
        self.assertEqual(actual, sorted(expected.values()))
        self.workload.assert_current(0, table_path=self.workload.anchor_path)
        self.monitor.wait_registry_empty()

    def test_serializable_commit_after_first_read_below_watermark(self):
        self._read_write_after_watermark(ydb.SerializableReadWrite(), conflict=False, write_key=KEY_COUNT)

    def test_serializable_read_conflict_below_watermark(self):
        # Only the read key conflicts; both written keys are different.
        self._read_write_after_watermark(ydb.SerializableReadWrite(), conflict=True, write_key=KEY_COUNT)

    def test_snapshot_rw_commit_after_first_read_below_watermark(self):
        self._read_write_after_watermark(_SnapshotReadWrite(), conflict=False, write_key=0)

    def test_snapshot_rw_write_conflict_below_watermark(self):
        # Snapshot isolation rejects competing writes of the same key.
        self._read_write_after_watermark(_SnapshotReadWrite(), conflict=True, write_key=0)

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
        self._past(snapshots[0], generation=3, paths=(self.workload.target_path, self.workload.index_path))
        self._compact()
        for snapshot in (snapshots[0], snapshots[2]):
            snapshot.assert_matches()
            snapshot.assert_matches(kind="index")
        snapshots[0].close("commit")
        self.monitor.wait_registry_count(1, node_ids=registry_nodes)
        self._past(snapshots[2], generation=3, paths=(self.workload.target_path, self.workload.index_path))
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
        split = self.monitor.split(self.workload.target_path, original[0], 10)
        self.assertEqual(len(split), 2)
        self.assertNotIn(original[0], split)
        self._past(snapshot)
        snapshot.assert_matches()

        merged = self.monitor.merge(self.workload.target_path, split)
        self.assertEqual(len(merged), 1)
        self.assertNotIn(merged[0], split)
        self._past(snapshot)
        self._compact()
        snapshot.assert_matches(kind="point")
        self.workload.assert_current(1)

    def test_copy_has_its_own_retained_snapshot(self):
        source_snapshot = self._hold()
        self.workload.write_generation(1)
        self._past(source_snapshot, paths=(self.workload.target_path, self.workload.anchor_path))
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

        self._past(copy_snapshot, generation=2, paths=(copy_path,), extra_advance=advance_copy)
        self.workload.execute_scheme(f"ALTER TABLE `{copy_path}` COMPACT;")
        source_snapshot.assert_matches()
        self.assertEqual(copy_snapshot.read(table_path=copy_path), expected_rows(1))
        expected_copy = [(key, generation, "changed") for key, generation, _ in expected_rows(1)]
        self.assertEqual(self.workload.read_current(table_path=copy_path), expected_copy)
        self.workload.assert_current(2)


class TestSnapshotRetentionSameNode(FirstSnapshotReadChecks, RetentionClusterTest):
    static_nodes = 1
    target_node_index = 1


class TestSnapshotRetentionOverflow(RetentionClusterTest):
    max_version = (2**64 - 1, 2**64 - 1)
    max_remote_snapshots = 1

    @classmethod
    def _cluster_config(cls):
        config = super()._cluster_config()
        config.yaml_config["long_tx_service_config"]["max_remote_snapshots"] = cls.max_remote_snapshots
        return config

    def test_overflow_preserves_snapshots_and_releases_cleanup_boundary(self):
        snapshot_count = self.max_remote_snapshots + 2
        snapshots = []
        for generation in range(snapshot_count):
            self.workload.write_generation(generation)
            snapshot = self._hold()
            self.assertEqual(snapshot.generation, generation)
            snapshots.append(snapshot)
        self.workload.write_generation(snapshot_count)

        def held_versions():
            versions = self.monitor.snapshot_state(self.owner_node.node_id)["local_snapshots"]
            return sorted(versions) if len(versions) == snapshot_count else None

        versions = self.monitor._wait(f"{snapshot_count} distinct promoted held versions", held_versions)
        # Distinct generations were committed between the anchor reads, so
        # these ordered versions correspond to the held transactions.
        target_id, owner_id = self.target_node.node_id, self.owner_node.node_id
        tablets = [
            tablet
            for path in (self.workload.target_path, self.workload.index_path)
            for tablet in self.monitor.wait_tablets(path)
        ]

        def advance():
            self.workload.pulse(snapshot_count)

        def wait_registry(border, explicit_versions):
            expected = {(owner_id, *version) for version in explicit_versions}

            def matched():
                state = self.monitor.snapshot_state(target_id)
                if (
                    state["registry_ready"]
                    and state["border"] == border
                    and state["remote_snapshots"] == expected
                ):
                    return state
                return None

            # Poll without SQL in flight: a slow independent query can itself
            # temporarily exceed the configured limit and lower the border.
            self.monitor._wait(f"registry border {border} and explicit snapshots {expected}", matched)
            self.monitor.wait_registry_count(len(expected), node_ids=[target_id])

        def cleanup_at_border(border, explicit_versions):
            wait_registry(border, explicit_versions)
            observed = {}

            def reached():
                state = self.monitor.snapshot_state(target_id)
                if not state["registry_ready"] or state["border"] != border:
                    return None
                observed.update({tablet: self.monitor.low_watermark(tablet) for tablet in tablets})
                for watermark in observed.values():
                    self.assertLessEqual(watermark, border, f"Cleanup exceeded overflow border: {observed}")
                return observed if all(watermark == border for watermark in observed.values()) else None

            # Overflow protects an entire range at/above border. Requiring LWM
            # to pass that border, as _past() does, would wait forever by design.
            try:
                self.monitor._wait(f"base/index cleanup reaches overflow border {border}", reached, advance=advance)
            except AssertionError as error:
                raise AssertionError(f"{error}; last watermarks: {observed}") from error
            self._compact()

        def check_snapshots(held):
            for snapshot in held:
                for kind in ("range", "point", "index"):
                    snapshot.assert_matches(kind=kind)

        def cleanup_past_fresh_write():
            advance()
            bound = self.monitor.write_edge(self.workload.anchor_path)
            for path in (self.workload.target_path, self.workload.index_path):
                self.monitor.wait_low_watermark(path, bound, advance=advance)
            self._compact()

        def check_active(active):
            remaining = [versions[index] for index in active]
            explicit = remaining[:self.max_remote_snapshots]
            border = (
                remaining[self.max_remote_snapshots]
                if len(remaining) > self.max_remote_snapshots else self.max_version
            )
            if border == self.max_version:
                wait_registry(border, explicit)
                cleanup_past_fresh_write()
            else:
                cleanup_at_border(border, explicit)
            check_snapshots(snapshots[index] for index in active)

        # The oldest points are explicit; the first omitted version is an
        # inclusive border protecting the two newer snapshots as a range.
        active = list(range(snapshot_count))
        check_active(active)

        snapshots[1].close("rollback")
        active.remove(1)
        # At limit 3 this removes a middle explicit point, promotes the former
        # border into the explicit set, and advances the border to the last one.
        check_active(active)

        snapshots[0].close("commit")
        active.remove(0)
        check_active(active)

        for index in list(active[:-1]):
            snapshots[index].close("commit")
            active.remove(index)
            check_active(active)

        snapshots[active.pop()].close("session")
        check_active(active)
        self.monitor.wait_registry_empty()
        self.workload.assert_current(snapshot_count)
        self.workload.assert_current(snapshot_count, kind="index")


class TestSnapshotRetentionOverflowThreePoints(TestSnapshotRetentionOverflow):
    max_remote_snapshots = 3


class TestSnapshotRetentionNodeFailures(RetentionClusterTest):
    # Disks and root services stay on a static node. Faults affect only tenant
    # compute processes, so loss of storage cannot masquerade as a GC failure.
    static_nodes = 1
    database = "/Root/snapshot_retention_nodes"
    unavailable_node_expiration_seconds = 30

    @staticmethod
    def _runtime_node_id(driver):
        settings = ydb.BaseRequestSettings().with_timeout(50).with_operation_timeout(45)
        session = driver.table_client.session().create(settings=settings)
        try:
            session.transaction().execute("SELECT 1;", commit_tx=True, settings=settings)
            return session_owner_node_id(session.session_id)
        finally:
            session.delete(settings=settings)

    @classmethod
    def setUpClass(cls):
        config = cls._cluster_config()
        config.yaml_config["long_tx_service_config"]["unavailable_node_snapshots_expiration_time_seconds"] = (
            cls.unavailable_node_expiration_seconds
        )
        cls.cluster = KiKiMR(config)
        cls.addClassCleanup(cls.cluster.stop)
        cls.cluster.start()
        cls.cluster.create_database(cls.database, storage_pool_units_count={"hdd": 1})
        slots = cls.cluster.register_and_start_slots(cls.database, count=3)
        cls.cluster.wait_tenant_up(cls.database)
        drivers = []
        for slot in slots:
            driver = pinned_driver(cls.cluster, node=slot, database=cls.database)
            cls.addClassCleanup(driver.stop)
            # Harness slot indices are not NodeBroker ids. Monitoring and Hive
            # must use the runtime id, as must the placement/owner assertions.
            slot.node_id = SnapshotRetentionMonitor._wait(
                "compute node accepting queries", lambda: cls._runtime_node_id(driver), timeout=90,
            )
            drivers.append(driver)
        if len({slot.node_id for slot in slots}) != 3:
            raise AssertionError("The node-failure fixture requires three distinct compute nodes")
        cls.owner_node, cls.target_node, cls.peer_node = slots
        cls.driver, cls.target_driver, cls.peer_driver = drivers
        # Resolve before any node fault; a replacement monitor must not need
        # another tenant SchemeShard lookup while that tablet is recovering.
        cls.hive_id = configure_retention_hive(cls.cluster, database=cls.database)

    def setUp(self):
        super().setUp()
        for tablet in self.monitor.wait_tablets(self.workload.anchor_path):
            self.monitor.move_tablet(tablet, self.target_node.node_id)
        nodes = (self.owner_node, self.target_node, self.peer_node)
        for observer in nodes:
            for peer in nodes:
                if observer is not peer:
                    self.monitor.wait_remote_node(observer.node_id, peer.node_id)

    def _restore_node(self, node):
        if node.is_alive():
            return
        node.start()
        driver = pinned_driver(self.cluster, node=node, database=self.database)
        cls = type(self)
        cls.addClassCleanup(driver.stop)
        node.node_id = self.monitor._wait(
            "restarted compute node accepting queries", lambda: self._runtime_node_id(driver), timeout=90,
        )
        if node is self.owner_node:
            cls.driver = driver
        elif node is self.target_node:
            cls.target_driver = driver
        else:
            assert node is self.peer_node
            cls.peer_driver = driver

    def _held_before_node_failure(self):
        snapshot = self._hold()
        self.assertEqual(snapshot.generation, 0)
        self.workload.write_generation(1)
        self._past(snapshot, paths=(self.workload.target_path, self.workload.index_path))
        self._compact()
        self.monitor.wait_registry_count(1, node_ids=[self.target_node.node_id, self.peer_node.node_id])
        return snapshot

    def _wait_for_cleanup(self, bound, generation):
        def advance():
            self.workload.pulse(generation)

        for path in (self.workload.target_path, self.workload.index_path):
            self.monitor.wait_low_watermark(path, bound, advance=advance, timeout=90)
        self._compact()
        self.workload.assert_current(generation)
        self.workload.assert_current(generation, kind="index")

    def test_datashard_node_restart_preserves_held_snapshot(self):
        snapshot = self._held_before_node_failure()
        self.addCleanup(self._restore_node, self.target_node)
        # KiKiMRNode.kill() starts the process again immediately. The base
        # daemon operation lets this test control the actual process restart.
        daemon.Daemon.kill(self.target_node)
        self.assertFalse(self.target_node.is_alive())
        self._restore_node(self.target_node)
        self._place_target_tablets()
        self.monitor.wait_remote_node(self.target_node.node_id, self.owner_node.node_id)
        self.monitor.wait_registry_count(1, node_ids=[self.target_node.node_id])
        self._past(snapshot, paths=(self.workload.target_path, self.workload.index_path))
        self._compact()
        for kind in ("range", "point", "index"):
            snapshot.assert_matches(kind=kind)
        snapshot.close()
        self.monitor.wait_registry_empty(
            node_ids=[node.node_id for node in (self.owner_node, self.target_node, self.peer_node)]
        )

    def test_owner_node_crash_expires_retained_snapshot(self):
        snapshot = self._held_before_node_failure()

        def held_version():
            versions = self.monitor.snapshot_state(self.owner_node.node_id)["local_snapshots"]
            return next(iter(versions)) if len(versions) == 1 else None

        version = self.monitor._wait("the owner's promoted held snapshot", held_version)
        owner_id = self.owner_node.node_id
        remote_version = (owner_id, *version)
        survivor_ids = [self.target_node.node_id, self.peer_node.node_id]
        for node_id in survivor_ids:
            self.monitor._wait(
                f"held snapshot {version} propagated to node {node_id}",
                lambda node_id=node_id: remote_version in self.monitor.snapshot_state(node_id)["remote_snapshots"],
            )
        self.addCleanup(self._restore_node, self.owner_node)
        daemon.Daemon.kill(self.owner_node)
        self.assertFalse(self.owner_node.is_alive())
        # The process is already dead: do not turn this into a normal release
        # by sending rollback/session.delete during client-side cleanup.
        snapshot.abandon()
        self.workload = SnapshotRetentionWorkload(self.target_driver, self.database, self.table_prefix)
        self.addCleanup(self.workload.close)
        self.monitor = SnapshotRetentionMonitor(self.cluster, self.target_driver, self.database, hive_id=self.hive_id)
        self.addCleanup(self.monitor.close)
        for node_id in survivor_ids:
            state = self.monitor.wait_remote_node(node_id, owner_id, present=False, timeout=90)
            self.assertNotIn(remote_version, state["remote_snapshots"])
        self.monitor.wait_registry_empty(node_ids=survivor_ids, timeout=90)
        # A new acknowledged bound after expiration prevents an already high
        # watermark from making the GC-resumption check pass vacuously.
        self.workload.write_generation(2)
        bound = self.monitor.write_edge(self.workload.anchor_path)
        self._wait_for_cleanup(bound, generation=2)
        self.assertFalse(self.owner_node.is_alive())

    def test_unavailable_peer_cleanup_resumes_after_expiration(self):
        node_ids = [node.node_id for node in (self.owner_node, self.target_node, self.peer_node)]
        self.monitor.wait_registry_empty(node_ids=node_ids)
        observer_id, peer_id = self.target_node.node_id, self.peer_node.node_id
        state = self.monitor.wait_remote_node(observer_id, peer_id)
        previous_time = state["remote_nodes"][peer_id]

        def refreshed():
            current = self.monitor.snapshot_state(observer_id)
            collection_time = current["remote_nodes"].get(peer_id)
            return current if collection_time is not None and collection_time != previous_time else None

        # Observe a live collection round first, so an unknown/unpublished peer
        # cannot make this test pass without exercising a stale collection time.
        self.monitor._wait("peer collection time advancing before its crash", refreshed)
        tablets = [
            tablet
            for path in (self.workload.target_path, self.workload.index_path)
            for tablet in self.monitor.wait_tablets(path)
        ]
        self.addCleanup(self._restore_node, self.peer_node)
        daemon.Daemon.kill(self.peer_node)
        self.assertFalse(self.peer_node.is_alive())
        self.workload.write_generation(1)
        config = self.cluster.config.yaml_config["long_tx_service_config"]
        freshness_margin_ms = config["local_snapshot_promotion_time_seconds"] * 1000 + config["max_clock_skew_ms"]

        def advance():
            self.workload.pulse(1)

        def expired():
            before = self.monitor.snapshot_state(observer_id)
            if peer_id not in before["remote_nodes"]:
                return before
            collection_time = before["remote_nodes"][peer_id]
            collection_ms = int(datetime.fromisoformat(collection_time.replace("Z", "+00:00")).timestamp() * 1000)
            watermarks = {tablet: self.monitor.low_watermark(tablet) for tablet in tablets}
            after = self.monitor.snapshot_state(observer_id)
            # A slow write or monitoring request may outlive expiration. Do
            # not require the peer to stay present for an extra fixed interval.
            if peer_id not in after["remote_nodes"]:
                return after
            if after["remote_nodes"].get(peer_id) != collection_time:
                return None
            self.assertTrue(
                all(version[0] <= collection_ms - freshness_margin_ms for version in watermarks.values()),
                f"Cleanup crossed the unavailable peer's freshness floor: {collection_time}, {watermarks}",
            )
            return None

        self.monitor._wait(
            "unavailable snapshot peer expires while writes continue", expired, advance=advance, timeout=90,
        )
        self.workload.write_generation(2)
        bound = self.monitor.write_edge(self.workload.anchor_path)
        self._wait_for_cleanup(bound, generation=2)
        self.assertFalse(self.peer_node.is_alive())


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
