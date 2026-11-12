import pytest

from ydb.tests.library.compatibility.fixtures import (
    MixedClusterFixture,
    RestartToAnotherVersionFixture,
    RollingUpgradeAndDowngradeFixture,
    current_binary_path,
    current_name,
)
from ydb.tests.library.snapshot_retention import SnapshotRetentionMonitor
from ydb.tests.stress.snapshot_retention.workload import SnapshotRetentionWorkload, expected_rows


def _workload(fixture):
    # Rolling/restart fixtures replace the driver. Do not keep a pool or a
    # transaction belonging to the stopped driver across a version change.
    return SnapshotRetentionWorkload(fixture.driver, fixture.database_path, "snapshot_retention_compat")


def _check_snapshot_and_copy(fixture, generation):
    with _workload(fixture) as workload:
        workload.assert_current(generation - 1)
        with workload.begin_snapshot() as snapshot:
            assert snapshot.generation == generation - 1
            workload.write_generation(generation)

            # The anchor has no secondary index: the legacy CopyTable API
            # accepts it on every tested binary. This snapshot has not read
            # target yet, so SELECT cannot reuse target's previous result.
            copy_path = f"{workload.anchor_path}_copy_{generation}"
            workload.copy_table(workload.anchor_path, copy_path)
            try:
                assert workload.read_current(table_path=copy_path) == expected_rows(generation)
                snapshot.assert_matches()
                workload.assert_current(generation)
            finally:
                workload.drop_table(copy_path)


def _check_split_merge(fixture, generation):
    # Run after the snapshot/copy checks have released all transactions. Old
    # DataShards need to preserve current data across transfers, but are not
    # required to retain an aged snapshot introduced by the new feature.
    with _workload(fixture) as workload:
        workload.assert_current(generation)
        monitor = SnapshotRetentionMonitor(fixture.cluster, fixture.driver, fixture.database_path)
        original = monitor.wait_tablets(workload.target_path, count=1)
        monitor.split(workload.target_path, original[0], boundary=10, timeout=180)
        split = monitor.wait_tablets(workload.target_path, count=2)
        assert original[0] not in split, "Split must replace the source tablet"
        workload.assert_current(generation)
        workload.assert_current(generation, kind="index")

        monitor.merge(workload.target_path, split, timeout=180)
        merged = monitor.wait_tablets(workload.target_path, count=1)
        assert not set(merged).intersection(split), "Merge must replace both source tablets"
        workload.assert_current(generation)
        workload.assert_current(generation, kind="index")


class TestSnapshotRetentionLegacyMixedCluster(MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        # Deliberately omit EnableDataShardSnapshotLocking. Older binaries do
        # not know this field, and the new implementation must remain opt-in.
        # Use the ordinary MVCC window: this is a compatibility regression
        # check, not a promise of retention by an older DataShard.
        yield from self.setup_cluster()

    def test_snapshot_and_copy_with_default_flags(self):
        with _workload(self) as workload:
            workload.create_tables()
            workload.write_generation(0)
        _check_snapshot_and_copy(self, 1)
        _check_snapshot_and_copy(self, 2)
        _check_split_merge(self, 2)


class TestSnapshotRetentionLegacyRestart(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_snapshot_and_copy_after_version_change(self):
        with _workload(self) as workload:
            workload.create_tables()
            workload.write_generation(0)
        _check_snapshot_and_copy(self, 1)

        self.change_cluster_version()

        # A full restart invalidates client sessions. Check persisted data and
        # acquire a new snapshot instead of retrying the old checked one.
        with _workload(self) as workload:
            workload.assert_current(1)
        _check_snapshot_and_copy(self, 2)


class TestSnapshotRetentionLegacyRolling(RollingUpgradeAndDowngradeFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def test_snapshot_and_copy_during_upgrade_and_downgrade(self):
        with _workload(self) as workload:
            workload.create_tables()
            workload.write_generation(0)
        completed = 0
        for generation, _ in enumerate(self.roll(), start=1):
            _check_snapshot_and_copy(self, generation)
            completed += 1

        nodes = len(self.cluster.nodes) + len(self.cluster.slots)
        assert completed == 1 + 2 * nodes, "Both upgrade and downgrade must finish"
        with _workload(self) as workload:
            workload.assert_current(completed)


class TestSnapshotRetentionEnabledCurrent(MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        # A release number is not a capability check: a stable target can
        # predate the flag even when it shares the current release branch.
        # Keep the positive case for binaries built from these sources. The
        # legacy classes above still exercise all mixed/restart/rolling pairs.
        if current_name != "current" or self.all_binary_paths != [current_binary_path]:
            pytest.skip("Retained old reads require all nodes built from current sources")
        yield from self.setup_cluster(
            extra_feature_flags=[
                "enable_snapshots_locking",
                "enable_data_shard_snapshot_locking",
            ],
            datashard_config={
                "keep_snapshot_timeout": 5000,
                "cleanup_snapshot_period": 200,
            },
        )

    def test_first_read_after_watermark_and_copy(self):
        with _workload(self) as workload:
            workload.create_tables()
            workload.write_generation(0)
            with workload.begin_snapshot() as snapshot:
                assert snapshot.generation == 0
                workload.write_generation(1)
                monitor = SnapshotRetentionMonitor(self.cluster, self.driver, self.database_path)

                def advance():
                    # Identical upserts may be skipped by index maintenance.
                    # Commit actual changes in both directions so the index's
                    # maxWriteVersion can advance, ending at generation 1.
                    workload.write_generation(2)
                    workload.write_generation(1)

                # Includes the registry's ordinary 35-second safety margin.
                # Poll the watermark instead of assuming a sleep expired it.
                for path in (workload.target_path, workload.index_path):
                    monitor.wait_past_snapshot(
                        snapshot,
                        path,
                        advance=advance,
                    )
                copy_path = f"{workload.anchor_path}_retained_copy"
                workload.copy_table(workload.anchor_path, copy_path)
                try:
                    assert workload.read_current(table_path=copy_path) == expected_rows(1)
                    for kind in ("range", "point", "index"):
                        snapshot.assert_matches(kind=kind)
                    workload.assert_current(1)
                finally:
                    workload.drop_table(copy_path)
