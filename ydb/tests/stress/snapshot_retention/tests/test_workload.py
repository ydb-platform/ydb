import threading
import unittest
from urllib.parse import parse_qs, urlparse

import yatest.common

from ydb.tests.library.clients.kikimr_http_client import HiveClient
from ydb.tests.library.harness.kikimr_config import Erasure, KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.snapshot_retention import SnapshotRetentionMonitor
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.stress.snapshot_retention.workload import (
    HOLD_SECONDS,
    KEEP_SNAPSHOT_TIMEOUT_MS,
    SnapshotRetentionWorkload,
)


class TestSnapshotRetentionWorkload(unittest.TestCase):
    def test_concurrent_writes_and_retained_reads(self):
        self._run("normal")

    def test_tablet_restarts_and_retained_reads(self):
        self._run("restart")

    def test_split_merge_and_retained_reads(self):
        self._run("split_merge")

    def _run(self, mode):
        config = KikimrConfigGenerator(
            erasure=Erasure.NONE,
            nodes=3,
            use_in_memory_pdisks=False,
            extra_feature_flags=["enable_snapshots_locking", "enable_data_shard_snapshot_locking"],
            # The default write-sequence protocol can fail during a split with
            # "Ancestor shard ... not found in lock" before retained reads run.
            # Isolate that unrelated protocol in this retention scenario; the
            # normal and restart cases keep the default protocol enabled.
            disabled_feature_flags=(
                ["enable_data_shard_uncommitted_write_seq_num"] if mode == "split_merge" else []
            ),
            table_service_config={
                "session_balancer_settings": {"enable_tier": "TIER_DISABLED"},
            },
            datashard_config={
                "keep_snapshot_timeout": KEEP_SNAPSHOT_TIMEOUT_MS,
                "cleanup_snapshot_period": 200,
                "stats_report_interval_seconds": 1,
            },
        )
        config.yaml_config["long_tx_service_config"] = {
            "local_snapshot_promotion_time_seconds": 1,
            "max_clock_skew_ms": 1000,
            "snapshots_exchange_interval_seconds": 1,
            "snapshots_registry_update_interval_seconds": 1,
            "prefill_timeout_seconds": 1,
        }
        cluster = KiKiMR(config)
        self.addCleanup(cluster.stop)
        cluster.start()
        driver = ydb.Driver(ydb.DriverConfig(
            endpoint=cluster.nodes[1].endpoint,
            database="/Root",
            disable_discovery=True,
        ))
        self.addCleanup(driver.stop)
        driver.wait(timeout=60)
        HiveClient("localhost", cluster.nodes[1].mon_port).set_min_scatter_to_balance(101)
        monitor = SnapshotRetentionMonitor(cluster, driver, "/Root")
        with SnapshotRetentionWorkload(driver, "/Root") as workload:
            workload.create_tables()
            workload.write_generation(0)
            for path in (workload.target_path, workload.index_path):
                for tablet in monitor.wait_tablets(path):
                    monitor.move_tablet(tablet, cluster.nodes[2].node_id)

            faults = {"restart": 0, "split": 0, "merge": 0}

            def wait_past_snapshot(held):
                # Background writes advance maxWriteVersion. Monitoring does
                # not access target through the checked user transaction.
                for path in (workload.target_path, workload.index_path):
                    monitor.wait_past_snapshot(held, path)

            def before_read(held):
                owner = int(parse_qs(urlparse(held.session_id).query)["node_id"][0])
                self.assertEqual(owner, cluster.nodes[1].node_id)
                wait_past_snapshot(held)
                injected = False
                if mode == "restart" and faults["restart"] < 2:
                    # Restart only a remote DataShard tablet; the session and
                    # retention handle owner remain alive on node 1.
                    tablets = monitor.wait_tablets(workload.target_path)
                    self.assertEqual(len(tablets), 1)
                    self.assertEqual(monitor.tablet_node(tablets[0]), cluster.nodes[2].node_id)
                    monitor.restart_tablet(tablets[0])
                    faults["restart"] += 1
                    injected = True
                elif mode == "split_merge" and not faults["split"]:
                    tablets = monitor.wait_tablets(workload.target_path)
                    self.assertEqual(len(tablets), 1)
                    monitor.split(workload.target_path, tablets[0], boundary=10)
                    faults["split"] += 1
                    injected = True
                elif mode == "split_merge" and not faults["merge"]:
                    tablets = monitor.wait_tablets(workload.target_path)
                    self.assertEqual(len(tablets), 2)
                    monitor.merge(workload.target_path, tablets)
                    faults["merge"] += 1
                    injected = True
                if injected:
                    # Split/merge may place destination tablets on a different
                    # node. Keep the checked DataShard remote from the KQP owner.
                    for tablet in monitor.wait_tablets(workload.target_path):
                        if monitor.tablet_node(tablet) != cluster.nodes[2].node_id:
                            monitor.move_tablet(tablet, cluster.nodes[2].node_id)
                        self.assertEqual(monitor.tablet_node(tablet), cluster.nodes[2].node_id)
                    wait_past_snapshot(held)

            readers = 4
            duration = float(yatest.common.get_param("stress_default_duration", default="60"))
            # Serialize injection and the entire checked read, including
            # every point-read RPC. Other readers keep their old snapshots,
            # and the independent writer runs throughout tablet operations.
            guard = threading.Lock() if mode != "normal" else None
            stats = workload.run(
                duration, HOLD_SECONDS, readers, seed=57975,
                before_read=before_read, read_guard=guard,
                retry_uncertain_writes=mode != "normal",
            )
            self.assertGreaterEqual(stats.completed_reads, readers)
            self.assertTrue(all(count > 0 for count in stats.completed_by_reader))
            self.assertGreater(stats.writes, 1)
            if mode == "restart":
                self.assertEqual(faults["restart"], 2)
            elif mode == "split_merge":
                self.assertEqual(faults["split"], 1)
                self.assertEqual(faults["merge"], 1)
            monitor.wait_registry_empty()
