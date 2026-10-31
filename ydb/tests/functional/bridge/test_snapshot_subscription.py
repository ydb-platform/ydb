import hashlib
import json
import logging
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.request import urlopen

import ydb
import yatest.common

from common import BridgeKiKiMRTest
from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.tests.library.harness import daemon


MIB = 1 << 20
HARD_LIMIT = 1536 * MIB
DATABASE = "/Root/db1"
ACTIVITIES = {"BOARD_LOOKUP_ACTOR", "LONG_TX_SERVICE"}
MAX_IDLE_EVENT_RATE = 500


class _SnapshotBridgeCluster(BridgeKiKiMRTest):
    def configure_cluster(self):
        config = self.configurator.yaml_config
        config.setdefault("feature_flags", {})["enable_snapshots_locking"] = True
        config.setdefault("memory_controller_config", {})["hard_limit_bytes"] = HARD_LIMIT
        config.setdefault("interconnect_config", {})["enable_external_data_channel"] = False
        config["actor_system_config"] = {
            "use_auto_config": False,
            "executor": [
                {"type": "BASIC", "name": "Common", "threads": 1, "max_threads": 1,
                 "spin_threshold": 0, "priority": 40, "time_per_mailbox_micro_secs": 100},
                {"type": "IO", "name": "IO", "threads": 1},
            ],
            "sys_executor": 0, "user_executor": 0, "batch_executor": 0, "io_executor": 1,
            "service_executor": [{"service_name": "Interconnect", "executor_id": 0}],
            "scheduler": {"resolution": 1024, "progress_threshold": 10000, "spin_threshold": 0},
        }
        config["storage_pool_types"] = [{
            "kind": "hdd", "pool_config": {
                "box_id": 1, "kind": "hdd", "erasure_species": "block-4-2", "bridge_mode": True,
                "vdisk_kind": "Default", "pdisk_filter": [{"property": [{"type": "ROT"}]}],
            },
        }]
        for index, host in enumerate(config["hosts"]):
            location = host["location"]
            location.update(data_center=location["bridge_pile_name"], rack=f"snapshot-rack-{index}", unit=str(index))

    def cleanup(self):
        try:
            for client in getattr(self, "bridge_clients", {}).values():
                client.close()
        finally:
            if getattr(self, "cluster", None) is not None:
                self.cluster.stop()


class TestSnapshotSubscription(unittest.TestCase):
    def setUp(self):
        if yatest.common.context.sanitize:
            self.skipTest("Event-rate bounds and 32-process memory limits require an unsanitized build")
        sdk_logger = logging.getLogger("ydb")
        self.addCleanup(sdk_logger.setLevel, sdk_logger.level)
        sdk_logger.setLevel(logging.WARNING)
        self.bridge = _SnapshotBridgeCluster()
        self.addCleanup(self.bridge.cleanup)
        self.bridge.setup_method()

    def prepare_data(self, node):
        driver = ydb.Driver(ydb.DriverConfig(endpoint=f"grpc://{node.endpoint}", database=DATABASE, disable_discovery=True))
        pool = ydb.SessionPool(driver, size=1)
        try:
            driver.wait(timeout=30)
            settings = ydb.BaseRequestSettings().with_timeout(30)
            pool.retry_operation_sync(lambda session: session.execute_scheme(
                "CREATE TABLE data (id Uint64, payload String, PRIMARY KEY(id));", settings=settings,
            ))
            columns = ydb.BulkUpsertColumns().add_column("id", ydb.PrimitiveType.Uint64).add_column("payload", ydb.PrimitiveType.String)
            for begin in range(0, 1024, 64):
                rows = [{"id": index, "payload": hashlib.shake_256(f"row-{index}".encode()).digest(32768)}
                        for index in range(begin, begin + 64)]
                ydb.retry_operation_sync(
                    lambda: driver.table_client.bulk_upsert(DATABASE + "/data", rows, columns, settings=settings),
                    retry_settings=ydb.RetrySettings(idempotent=True),
                )
            row = pool.retry_operation_sync(lambda session: session.transaction().execute(
                "SELECT COUNT(*) AS row_count, SUM(LENGTH(payload)) AS payload_bytes FROM data;",
                commit_tx=True, settings=settings,
            ))[0].rows[0]
            assert (row.row_count, row.payload_bytes) == (1024, 32 * MIB)
        finally:
            try:
                pool.stop()
            finally:
                self.bridge.stop_driver(driver)

    @staticmethod
    def sample(node):
        assert node.is_alive(), (node.pid, node.daemon.exit_code, node.stderr_file_name)
        pid = node.pid
        identity = (pid, int(Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]))
        status = dict(line.split(":", 1) for line in Path(f"/proc/{pid}/status").read_text().splitlines())
        try:
            with urlopen(f"http://localhost:{node.mon_port}/counters/counters=utils/json", timeout=3) as response:
                sensors = json.load(response)["sensors"]
        finally:
            assert node.is_alive(), (node.pid, node.daemon.exit_code, node.stderr_file_name)
            current_identity = (node.pid, int(Path(f"/proc/{node.pid}/stat").read_text().rsplit(")", 1)[1].split()[19]))
            assert current_identity == identity, (identity, current_identity, node.stderr_file_name)
        counters = {}
        totals = {}
        for sensor in sensors:
            labels = sensor.get("labels", {})
            if labels.get("sensor") == "ReceivedEventsByActivity" and labels.get("activity") in ACTIVITIES:
                counters[json.dumps(labels, sort_keys=True)] = sensor["value"]
                activity = labels["activity"]
                totals[activity] = totals.get(activity, 0) + sensor["value"]
        assert totals.keys() == ACTIVITIES, (pid, counters)
        limits = [sensor["value"] for sensor in sensors if sensor.get("labels", {}).get("sensor") == "Stats/HardLimit"]
        assert limits and all(limit == HARD_LIMIT for limit in limits), (pid, limits)
        return {
            "pid": pid, "identity": identity, "seconds": time.monotonic(), "counters": counters, "totals": totals,
            "rss": int(status["VmRSS"].split()[0]) * 1024, "stderr": node.stderr_file_name,
        }

    def observe(self, phase, nodes, duration, identities, samples):
        deadline = time.monotonic() + duration
        first = previous = None
        with ThreadPoolExecutor(max_workers=16) as executor:
            while True:
                current = {sample["pid"]: sample for sample in executor.map(self.sample, nodes)}
                samples.append({"phase": phase, "nodes": current})
                if max(sample["rss"] for sample in current.values()) > 3 * 1024 * MIB or sum(
                    sample["rss"] for sample in current.values()
                ) > 60 * 1024 * MIB:
                    list(executor.map(daemon.Daemon.kill, [node for node in self.monitored_nodes if node.is_alive()]))
                    raise AssertionError("RSS safety guard exceeded; see snapshot_subscription.json")
                for pid, sample in current.items():
                    assert sample["identity"] == identities[pid], (phase, sample)
                    if previous is not None:
                        before, after = previous[pid]["counters"], sample["counters"]
                        assert before.keys() <= after.keys() and all(after[key] >= value for key, value in before.items()), (phase, sample)
                        elapsed = sample["seconds"] - first[pid]["seconds"]
                        if elapsed >= 10:
                            rates = {
                                activity: (sample["totals"][activity] - first[pid]["totals"][activity]) / elapsed
                                for activity in ACTIVITIES
                            }
                            sample["events_per_second"] = rates
                            assert all(rate <= MAX_IDLE_EVENT_RATE for rate in rates.values()), (phase, pid, rates, sample["stderr"])
                if first is None:
                    first = current
                previous = current
                if time.monotonic() >= deadline:
                    return
                time.sleep(min(5, max(0, deadline - time.monotonic())))

    @staticmethod
    def wait_healthy(node):
        cli = yatest.common.binary_path("ydb/apps/ydb/ydb")
        deadline, result = time.monotonic() + 120, None
        while time.monotonic() < deadline:
            try:
                response = yatest.common.execute(
                    [cli, "--endpoint", f"grpc://{node.endpoint}", "monitoring", "healthcheck", "--format", "json"],
                    timeout=20, check_exit_code=False,
                )
            except yatest.common.ExecutionTimeoutError as error:
                result = str(error)
                continue
            if response.exit_code == 0:
                result = json.loads(response.std_out)
                if result.get("self_check_result") == "GOOD":
                    return
            time.sleep(1)
        raise AssertionError(f"Cluster did not reach GOOD health: {result}")

    def test_idle_secondary_pile_loss(self):
        samples = []
        bridge = self.bridge
        try:
            expected = {"r1": PileState.PRIMARY, "r2": PileState.SYNCHRONIZED}
            bridge.wait_for_cluster_state(bridge.bridge_client, expected, timeout_seconds=120)
            bridge.cluster.create_database(DATABASE, storage_pool_units_count={"hdd": 1})
            slots = {pile: bridge.cluster.register_and_start_slots(DATABASE, count=8, bridge_pile_name=pile) for pile in bridge.pile_names}
            bridge.cluster.wait_tenant_up(DATABASE)
            groups = {pile: bridge.pile_nodes[pile] + slots[pile] for pile in bridge.pile_names}
            assert all(len(bridge.pile_nodes[pile]) == len(slots[pile]) == 8 for pile in bridge.pile_names)
            self.monitored_nodes = groups["r1"] + groups["r2"]
            self.prepare_data(slots["r1"][0])
            bridge.wait_for_cluster_state(bridge.bridge_client, expected, timeout_seconds=120)
            self.wait_healthy(bridge.pile_nodes["r1"][0])
            initial = [self.sample(node) for node in self.monitored_nodes]
            identities = {sample["pid"]: sample["identity"] for sample in initial}
            samples.append({"phase": "initial", "nodes": initial})
            self.observe("baseline", self.monitored_nodes, 10, identities, samples)
            self.wait_healthy(bridge.pile_nodes["r1"][0])
            with ThreadPoolExecutor(max_workers=16) as executor:
                list(executor.map(daemon.Daemon.kill, groups["r2"]))
            assert all(not node.is_alive() for node in groups["r2"])
            self.observe("pile_absent", groups["r1"], 30, identities, samples)
        finally:
            with open(yatest.common.output_path("snapshot_subscription.json"), "w") as output:
                json.dump(samples, output, indent=2, sort_keys=True)
