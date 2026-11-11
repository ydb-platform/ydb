"""Automatic Skipper failover against real YDB bridge clusters."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import json
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.request
import uuid

import grpc
import pytest
import yatest.common
import ydb
from library.python.port_manager import PortManager

from common import BridgeKiKiMRTest
from ydb.public.api.grpc.draft import ydb_bridge_v1_pb2_grpc as bridge_grpc
from ydb.public.api.grpc.draft import ydb_tablet_v1_pb2_grpc as tablet_grpc
from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge
from ydb.public.api.protos.draft import ydb_tablet_pb2 as tablet
from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.library.common.msgbus_types import MessageBusStatus
from ydb.tests.library.common.types import Erasure, TabletTypes
from ydb.tests.library.harness import daemon


TOKEN = (("x-ydb-auth-ticket", "root@builtin"),)
PRIMARY = PileState.PRIMARY
SYNC = PileState.SYNCHRONIZED
DISCONNECTED = PileState.DISCONNECTED
UNSYNC = PileState.NOT_SYNCHRONIZED


def http(url, body=None, timeout=3):
    request = urllib.request.Request(url, data=None if body is None else json.dumps(body).encode(),
                                     headers={} if body is None else {"Content-Type": "application/json"})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        response = opener.open(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, json.load(response)


def eventually(action, description, timeout=120):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            result = action()
            if result:
                return result
        except (AssertionError, grpc.RpcError, OSError, ValueError) as error:
            last = error
        time.sleep(0.25)
    raise AssertionError("Timed out waiting for %s; last=%r" % (description, last))


def state_rpc(node, connectivity=True):
    request = bridge.GetClusterStateRequest(include_pile_connectivity=connectivity)
    request.operation_params.operation_timeout.seconds = 5
    with grpc.insecure_channel(node.endpoint) as channel:
        response = bridge_grpc.BridgeServiceStub(channel).GetClusterState(
            request, metadata=TOKEN, timeout=7)
    assert response.operation.status == StatusIds.SUCCESS, response
    result = bridge.GetClusterStateResult()
    assert response.operation.result.Unpack(result), response
    return result


class Skipper:
    def __init__(self, binary, directory, piles, port):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.url = "http://127.0.0.1:%d" % port
        token = self.directory / "token"
        token.write_text("root@builtin\n")
        token.chmod(0o600)
        config = dict(cluster_id="functional-test", endpoints={
            name: ["grpc://" + node.endpoint for node in nodes] for name, nodes in piles.items()},
            auth=dict(token_file=str(token)), http=dict(listen="127.0.0.1:%d" % port))
        config_path = self.directory / "config.json"
        config_path.write_text(json.dumps(config, indent=2))
        self.log = (self.directory / "skipper.log").open("w")
        self.process = subprocess.Popen([binary, "--config", str(config_path), "--state-dir", str(self.directory)],
                                        stdin=subprocess.DEVNULL, stdout=self.log, stderr=subprocess.STDOUT)
        try:
            self.state()
        except BaseException:
            self.close()
            raise

    def state(self):
        assert self.process.poll() is None, "Skipper exited; see %s" % self.directory

        def read():
            status, result = http(self.url + "/api/state")
            return result if status == 200 and result["controller_ready"] else None
        return eventually(read, "Skipper readiness", timeout=20)

    def mode(self, mode):
        for _ in range(20):
            state = self.state()
            body = dict(mode=mode, expected_boot_id=state["controller_boot_id"],
                        expected_mode_revision=state["mode_revision"], request_id=uuid.uuid4().hex)
            if mode == "auto":
                body["reviewed_snapshot_revision"] = state["snapshot_revision"]
            status, result = http(self.url + "/api/mode", body, timeout=18)
            if status == 409 and result.get("error") == "SNAPSHOT_CHANGED":
                # A definite refusal consumes this revision. Never repeat an uncertain POST.
                time.sleep(0.25)
                continue
            assert status == 200, (status, result)
            return
        raise AssertionError("Could not review a stable Auto snapshot")

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
                raise
        self.log.close()


class RealSkipperCluster(BridgeKiKiMRTest):
    def setup_method(self, method):
        self.hold_stage2 = method.__name__ == "test_stage2_failure"
        self.skipper = None
        self.ports = PortManager()
        self.cleanup = ExitStack()
        self.cleanup.callback(self.ports.release)
        self.bridge_clients = {}
        self.cleanup.callback(self.close_cluster)
        self.rows = {i: "acknowledged-%d-%s" % (i, "x" * 4096) for i in range(64)}
        self.output = Path(yatest.common.test_output_path("skipper"))
        self.output.mkdir(parents=True, exist_ok=True)
        try:
            super().setup_method()
            # The harness can attempt binding before SchemeShard is ready and
            # ignore its error. Check the stored binding and repair it if needed.
            pools = {pool["name"]: pool["kind"] for pool in self.configurator.dynamic_storage_pools}
            token = self.cluster.root_token or self.configurator.default_clusteradmin

            def storage_bound():
                response = self.cluster.client.describe("/" + self.cluster.domain_name, token or "")
                assert response.Status == MessageBusStatus.MSTATUS_OK, response
                actual = {pool.Name: pool.Kind for pool in response.PathDescription.DomainDescription.StoragePools}
                if actual == pools:
                    return True
                self.cluster.client.bind_storage_pools(self.cluster.domain_name, pools, token=token)
                return False

            eventually(storage_bound, "storage pool binding before table creation")
        except BaseException:
            self.cleanup.close()
            raise

    def configure_cluster(self):
        # Skipper intentionally sends whole-cluster SelfCheck with no database.
        # The general bridge harness disables this by default.
        self.configurator.yaml_config.setdefault("feature_flags", {})["allow_ydb_requests_without_database"] = True
        if self.hold_stage2:
            # Only this scenario changes sync rate: its later Rejoin must remain
            # in stage 2 long enough to inject loss after proving participation.
            self.configurator.yaml_config.setdefault("blob_storage_config", {})["bridge_sync_rate_bytes_per_second"] = 1024

    def teardown_method(self):
        self.cleanup.close()

    def close_cluster(self):
        if hasattr(self, "cluster"):
            super().teardown_method()

    def query(self, pile, query, scheme=False, timeout=120):
        def attempt():
            node = self.pile_nodes[pile][0]
            driver = ydb.Driver(ydb.DriverConfig(
                endpoint="grpc://" + node.endpoint, database="/Root", disable_discovery=True))
            pool = ydb.SessionPool(driver, size=1)
            try:
                def operation(session):
                    settings = ydb.BaseRequestSettings().with_timeout(10)
                    if scheme:
                        session.execute_scheme(query, settings=settings)
                        return True
                    return session.transaction(ydb.SerializableReadWrite()).execute(
                        query, commit_tx=True, settings=settings)
                try:
                    return pool.retry_operation_sync(operation, retry_settings=ydb.RetrySettings(
                        max_retries=0, get_session_client_timeout=5)) or True
                except (grpc.RpcError, ydb.Error, ydb.SessionPoolEmpty) as error:
                    raise AssertionError(str(error)) from error
            finally:
                pool.stop(timeout=5)
                self.stop_driver(driver)
        return eventually(attempt, "table operation through %s" % pile, timeout)

    def create_data(self):
        self.query("r1", "CREATE TABLE `/Root/p0_data` (id Uint64, value Utf8, PRIMARY KEY(id));", scheme=True)
        values = ",".join('(%du,"%s")' % (key, value) for key, value in self.rows.items())
        self.query("r1", "UPSERT INTO `/Root/p0_data` (id,value) VALUES " + values)
        self.check_data("r1")

    def check_data(self, pile):
        result = self.query(pile, "SELECT id,value FROM `/Root/p0_data` ORDER BY id")
        assert {row.id: row.value for row in result[0].rows} == self.rows

    def write_after_fault(self, pile):
        self.query(pile, 'UPSERT INTO `/Root/p0_data` (id,value) VALUES (999u,"after-failover")')
        self.rows[999] = "after-failover"
        self.check_data(pile)

    def start_skipper(self):
        binary = yatest.common.binary_path("ydb/apps/bridge_skipper/bridge_skipper")
        self.skipper = Skipper(binary, self.output / "controller", self.pile_nodes, self.ports.get_port())
        self.cleanup.callback(self.skipper.close)
        self.wait_snapshot(lambda s: len(s["cards"]) == len(self.pile_names)
                           and all(card["verdict"] == "HEALTHY" for card in s["cards"]),
                           "BSC warm-up", timeout=150)
        return self.skipper.state()

    def wait_snapshot(self, predicate, description, timeout=150):
        def check():
            state = self.skipper.state()
            return state if predicate(state) else None
        return eventually(check, description, timeout)

    def guarded_update(self, changes, via="r1"):
        node = self.pile_nodes[via][0]
        before = eventually(lambda: state_rpc(node, connectivity=False), "preparation generation", timeout=60)
        request = bridge.UpdateClusterStateRequest(expected_generation=before.generation)
        request.operation_params.operation_timeout.seconds = 25
        request.updates.extend(PileState(pile_name=name, state=state) for name, state in changes.items())
        completed = {PileState.SUSPENDED: DISCONNECTED, UNSYNC: SYNC}
        last_sent = None

        def observed():
            nonlocal last_sent
            current = state_rpc(node, connectivity=False)
            actual = {p.pile_name: p.state for p in current.pile_states}
            if current.generation > before.generation:
                assert all(actual.get(name) in (state, completed.get(state, state))
                           for name, state in changes.items()), current
                return current
            if current.generation != before.generation:
                return None
            if last_sent is not None and time.monotonic() - last_sent < 5:
                return None
            # Every retry retains the original guard. If an acknowledgement
            # was lost, a resend cannot restart Rejoin in a newer generation.
            try:
                with grpc.insecure_channel(node.endpoint) as channel:
                    response = bridge_grpc.BridgeServiceStub(channel).UpdateClusterState(
                        request, metadata=TOKEN, timeout=27)
                if response.operation.status in (StatusIds.BAD_REQUEST, StatusIds.UNAUTHORIZED):
                    raise RuntimeError(str(response))
                self.logger.debug("Preparation request: %s; response: %s", request, response)
            except grpc.RpcError as error:
                self.logger.debug("Preparation request: %s; error: %s", request, error)
            last_sent = time.monotonic()
            return None

        current = eventually(observed, "observed guarded preparation state", timeout=120)
        self.logger.debug("Observed preparation state: %s", current)
        return before.generation

    def kill_pile(self, pile):
        nodes = self.pile_nodes[pile]
        with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
            list(executor.map(daemon.Daemon.kill, nodes))
        assert all(not node.is_alive() for node in nodes)

    def assert_auto_commit(self, failed, expected, generation):
        state = self.skipper.state()
        if state["decision"]["kind"] == "READY":
            plan = state["decision"]["plan"]
            assert int(plan["expected_generation"]) == generation, state
            assert plan["update"] == expected, state
        self.skipper.mode("auto")
        expected_states = {name: "DISCONNECTED" if name == failed else
                           "PRIMARY" if expected.get(name) == "PRIMARY" or name == "r1" else "SYNCHRONIZED"
                           for name in self.pile_names}
        state = self.wait_snapshot(lambda s: s["view"] and {
            p["name"]: p["state"] for p in s["view"]["piles"]} == expected_states, "committed failover")
        assert int(state["view"]["generation"]) > generation, state
        attempt = state["last_mutation"]
        assert int(attempt["expected_generation"]) == generation, attempt
        assert attempt["update"] == expected, attempt
        survivor = next(name for name, value in expected_states.items() if value == "PRIMARY")
        self.check_data(survivor)
        self.write_after_fault(survivor)


class SkipperScenarios(RealSkipperCluster):
    def test_primary_failure(self):
        self.create_data()
        before = self.start_skipper()
        generation = int(before["view"]["generation"])
        self.kill_pile("r1")
        self.assert_auto_commit("r1", {"r1": "DISCONNECTED", "r2": "PRIMARY"}, generation)

    def test_stage2_failure(self):
        self.create_data()
        self.start_skipper()
        self.guarded_update({"r2": DISCONNECTED})
        eventually(lambda: next(p.state for p in state_rpc(self.pile_nodes["r1"][0]).pile_states
                                if p.pile_name == "r2") == DISCONNECTED, "preparation disconnect")
        new_rows = {key: "y" * 4096 for key in range(1000, 1064)}
        values = ",".join('(%du,"%s")' % (key, value) for key, value in new_rows.items())
        self.query("r1", "UPSERT INTO `/Root/p0_data` (id,value) VALUES " + values)
        self.rows.update(new_rows)
        self.guarded_update({"r2": UNSYNC})

        def stage2():
            result = state_rpc(self.pile_nodes["r1"][0])
            states = {p.pile_name: p.state for p in result.pile_states}
            return result if (states.get("r2") == UNSYNC
                              and result.pile_connectivity.generation == result.generation and any(
                p.pile_name == "r2" and p.in_config_quorum and p.in_data_quorum
                for p in result.pile_connectivity.piles)) else None
        before = eventually(stage2, "real NOT_SYNCHRONIZED stage-2 participation", timeout=180)
        self.wait_snapshot(lambda s: s["view"] and int(s["view"]["generation"]) == before.generation,
                           "Skipper observes stage 2")
        self.kill_pile("r2")
        self.assert_auto_commit("r2", {"r2": "DISCONNECTED"}, before.generation)


class TestSkipperTwoPiles(SkipperScenarios):
    pile_names = ("r1", "r2")
    nodes_count = 16


class TestSkipperThreePiles(SkipperScenarios):
    pile_names = ("r1", "r2", "r3")
    nodes_count = 24


class TestSkipperSuspended(RealSkipperCluster):
    pile_names = ("r1", "r2")
    nodes_count = 16

    def configure_cluster(self):
        super().configure_cluster()
        # Keep CMS on one known r2 node. Pausing it models an unavailable CMS
        # and holds the real SUSPENDED state before its automatic completion.
        self.configurator.yaml_config.setdefault("system_tablets", {})["cms"] = [{"node": [2]}]

    def test_lost_suspended_is_blocked(self):
        self.create_data()
        self.start_skipper()
        cms = self.cluster.nodes[2]
        assert cms in self.pile_nodes["r2"]
        cms.freeze()
        self.cleanup.callback(lambda: cms.unfreeze() if cms.is_alive() else None)
        self.guarded_update({"r2": PileState.SUSPENDED})
        before = self.wait_snapshot(lambda s: s["view"] and any(
            p["name"] == "r2" and p["state"] == "SUSPENDED" for p in s["view"]["piles"]),
            "real SUSPENDED state while CMS is paused")
        self.skipper.mode("auto")
        self.kill_pile("r2")
        blocked = self.wait_snapshot(lambda s: s["decision"]["blocker"] == "SUSPENDED_PILE_LOST",
                                     "lost SUSPENDED blocker")
        assert blocked["last_mutation"] is None, blocked
        assert blocked["view"]["generation"] == before["view"]["generation"], blocked
        assert not blocked["decision"]["plan"], blocked


class TestSkipperFailoverLatency(RealSkipperCluster):
    erasure = Erasure.NONE
    pile_names = ("r1", "r2")
    nodes_count = 6
    use_in_memory_pdisks = True

    @pytest.mark.parametrize("fault", ("kill_two", "kill_all", "freeze_all"))
    def test_default_failover_commits_within_20_seconds(self, fault):
        self.create_data()
        before = self.start_skipper()
        self.skipper.mode("auto")
        generation = int(before["view"]["generation"])
        affected = self.pile_nodes["r1"][:2] if fault == "kill_two" else self.pile_nodes["r1"]
        if fault == "freeze_all":
            for node in affected:
                self.cleanup.callback(lambda node=node: node.unfreeze() if node.is_alive() else None)
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=len(affected)) as executor:
            list(executor.map(lambda node: node.freeze() if fault == "freeze_all" else daemon.Daemon.kill(node), affected))
        expected = {"r1": "DISCONNECTED", "r2": "PRIMARY"}
        state = self.wait_snapshot(
            lambda s: s["view"] and {pile["name"]: pile["state"] for pile in s["view"]["piles"]} == expected,
            "default automatic failover within 20 seconds", timeout=max(0, 20 - (time.monotonic() - started)))
        elapsed = time.monotonic() - started
        assert elapsed <= 20, (elapsed, state)
        self.logger.info("Skipper committed failover after %.3f seconds (%s)", elapsed, fault)
        attempt = state["last_mutation"]
        assert int(attempt["expected_generation"]) == generation, attempt
        assert attempt["update"] == expected, attempt
        self.check_data("r2")
        self.write_after_fault("r2")
        # A completed failover must not produce another update on later polls.
        time.sleep(3)
        records = [json.loads(line) for line in (self.skipper.directory / "skipper.log").read_text().splitlines()]
        sends = [record for record in records if record.get("event") == "mutation_sent"]
        assert len(sends) == 1, sends
        assert int(sends[0]["expected_generation"]) == generation, sends


class TestSkipperLiveStorageFailure(RealSkipperCluster):
    erasure = Erasure.NONE
    pile_names = ("r1", "r2")
    nodes_count = 6
    use_in_memory_pdisks = True

    def fail_ram_storage(self, pile):
        paths = [drive["path"] for config in self.configurator.yaml_config["host_configs"]
                 for drive in config["drive"]]
        assert self.use_in_memory_pdisks and paths and all(path.startswith("SectorMap:") for path in paths), paths

        def fail_node(node):
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            base = "http://%s:%d/actors/pdisks/" % (node.host, node.mon_port)
            try:
                with opener.open(base, timeout=5) as response:
                    index = response.read().decode()
            except urllib.error.HTTPError as error:
                if error.code == 404:
                    return []
                raise
            disks = sorted(set(re.findall(r"href=['\"](pdisk\d+)['\"]", index)))
            for disk in disks:
                with opener.open(base + disk, timeout=5) as response:
                    page = response.read().decode()
                assert "SectorMap" in page and "stopPDisk" in page, (node.node_id, disk)
                request = urllib.request.Request(base + disk, data=b"stopPDisk=", headers={
                    "Content-Type": "application/x-www-form-urlencoded"})
                with opener.open(request, timeout=10) as response:
                    assert response.status == 200, (node.node_id, disk, response.status)
                    response.read()
            return [(node.node_id, disk) for disk in disks]

        nodes = self.pile_nodes[pile]
        with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
            failed = [disk for disks in executor.map(fail_node, nodes) for disk in disks]
        assert failed, "No active RAM PDisk found in %s" % pile
        self.logger.info("Injected DeviceIoError into RAM PDisks: %s", failed)

    def check_processes(self, pids):
        assert all(node.is_alive() and node.pid == pids[node_id]
                   for node_id, node in self.cluster.nodes.items()), pids

    @pytest.mark.parametrize("prepare_fallback", (False, True), ids=("auto_before_fault", "fallback_before_auto"))
    def test_primary_storage_error_fails_over_with_live_nodes(self, prepare_fallback):
        self.create_data()
        before = self.start_skipper()
        if not prepare_fallback:
            self.skipper.mode("auto")
        generation = int(before["view"]["generation"])
        pids = {node_id: node.pid for node_id, node in self.cluster.nodes.items()}
        started = time.monotonic()
        self.fail_ram_storage("r1")
        self.check_processes(pids)
        # Storage failure does not remove node quorum. Capture the primary's
        # own report before the automatic commit changes the participating set.
        report = state_rpc(self.pile_nodes["r1"][0])
        assert report.generation == generation and report.pile_connectivity.generation == generation, report
        assert any(p.pile_name == "r1" and p.has_node_quorum for p in report.pile_connectivity.piles), report
        if prepare_fallback:
            # BSC may keep serving RED from memory after disk failure. Restart
            # just that tablet so this case must use the new fallback proof.
            # One bounded call: an uncertain poison must not be retried.
            node = self.pile_nodes["r2"][0]
            with grpc.insecure_channel(node.endpoint) as channel:
                response = tablet_grpc.TabletServiceStub(channel).RestartTablet(
                    tablet.RestartTabletRequest(tablet_id=TabletTypes.FLAT_BS_CONTROLLER.tablet_id_for(0)),
                    metadata=TOKEN, timeout=5)
            assert response.status in (StatusIds.SUCCESS, StatusIds.UNAVAILABLE), response

            def storage_failed(state):
                self.check_processes(pids)
                primary = next((pile for pile in state["piles"] if pile["name"] == "r1"), {})
                return primary.get("verdict") == "FAILED" and primary.get("self_quorum") and any(
                    "PDisk DeviceIoError" in reason for reason in primary.get("reasons", []))

            self.wait_snapshot(storage_failed, "explicit storage proof with live primary quorum",
                               timeout=max(0, 20 - (time.monotonic() - started)))
            self.skipper.mode("auto")
        expected = {"r1": "DISCONNECTED", "r2": "PRIMARY"}

        def committed(state):
            self.check_processes(pids)
            return state["view"] and {pile["name"]: pile["state"] for pile in state["view"]["piles"]} == expected

        state = self.wait_snapshot(committed, "storage failover while every node remains alive",
                                   timeout=max(0, 20 - (time.monotonic() - started)))
        elapsed = time.monotonic() - started
        assert elapsed <= 20, (elapsed, state)
        assert state["controller_boot_id"] == before["controller_boot_id"], state
        attempt = state["last_mutation"]
        assert int(attempt["expected_generation"]) == generation and attempt["update"] == expected, attempt
        self.check_data("r2")
        self.write_after_fault("r2")
        self.check_processes(pids)
        time.sleep(3)
        records = [json.loads(line) for line in (self.skipper.directory / "skipper.log").read_text().splitlines()]
        sends = [record for record in records if record.get("event") == "mutation_sent"]
        assert len(sends) == 1 and int(sends[0]["expected_generation"]) == generation, sends

    def test_storage_error_candidate_is_not_promoted(self):
        self.create_data()
        before = self.start_skipper()
        pids = {node_id: node.pid for node_id, node in self.cluster.nodes.items()}
        self.fail_ram_storage("r2")
        primary = self.pile_nodes["r1"]
        for node in primary:
            self.cleanup.callback(lambda node=node: node.unfreeze() if node.is_alive() else None)
        with ThreadPoolExecutor(max_workers=len(primary)) as executor:
            list(executor.map(lambda node: node.freeze(), primary))
        state = self.wait_snapshot(lambda s: s["decision"]["blocker"] == "NO_CANDIDATE",
                                   "damaged candidate blocks promotion", timeout=30)
        self.skipper.mode("auto")
        until = time.monotonic() + 30
        while time.monotonic() < until:
            self.check_processes(pids)
            assert state["decision"]["blocker"] == "NO_CANDIDATE", state
            assert state["last_mutation"] is None, state
            assert state["view"] and state["view"]["generation"] == before["view"]["generation"], state
            time.sleep(0.25)
            state = self.skipper.state()
