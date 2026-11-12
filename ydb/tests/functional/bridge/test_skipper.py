"""Automatic Skipper failover against real YDB bridge clusters."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import subprocess
from threading import Event, Lock
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import grpc
import pytest
import yatest.common
import ydb
from library.python.port_manager import PortManager

from common import BridgeKiKiMRTest
from ydb.core.protos.blobstorage_disk_pb2 import TPDiskState
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
    def __init__(self, binary, directory, piles, port, *, timing=None, health_endpoints_per_pile=2):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.url = "http://127.0.0.1:%d" % port
        token = self.directory / "token"
        token.write_text("root@builtin\n")
        token.chmod(0o600)
        config = dict(cluster_id="functional-test", endpoints={
            name: ["grpc://" + node.endpoint for node in nodes] for name, nodes in piles.items()},
            auth=dict(token_file=str(token)), http=dict(listen="127.0.0.1:%d" % port),
            timing=timing or {}, health_endpoints_per_pile=health_endpoints_per_pile)
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

    def mode(self, mode, *, deadline=None):
        def timeout(limit):
            if deadline is None:
                return limit
            remaining = deadline - time.monotonic()
            assert remaining > 0, ("No time remains to set controller mode", mode)
            return min(limit, remaining)

        for _ in range(20):
            if deadline is None:
                state = self.state()
            else:
                assert self.process.poll() is None, "Skipper exited; see %s" % self.directory
                status, state = http(self.url + "/api/state", timeout=timeout(1))
                assert status == 200, (status, state)
            body = dict(mode=mode, expected_boot_id=state["controller_boot_id"],
                        expected_mode_revision=state["mode_revision"], request_id=uuid.uuid4().hex)
            if mode == "auto":
                body["reviewed_snapshot_revision"] = state["snapshot_revision"]
            status, result = http(self.url + "/api/mode", body, timeout=timeout(18 if deadline is None else 2))
            if status == 409 and result.get("error") == "SNAPSHOT_CHANGED":
                # A definite refusal consumes this revision. Never repeat an uncertain POST.
                time.sleep(timeout(0.25))
                continue
            assert status == 200, (status, result)
            assert deadline is None or time.monotonic() < deadline, (status, result)
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


class LoadedWrites:
    """Continuous independent writers, retaining every acknowledged key and value."""

    def __init__(self, test, workers=4, batch_size=1, rate=80):
        self.test = test
        test.query("r2", "CREATE TABLE `/Root/rto_load` (id Uint64, value Utf8, PRIMARY KEY(id));", scheme=True)
        self.driver = ydb.Driver(ydb.DriverConfig(
            endpoint="grpc://" + test.pile_nodes["r2"][0].endpoint, database="/Root", disable_discovery=True))
        test.cleanup.callback(test.stop_driver, self.driver)
        self.driver.wait(timeout=15)
        self.stop = Event()
        self.lock = Lock()
        self.workers = workers
        self.batch_size = batch_size
        self.period = workers / rate
        self.acknowledgements = {}
        self.successful = []
        self.worker_counts = [0] * self.workers
        self.last_success = [None] * self.workers
        self.last_errors = [None] * self.workers
        self.futures = []

    @staticmethod
    def settings():
        return (ydb.BaseRequestSettings().with_operation_timeout(1.5)
                .with_cancel_after(1.5).with_timeout(2))

    def write(self, worker):
        suffixes = [""] if self.batch_size == 1 else [str(index) for index in range(self.batch_size)]
        statement = "".join("DECLARE $id%s AS Uint64; DECLARE $value%s AS Utf8; " % (suffix, suffix)
                            for suffix in suffixes)
        statement += "UPSERT INTO `/Root/rto_load` (id,value) VALUES " + ",".join(
            "($id%s,$value%s)" % (suffix, suffix) for suffix in suffixes) + ";"
        session = query = None
        serial = 0
        while not self.stop.is_set():
            started = time.monotonic()
            try:
                if session is None:
                    session = self.driver.table_client.session().create(settings=self.settings())
                if query is None:
                    query = session.prepare(statement, settings=self.settings())
                rows = {}
                parameters = {}
                for suffix in suffixes:
                    serial += 1
                    key = (worker + 1) * 1_000_000_000 + serial
                    value = "writer-%d-row-%d-%s" % (worker, serial, "x" * 256)
                    rows[key] = value
                    parameters["$id" + suffix] = key
                    parameters["$value" + suffix] = value
                sent = time.monotonic()
                session.transaction(ydb.SerializableReadWrite()).execute(
                    query, parameters, commit_tx=True, settings=self.settings())
                finished = time.monotonic()
                with self.lock:
                    self.acknowledgements.update(rows)
                    self.successful.append((sent, finished))
                    self.worker_counts[worker] += 1
                    self.last_success[worker] = finished
            except (ydb.Error, grpc.RpcError) as error:
                with self.lock:
                    self.last_errors[worker] = str(error)[:500]
                if isinstance(error, (ydb.BadSession, ydb.SessionExpired, ydb.SessionBusy)):
                    session = query = None
            # Distribute the requested transaction rate across all workers.
            self.stop.wait(max(0, self.period - (time.monotonic() - started)))

    def ready(self, require_fresh=False):
        for future in self.futures:
            if future.done():
                future.result()
                raise AssertionError("Load writer stopped unexpectedly")
        with self.lock:
            now = time.monotonic()
            return (all(count >= 4 for count in self.worker_counts)
                    and (not require_fresh or all(at is not None and now - at <= 2 for at in self.last_success)))

    def first_ack_after(self, boundary):
        self.ready()
        with self.lock:
            return min((record for record in self.successful if record[0] >= boundary),
                       key=lambda record: record[1], default=None)

    def __enter__(self):
        self.executor = ThreadPoolExecutor(max_workers=self.workers)
        self.futures.extend(self.executor.submit(self.write, worker) for worker in range(self.workers))
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.executor.shutdown(wait=True)
        for future in self.futures:
            future.result()

    def verify(self):
        assert all(self.worker_counts), self.worker_counts
        keys = sorted(self.acknowledgements)
        for offset in range(0, len(keys), 500):
            expected = {key: self.acknowledgements[key] for key in keys[offset:offset + 500]}
            result = self.test.query("r2", "SELECT id,value FROM `/Root/rto_load` WHERE id IN (%s)" %
                                     ",".join("%dul" % key for key in expected))
            assert {row.id: row.value for row in result[0].rows} == expected
        self.test.logger.info("Verified all %d acknowledged load rows", len(keys))


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

    def start_skipper(self, piles=None, *, timing=None, health_endpoints_per_pile=2):
        binary = yatest.common.binary_path("ydb/apps/bridge_skipper/bridge_skipper")
        endpoints = self.pile_nodes if piles is None else {name: self.pile_nodes[name] for name in piles}
        self.skipper = Skipper(binary, self.output / "controller", endpoints, self.ports.get_port(),
                               timing=timing, health_endpoints_per_pile=health_endpoints_per_pile)
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

    def test_default_failover_restores_loaded_writes_within_20_seconds(self):
        self.create_data()
        with LoadedWrites(self) as load:
            eventually(load.ready, "writes from every load worker", timeout=15)
            # The load stays active throughout Skipper's healthy warm-up and the fault.
            before = self.start_skipper()
            self.skipper.mode("auto")
            generation = int(before["view"]["generation"])
            primary = self.pile_nodes["r1"]
            for node in primary:
                self.cleanup.callback(lambda node=node: node.unfreeze() if node.is_alive() else None)
            eventually(lambda: load.ready(require_fresh=True), "fresh writes from every worker before the fault", timeout=5)
            started = time.monotonic()
            with ThreadPoolExecutor(max_workers=len(primary)) as freezer:
                list(freezer.map(lambda node: node.freeze(), primary))
            expected = {"r1": "DISCONNECTED", "r2": "PRIMARY"}
            state = self.wait_snapshot(
                lambda state: state["view"] and {
                    pile["name"]: pile["state"] for pile in state["view"]["piles"]} == expected,
                "committed failover under write load", timeout=60)
            committed = time.monotonic()
            attempt = state["last_mutation"]
            assert int(state["view"]["generation"]) > generation, state
            assert int(attempt["expected_generation"]) == generation, attempt
            assert attempt["update"] == expected, attempt
            # Only a newly sent, post-commit write proves recovery; an old in-flight
            # success during fault propagation cannot satisfy this check. Allow
            # recovery before asserting its timestamp so an RTO regression still
            # verifies every acknowledged row on the surviving pile.
            elapsed = None
            recovery_error = None
            try:
                first = eventually(lambda: load.first_ack_after(committed), "acknowledged post-failover write",
                                   timeout=max(0, 60 - (time.monotonic() - started)))
                elapsed = first[1] - started
                self.logger.info("Skipper restored loaded writes after %.3f seconds", elapsed)
            except AssertionError as error:
                recovery_error = error
            # Keep observing after recovery to catch a repeated mutation.
            time.sleep(3)

        load.verify()
        self.check_data("r2")
        self.write_after_fault("r2")
        records = [json.loads(line) for line in (self.skipper.directory / "skipper.log").read_text().splitlines()]
        sends = [record for record in records if record.get("event") == "mutation_sent"]
        assert len(sends) == 1, sends
        assert int(sends[0]["expected_generation"]) == generation, sends
        assert recovery_error is None, recovery_error
        assert elapsed <= 20, (elapsed, load.last_errors, state)


class ControlInputs(HTMLParser):
    def __init__(self, page):
        super().__init__()
        self.values = {}
        self.feed(page)

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag.lower() == "input" and attrs.get("name") and "value" in attrs:
            self.values[attrs["name"]] = attrs["value"]


class TestSkipperSilentIoHang(RealSkipperCluster):
    erasure = Erasure.NONE
    pile_names = ("r1", "r2")
    nodes_count = 6
    use_in_memory_pdisks = True

    def configure_cluster(self):
        super().configure_cluster()
        # NVME gives each RAM device the supported run-time seek-delay control.
        # Configure it before startup; this scenario never opens a real drive.
        for config in self.configurator.yaml_config["host_configs"]:
            for drive in config["drive"]:
                path = drive["path"]
                assert path.startswith("SectorMap:") and len(path.split(":")) == 3, path
                drive["path"] = path + ":NVME"

    @staticmethod
    def monitoring(node, path, values=None):
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        request = urllib.request.Request(
            "http://%s:%d%s" % (node.host, node.mon_port, path),
            data=None if values is None else urllib.parse.urlencode(values).encode(),
            headers={} if values is None else {"Content-Type": "application/x-www-form-urlencoded"})
        with opener.open(request, timeout=3) as response:
            assert response.status == 200, (node.node_id, path, response.status)
            return response.read().decode()

    def prepare_delays(self, pile):
        def prepare(node):
            fields = ControlInputs(self.monitoring(node, "/actors/icb")).values
            index = self.monitoring(node, "/actors/pdisks/")
            live = sorted(set(re.findall(r"href=['\"](pdisk\d+)['\"]", index)))
            assert live, (node.node_id, index)
            disks = []
            # ICB retains retired PDisk controls. Only the live index identifies
            # actors whose pages and RAM backing can be checked before mutation.
            for disk in live:
                pdisk = int(disk.removeprefix("pdisk"))
                control = "PDisk_%d_SectorMapSeekSleepMicroSeconds" % pdisk
                page = self.monitoring(node, "/actors/pdisks/" + disk)
                assert "SectorMap" in page and "stopPDisk" in page, (node.node_id, disk)
                assert control in fields, (node.node_id, control)
                old_value = int(fields[control])
                disks.append((node, pdisk, control, old_value))
            return disks

        nodes = self.pile_nodes[pile]
        with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
            return [disk for disks in executor.map(prepare, nodes) for disk in disks]

    def set_delay(self, disk, microseconds):
        node, _, control, _ = disk
        fields = ControlInputs(self.monitoring(node, "/actors/icb", {control: microseconds})).values
        assert int(fields[control]) == microseconds, (node.node_id, control, fields.get(control))

    def restore_delays(self, disks):
        def restore(disk):
            try:
                self.set_delay(disk, disk[3])
            except (AssertionError, OSError, ValueError) as error:
                return disk, str(error)
            return None

        if disks:
            with ThreadPoolExecutor(max_workers=len(disks)) as executor:
                failed = [result for result in executor.map(restore, disks) if result is not None]
            disks[:] = [disk for disk, _ in failed]
            assert not failed, failed

    def device_snapshot(self, disks):
        names = {"DeviceTakeoffs", "DeviceLandings", "DeviceIoErrors", "PDiskState"}
        nodes = {disk[0].node_id: disk[0] for disk in disks}

        def sample(node):
            data = json.loads(self.monitoring(node, "/counters/json"))
            counters = {}
            for sensor in data.get("sensors", []):
                labels = sensor.get("labels", {})
                if labels.get("counters") == "pdisks" and labels.get("sensor") in names and "pdisk" in labels:
                    counters.setdefault(int(labels["pdisk"]), {})[labels["sensor"]] = sensor["value"]
            return node.node_id, counters

        with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
            by_node = dict(executor.map(sample, nodes.values()))
        result = {}
        for node, pdisk, _, _ in disks:
            values = by_node[node.node_id].get(pdisk, {})
            assert values.keys() >= names, (node.node_id, pdisk, values)
            assert values["DeviceIoErrors"] == 0 and values["PDiskState"] == TPDiskState.Normal, (
                node.node_id, pdisk, values)
            result[node.node_id, pdisk] = values
        return result

    @staticmethod
    def update_stall_proof(samples, flat_since, sampled):
        stalled = {}
        for disk, values in samples.items():
            landings = values["DeviceLandings"]
            if values["DeviceTakeoffs"] > landings:
                previous, since = flat_since.get(disk, (None, sampled))
                if previous != landings:
                    since = sampled
                flat_since[disk] = landings, since
                if sampled - since >= 2:
                    stalled[disk] = landings
            else:
                flat_since.pop(disk, None)
        return stalled

    def check_processes(self, pids):
        assert all(node.is_alive() and node.pid == pids[node_id]
                   for node_id, node in self.cluster.nodes.items()), pids

    @pytest.mark.parametrize("workers,batch_size,rate", ((4, 1, 80), (32, 4, 400)), ids=("light", "loaded"))
    def test_primary_silent_io_hang_restores_loaded_writes_within_20_seconds(self, workers, batch_size, rate):
        self.create_data()
        with LoadedWrites(self, workers=workers, batch_size=batch_size, rate=rate) as load:
            eventually(load.ready, "writes from every load worker", timeout=15)
            before = self.start_skipper()
            self.skipper.mode("auto")
            generation = int(before["view"]["generation"])
            pids = {node_id: node.pid for node_id, node in self.cluster.nodes.items()}
            disks = self.prepare_delays("r1")
            self.cleanup.callback(self.restore_delays, disks)
            self.device_snapshot(disks)
            eventually(lambda: load.ready(require_fresh=True), "fresh writes before the silent I/O hang", timeout=5)
            expected = {"r1": "DISCONNECTED", "r2": "PRIMARY"}
            state = before
            committed = elapsed = None
            outcome_error = None
            flat_since = {}
            stalled = set()
            quorum_during_hang = False
            started = time.monotonic()
            try:
                # Bound the real backend sleep below the unchanged 60-second
                # terminal watchdog. Restoring the control cannot wake I/O that
                # has already started sleeping, so cleanup also waits for recovery.
                with ThreadPoolExecutor(max_workers=len(disks)) as executor:
                    list(executor.map(lambda disk: self.set_delay(disk, 40_000_000), disks))
                self.check_processes(pids)
                while time.monotonic() - started < 20:
                    self.check_processes(pids)
                    current = self.device_snapshot(disks)
                    sampled = time.monotonic()
                    stalled.update(self.update_stall_proof(current, flat_since, sampled))
                    if stalled and not quorum_during_hang:
                        report = state_rpc(self.pile_nodes["r1"][0])
                        assert report.generation == generation and report.pile_connectivity.generation == generation, report
                        assert any(pile.pile_name == "r1" and pile.has_node_quorum
                                   for pile in report.pile_connectivity.piles), report
                        quorum_during_hang = True
                        self.logger.info("Silent I/O proof with live primary quorum: %s",
                                         {disk: current[disk] for disk in stalled})
                    state = self.skipper.state()
                    if state["view"] and {pile["name"]: pile["state"] for pile in state["view"]["piles"]} == expected:
                        committed = time.monotonic()
                        break
                    time.sleep(0.25)
                assert stalled, ("No outstanding device I/O with flat completions", flat_since, current)
                assert quorum_during_hang, "No primary node quorum observed during the silent I/O hang"
                assert committed is not None, ("No automatic failover within 20 seconds", state)
                assert state["controller_boot_id"] == before["controller_boot_id"], state
                attempt = state["last_mutation"]
                assert int(state["view"]["generation"]) > generation, state
                assert int(attempt["expected_generation"]) == generation and attempt["update"] == expected, attempt
                first = eventually(lambda: load.first_ack_after(committed), "new post-commit write acknowledgement",
                                   timeout=max(0, 20 - (time.monotonic() - started)))
                elapsed = first[1] - started
                assert elapsed <= 20, (elapsed, load.last_errors, state)
                self.logger.info("Skipper restored loaded writes after a silent I/O hang in %.3f seconds", elapsed)
                time.sleep(3)
            except AssertionError as error:
                # Finish the data-loss checks on a timing/detection regression too.
                # Observe prevents a late automatic mutation while the bounded
                # backend delay naturally expires; no manual failover is used.
                outcome_error = error
                self.skipper.mode("observe")
            finally:
                self.restore_delays(disks)
            self.check_processes(pids)
            if outcome_error is not None:
                restored = time.monotonic()
                eventually(lambda: load.first_ack_after(restored), "writes after restoring RAM device controls", timeout=50)

        load.verify()
        self.check_data("r2")
        self.write_after_fault("r2")
        self.check_processes(pids)
        assert outcome_error is None, outcome_error
        records = [json.loads(line) for line in (self.skipper.directory / "skipper.log").read_text().splitlines()]
        sends = [record for record in records if record.get("event") == "mutation_sent"]
        assert len(sends) == 1 and int(sends[0]["expected_generation"]) == generation, sends

    def check_no_mutation(self, state, generation, pids):
        self.check_processes(pids)
        assert state["last_mutation"] is None, state
        assert state["view"] and int(state["view"]["generation"]) == generation, state
        assert {pile["name"]: pile["state"] for pile in state["view"]["piles"]} == {
            "r1": "PRIMARY", "r2": "SYNCHRONIZED"}, state

    def check_no_mutation_sent(self):
        records = [json.loads(line) for line in (self.skipper.directory / "skipper.log").read_text().splitlines()]
        assert not [record for record in records if record.get("event") == "mutation_sent"], records

    def set_mode_before(self, mode, deadline):
        try:
            self.skipper.mode(mode, deadline=deadline)
        except BaseException:
            # A late or uncertain mode POST must not leave Auto running when
            # the candidate recovers. Only the external controller is stopped.
            if self.skipper.process.poll() is None:
                self.skipper.process.kill()
                self.skipper.process.wait(timeout=5)
            raise

    def test_progressing_slow_io_does_not_trigger_failover(self):
        self.create_data()
        with LoadedWrites(self) as load:
            eventually(load.ready, "writes from every load worker", timeout=15)
            before = self.start_skipper()
            self.skipper.mode("auto")
            generation = int(before["view"]["generation"])
            pids = {node_id: node.pid for node_id, node in self.cluster.nodes.items()}
            disks = self.prepare_delays("r1")
            self.cleanup.callback(self.restore_delays, disks)
            baseline = self.device_snapshot(disks)
            eventually(lambda: load.ready(require_fresh=True), "fresh writes before slowing RAM devices", timeout=5)
            issued = set()
            pending = set()
            delayed = set()
            completions = {disk: 0 for disk in baseline}
            last_progress = {}
            previous = baseline
            outcome_error = None
            started = time.monotonic()
            try:
                with ThreadPoolExecutor(max_workers=len(disks)) as executor:
                    list(executor.map(lambda disk: self.set_delay(disk, 2_000_000), disks))
                report = state_rpc(self.pile_nodes["r1"][0])
                assert report.generation == generation and report.pile_connectivity.generation == generation, report
                assert any(pile.pile_name == "r1" and pile.has_node_quorum
                           for pile in report.pile_connectivity.piles), report
                while time.monotonic() - started < 20:
                    current = self.device_snapshot(disks)
                    sampled = time.monotonic()
                    for disk, values in current.items():
                        assert values["DeviceTakeoffs"] >= previous[disk]["DeviceTakeoffs"], (disk, values)
                        assert values["DeviceLandings"] >= previous[disk]["DeviceLandings"], (disk, values)
                        if values["DeviceTakeoffs"] > baseline[disk]["DeviceTakeoffs"]:
                            issued.add(disk)
                        if values["DeviceLandings"] > previous[disk]["DeviceLandings"]:
                            completions[disk] += 1
                            last_progress[disk] = sampled
                        if values["DeviceTakeoffs"] > values["DeviceLandings"]:
                            pending.add(disk)
                            last_progress.setdefault(disk, started)
                            if sampled - last_progress[disk] >= 1:
                                delayed.add(disk)
                            assert sampled - last_progress[disk] < 10, (disk, previous[disk], values)
                        else:
                            # An idle disk has no pending request to retire.
                            last_progress[disk] = sampled
                    previous = current
                    self.check_no_mutation(self.skipper.state(), generation, pids)
                    time.sleep(0.25)
                # Client deadlines may expire under this delay. Physical device
                # retirement, rather than success of every write, is the signal.
                assert any(disk in pending and disk in delayed and completions[disk] >= 3 for disk in issued), (
                    issued, pending, delayed, completions)
                self.logger.info("Slow RAM I/O continued completing without failover: %s", completions)
            except AssertionError as error:
                outcome_error = error
            finally:
                try:
                    self.skipper.mode("observe")
                except BaseException:
                    self.skipper.close()
                    raise
                finally:
                    self.restore_delays(disks)
            restored = time.monotonic()
            eventually(lambda: load.first_ack_after(restored), "writes after restoring progressing RAM devices", timeout=15)

        load.verify()
        self.check_data("r2")
        self.write_after_fault("r2")
        self.check_processes(pids)
        assert outcome_error is None, outcome_error
        self.check_no_mutation_sent()

    def test_stalled_candidate_is_not_promoted(self):
        self.create_data()
        with LoadedWrites(self) as load:
            eventually(load.ready, "writes from every load worker", timeout=15)
            before = self.start_skipper()
            generation = int(before["view"]["generation"])
            pids = {node_id: node.pid for node_id, node in self.cluster.nodes.items()}
            disks = self.prepare_delays("r2")
            self.cleanup.callback(self.restore_delays, disks)
            self.device_snapshot(disks)
            eventually(lambda: load.ready(require_fresh=True), "fresh writes before stalling the candidate", timeout=5)
            primary = self.pile_nodes["r1"]
            for node in primary:
                self.cleanup.callback(lambda node=node: node.unfreeze() if node.is_alive() else None)
            flat_since = {}
            stalled = {}
            outcome_error = None
            started = time.monotonic()
            try:
                # Keep Observe while establishing that the only candidate's
                # live devices have pending work and no completions.
                with ThreadPoolExecutor(max_workers=len(disks)) as executor:
                    list(executor.map(lambda disk: self.set_delay(disk, 40_000_000), disks))
                while not stalled and time.monotonic() - started < 8:
                    current = self.device_snapshot(disks)
                    sampled = time.monotonic()
                    stalled.update(self.update_stall_proof(current, flat_since, sampled))
                    self.check_no_mutation(self.skipper.state(), generation, pids)
                    if not stalled:
                        time.sleep(0.25)
                assert stalled, ("No candidate device I/O stall observed", flat_since)
                report = state_rpc(self.pile_nodes["r2"][0])
                assert report.generation == generation and report.pile_connectivity.generation == generation, report
                assert any(pile.pile_name == "r2" and pile.has_node_quorum
                           for pile in report.pile_connectivity.piles), report
                self.logger.info("Candidate RAM I/O is stalled with live node quorum: %s", stalled)
                with ThreadPoolExecutor(max_workers=len(primary)) as executor:
                    list(executor.map(lambda node: node.freeze(), primary))

                def no_candidate(state):
                    self.check_no_mutation(state, generation, pids)
                    candidate = next(pile for pile in state["piles"] if pile["name"] == "r2")
                    return (state["decision"]["blocker"] == "NO_CANDIDATE"
                            and (candidate["verdict"] == "FAILED" or candidate["fallback_red"]))

                self.wait_snapshot(no_candidate, "stalled candidate blocks promotion before Auto",
                                   timeout=max(0, started + 24 - time.monotonic()))
                self.set_mode_before("auto", started + 24)
                observing = time.monotonic()
                assert observing + 10 < started + 35, "No bounded observation window remains"
                while time.monotonic() - observing < 10:
                    assert time.monotonic() < started + 35, "Candidate observation exceeded the bounded I/O delay"
                    state = self.skipper.state()
                    assert no_candidate(state), state
                    current = self.device_snapshot(disks)
                    assert any(current[disk]["DeviceTakeoffs"] > landings
                               and current[disk]["DeviceLandings"] == landings
                               for disk, landings in stalled.items()), current
                    time.sleep(0.25)
                assert time.monotonic() < started + 35, "Candidate observation exceeded the bounded I/O delay"
            except AssertionError as error:
                outcome_error = error
            finally:
                # Disable dispatch before the candidate recovers or the primary
                # resumes. SIGSTOP preserves every node process and PID.
                try:
                    self.set_mode_before("observe", started + 38)
                finally:
                    try:
                        self.restore_delays(disks)
                    finally:
                        with ThreadPoolExecutor(max_workers=len(primary)) as executor:
                            list(executor.map(lambda node: node.unfreeze() if node.is_alive() else None, primary))
            self.check_processes(pids)
            restored = time.monotonic()
            eventually(lambda: load.first_ack_after(restored), "writes after restoring the candidate and primary", timeout=50)

        load.verify()
        self.check_data("r2")
        self.write_after_fault("r2")
        self.check_processes(pids)
        assert outcome_error is None, outcome_error
        self.check_no_mutation_sent()


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
        auto_log_offset = len((self.skipper.directory / "skipper.log").read_text().splitlines())
        self.skipper.mode("auto")
        until = time.monotonic() + 30
        while time.monotonic() < until:
            self.check_processes(pids)
            # Fresh observations can reset confirmation and temporarily
            # replace NO_CANDIDATE with CONFIRMING or UNKNOWN. Neither a ready
            # plan nor a mutation is safe while the replacement is damaged.
            assert state["decision"]["kind"] != "READY", state
            assert state["decision"]["plan"] is None, state
            assert state["last_mutation"] is None, state
            assert state["view"] and state["view"]["generation"] == before["view"]["generation"], state
            time.sleep(0.25)
            state = self.skipper.state()
        records = [json.loads(line) for line in (self.skipper.directory / "skipper.log").read_text().splitlines()]
        assert not [record for record in records if record.get("event") == "mutation_sent"], records
        assert not [record for record in records[auto_log_offset:]
                    if record.get("event") == "decision" and record.get("kind") == "READY"], records
