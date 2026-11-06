"""Real C++ binary/SDK against mock gRPC listeners; no production test hooks."""
import asyncio
import json
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.request

from library.python.port_manager import PortManager
from ydb.apps.bridge_skipper.ydbd_mock.lib import model, operations
from ydb.apps.bridge_skipper.ydbd_mock.lib.testkit import LiveCluster, tiny_timings
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Topology
from ydb.public.api.protos.draft import ydb_bridge_pb2 as B


BRIDGE_UPDATE = "/Ydb.Bridge.V1.BridgeService/UpdateClusterState"
MUTATIONS = {"UpdateClusterState", "AddPile", "RemovePile", "ConvertToBridge"}
STATE_SCHEMA = 2


async def wait(predicate, message, timeout=40):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if asyncio.iscoroutine(last):
            last = await last
        if last:
            return last
        await asyncio.sleep(0.05)
    raise AssertionError("timed out waiting for %s; last=%r" % (message, last))


def http_request(url, method="GET"):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(url, method=method)
    try:
        response = opener.open(request, timeout=1)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read().decode()
        try:
            body = json.loads(raw)
        except ValueError:
            body = raw
        return response.status, body, dict(response.headers)


class Controller:
    def __init__(self, binary, root, config):
        self.binary = binary
        self.root = Path(root)
        self.config = config
        self.config_path = self.root / "skipper.json"
        self.state_dir = self.root / "state"
        self.log_path = self.root / "skipper.log"
        self.url = "http://" + config["http"]["listen"]
        self.process = None
        self.output = None
        self.last_state = None
        self.config_path.write_text(json.dumps(config))

    async def start(self, env=None, ready=True):
        self.output = self.log_path.open("ab")
        self.process = subprocess.Popen(
            [self.binary, "--config", str(self.config_path), "--state-dir", str(self.state_dir)],
            env=env, stdout=self.output, stderr=subprocess.STDOUT)
        if ready:
            async def listening():
                if self.process.poll() is not None:
                    raise AssertionError("skipper exited: " + self.log_path.read_text())
                return await self.state()
            await wait(listening, "skipper HTTP listener", timeout=15)
        return self

    async def state(self):
        try:
            status, body, _ = await asyncio.to_thread(http_request, self.url + "/api/state")
        except (OSError, urllib.error.URLError):
            return None
        assert status == 200, (status, body)
        assert body["schema"] == STATE_SCHEMA, body
        self.last_state = body
        (self.root / "snapshot.json").write_text(json.dumps(body, indent=2))
        return body

    async def until(self, predicate, message, timeout=40):
        async def check():
            state = await self.state()
            return state if state and predicate(state) else None
        try:
            return await wait(check, message, timeout)
        except AssertionError as error:
            raise AssertionError("%s; snapshot=%r; log=%s" % (
                error, self.last_state, self.log_path.read_text()[-16000:])) from error

    async def stop(self, timeout=8):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                await asyncio.to_thread(self.process.wait, timeout=timeout)
            except subprocess.TimeoutExpired:
                # Stalled-child regressions must never leave a writer behind.
                self.process.kill()
                await asyncio.to_thread(self.process.wait, timeout=5)
                raise AssertionError("skipper failed to stop within %s seconds" % timeout)
        if self.output:
            self.output.close()
            self.output = None

    def persistent(self):
        return json.loads((self.state_dir / "state.json").read_text())


class Environment:
    def __init__(self, binary, root, names=("a", "b"), mode="auto", initial=None, timing=None):
        self.root = Path(root)
        self.ports = PortManager()
        topology = Topology(
            names, 3, initial=initial, timings=tiny_timings(lifecycle_delay=0.05),
            control_ports=[self.ports.get_port() for _ in names],
            grpc_ports=[[self.ports.get_port() for _ in range(3)] for _ in names])
        self.cluster = LiveCluster(topology=topology)
        # SelfCheck and Update client deadlines are their operation timeouts + 2 s
        # (GetClusterState uses read_timeout); the validator requires
        # selfcheck operation + 2 + round interval < evidence TTL.
        values = {"state_poll_interval_seconds": 1, "read_timeout_seconds": 1,
                  "round_interval_seconds": 1, "selfcheck_operation_timeout_seconds": 2,
                  "evidence_ttl_seconds": 6, "confirm_seconds": 2, "bsc_warmup_seconds": 20,
                  "mutation_operation_timeout_seconds": 3}
        values.update(timing or {})
        endpoints = {pile.name: ["grpc://" + node.endpoint for node in pile.nodes]
                     for pile in self.cluster.topo.piles}
        self.controller = Controller(binary, root, {
            "cluster_id": "acceptance", "mode": mode,
            "endpoints": endpoints, "http": {"listen": "127.0.0.1:%d" % self.ports.get_port()},
            "timing": values, "health_endpoints_per_pile": 3})

    async def start(self, skipper=True):
        await self.cluster.start()
        if skipper:
            await self.controller.start()
        return self

    async def close(self):
        try:
            try:
                await self.controller.stop()
            finally:
                (self.root / "requestlog.json").write_text(json.dumps(self.events(), indent=2))
                await self.cluster.stop()
        finally:
            self.ports.release()

    def events(self):
        return sorted([event for proc in self.cluster.procs.values()
                       for event in proc.events.query()], key=lambda event: event["ts"])

    def mutations(self, after=0):
        return [event for event in self.events() if event["kind"] == "rpc.start" and event["ts"] >= after
                and event["method"].rsplit("/", 1)[-1] in MUTATIONS]

    async def control(self, pile, path, **body):
        status, result = await self.cluster.http(pile, "POST", path, body)
        assert status == 200, (path, body, status, result)
        return result

    async def all_control(self, path, **body):
        for pile in self.cluster.topo.pile_names:
            await self.control(pile, path, **body)

    async def partition(self):
        """Block every peer link of every pile; skipper still reaches all endpoints."""
        for pile in self.cluster.topo.pile_names:
            await self.control(pile, "/link", block=[other for other in self.cluster.topo.pile_names if other != pile])

    async def blame(self, *piles):
        await self.all_control("/control", op="storage", all=True, piles=list(piles))

    async def update(self, update, source="a"):
        request = B.UpdateClusterStateRequest()
        for name, state in update.items():
            request.updates.add(pile_name=name, state=model.public_value(state))
        response = await self.cluster.bridge(self.cluster.label(source)).UpdateClusterState(request, timeout=5)
        assert response.operation.status == model.SUCCESS, operations.issue_texts(response.operation)

    async def healthy(self):
        return await self.controller.until(lambda state: state["piles"] and all(
            pile["verdict"] == "HEALTHY" for pile in state["piles"]), "BSC warmup", timeout=30)

    async def generation(self):
        state = await self.controller.until(lambda state: state["view"] is not None, "selected view")
        return int(state["view"]["generation"])

    async def mutation(self, generation, update, count=1, after=0):
        events = await wait(lambda: self.mutations(after) if len(self.mutations(after)) >= count else None,
                            "guarded mutation")
        # G comes from the decision/view BEFORE the send. A post-commit view
        # already at G+1 cannot establish the guard used for this invocation.
        for event in events:
            assert_mutation(event, generation, update)
        return events

    async def quiet(self, seconds):
        before = self.mutations()
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            assert self.mutations() == before, self.mutations()
            state = await self.controller.state()
            assert state and state["loop_age_ms"] < 1000, state
            await asyncio.sleep(0.05)


def assert_mutation(event, generation, update):
    assert event["method"] == BRIDGE_UPDATE, event
    match = re.fullmatch(r"updates=\[(.*)\] quorum=\[\] expected_generation=(\d+)", event["summary"])
    assert match, event
    observed = dict(item.split("=", 1) for item in match[1].split(", "))
    assert observed == update, event
    assert int(match[2]) == generation, ("pre-send generation", generation, event)
