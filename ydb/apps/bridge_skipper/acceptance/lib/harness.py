"""Real C++ binary/SDK against mock gRPC listeners; no production test hooks."""
import asyncio
import json
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.request
import uuid

from library.python.port_manager import PortManager
from ydb.apps.bridge_skipper.ydbd_mock.lib import model, operations
from ydb.apps.bridge_skipper.ydbd_mock.lib.testkit import LiveCluster, tiny_timings
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Topology
from ydb.public.api.protos.draft import ydb_bridge_pb2 as B


BRIDGE_UPDATE = "/Ydb.Bridge.V1.BridgeService/UpdateClusterState"
MUTATIONS = {"UpdateClusterState", "AddPile", "RemovePile", "ConvertToBridge"}
STATE_SCHEMA = 3

# Fetch rejects these HTTP(S) ports even when a listener can bind them.
# https://fetch.spec.whatwg.org/#port-blocking
FETCH_BAD_PORTS = frozenset({
    0, 1, 7, 9, 11, 13, 15, 17, 19, 20, 21, 22, 23, 25, 37, 42, 43, 53,
    69, 77, 79, 87, 95, 101, 102, 103, 104, 109, 110, 111, 113, 115, 117,
    119, 123, 135, 137, 139, 143, 161, 179, 389, 427, 465, 512, 513, 514,
    515, 526, 530, 531, 532, 540, 548, 554, 556, 563, 587, 601, 636, 989,
    990, 993, 995, 1719, 1720, 1723, 2049, 3659, 4045, 4190, 5060, 5061,
    6000, 6566, 6665, 6666, 6667, 6668, 6669, 6679, 6697, 10080,
})


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


def http_request(url, method="GET", body=None, headers=None):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    headers = dict(headers or {})
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        headers.setdefault("Content-Type", "application/json")
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        response = opener.open(request, timeout=18)
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

    async def raw_post(self, path, body):
        return await asyncio.to_thread(http_request, self.url + path, "POST", body)

    async def post(self, path, body, expected=200):
        status, result, headers = await self.raw_post(path, body)
        assert status == expected, (path, body, status, result)
        assert headers.get("Cache-Control") == "no-store", headers
        assert not any(name.lower().startswith("access-control-") for name in headers), headers
        return result

    async def set_mode(self, mode):
        # An uncertain POST is never repeated automatically. A caller must read
        # current canonical state before deliberately authorizing another body.
        state = await self.until(lambda value: value["controller_ready"], "controller ready for mode change")
        if mode == "auto":
            # Initial endpoint replies may change eligibility between separate
            # HTTP reads. Wait for one reviewable semantic state before creating
            # the single activation body; never retry an uncertain POST.
            revision = state["snapshot_revision"]
            stable_since = time.monotonic()

            async def stable_review():
                nonlocal revision, stable_since
                current = await self.state()
                if not current or not current["controller_ready"]:
                    return None
                if current["snapshot_revision"] != revision:
                    revision = current["snapshot_revision"]
                    stable_since = time.monotonic()
                return current if time.monotonic() - stable_since >= 0.3 else None
            state = await wait(stable_review, "stable Auto activation review", timeout=10)
        body = {"mode": mode, "expected_boot_id": state["controller_boot_id"],
                "expected_mode_revision": state["mode_revision"], "request_id": uuid.uuid4().hex}
        if mode == "auto":
            body["reviewed_snapshot_revision"] = state["snapshot_revision"]
        result = await self.post("/api/mode", body)
        await self.until(lambda value: value["mode"] == mode, "acknowledged " + mode)
        return result

    async def plan(self, action, target=None):
        state = await self.state()
        body = {"action": action, "snapshot_revision": state["snapshot_revision"]}
        if target is not None:
            body["target_pile"] = target
        return await self.post("/api/plans", body)

    async def execute(self, plan, request_id=None):
        return await self.post("/api/operations", {
            "plan_id": plan["plan_id"], "request_id": request_id or uuid.uuid4().hex}, expected=202)

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
    def __init__(self, binary, root, names=("a", "b"), mode="auto", initial=None, timing=None, mock_heartbeat_timeout=0.4):
        self.root = Path(root)
        self.ports = PortManager()
        topology = Topology(
            names, 3, initial=initial, timings=tiny_timings(lifecycle_delay=0.05, heartbeat_timeout=mock_heartbeat_timeout),
            control_ports=[self.ports.get_port() for _ in names],
            grpc_ports=[[self.ports.get_port() for _ in range(3)] for _ in names])
        self.cluster = LiveCluster(topology=topology)
        self.initial_mode = mode
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
        # Keep rejected candidates reserved until close, like the listener ports.
        while True:
            http_port = self.ports.get_port()
            if http_port not in FETCH_BAD_PORTS:
                break
        self.controller = Controller(binary, root, {
            "cluster_id": "acceptance",
            "endpoints": endpoints, "http": {"listen": "127.0.0.1:%d" % http_port},
            "timing": values, "health_endpoints_per_pile": 3})

    async def start(self, skipper=True):
        await self.cluster.start()
        if skipper:
            await self.controller.start()
            state = await self.healthy()
            assert state["mode"] == "observe", state
            assert not self.mutations(), self.mutations()
            if self.initial_mode != "observe":
                await self.controller.set_mode(self.initial_mode)
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
        def healthy(state):
            if not state["view"]:
                return False
            participants = {pile["name"] for pile in state["view"]["piles"]
                            if pile["state"] in ("PRIMARY", "PROMOTED", "SYNCHRONIZED", "NOT_SYNCHRONIZED")}
            verdicts = {pile["name"]: pile["verdict"] for pile in state["piles"]}
            return bool(participants) and all(verdicts.get(name) == "HEALTHY" for name in participants)
        return await self.controller.until(healthy, "BSC warmup", timeout=30)

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
