"""Executable failover scenarios against the real C++ binary and SDK."""
import asyncio
import fcntl
import json
from pathlib import Path
import re
import shlex
import time
import unittest
import uuid

import yatest.common

from ydb.apps.bridge_skipper.acceptance.lib.harness import (
    BRIDGE_UPDATE, Environment, assert_mutation, http_request, wait)
from ydb.apps.bridge_skipper.ydbd_mock.lib import model


BINARY = "ydb/apps/bridge_skipper/bridge_skipper"
SAMPLE = re.compile(r"([A-Za-z_:][\w:]*)(?:\{(.*)\})?\s+(\S+)")
LABEL = re.compile(r'(\w+)="((?:[^"\\]|\\.)*)"')


def piles(state):
    return {pile["name"]: pile for pile in state["piles"]}


def metric(text, name, **labels):
    """The value of the single Prometheus sample of `name` whose labels include `labels`."""
    values = []
    for line in text.splitlines():
        match = SAMPLE.match(line)
        if not line.startswith("#") and match and match[1] == name \
                and labels.items() <= dict(LABEL.findall(match[2] or "")).items():
            values.append(float(match[3]))
    assert len(values) == 1, (name, labels, values, text)
    return values[0]


class ExecutableCase(unittest.IsolatedAsyncioTestCase):
    async def make(self, start=True, **options):
        root = Path(yatest.common.output_path("e2e_" + uuid.uuid4().hex))
        root.mkdir(mode=0o700)
        environment = Environment(yatest.common.binary_path(BINARY), root, **options)
        self.addAsyncCleanup(environment.close)
        await environment.start(skipper=start)
        return environment

    async def scrape(self, e):
        status, body, _ = await asyncio.to_thread(http_request, e.controller.url + "/metrics")
        self.assertEqual(status, 200, body)
        return body


class ExecutableTest(ExecutableCase):
    async def test_http_startup_keeps_common_and_skipper_log_components(self):
        e = await self.make(start=False)
        e.controller.config["logging"] = {"level": "debug"}
        e.controller.config_path.write_text(json.dumps(e.controller.config))
        await e.controller.start()
        await self.scrape(e)
        await e.controller.stop()
        self.assertEqual(e.controller.process.returncode, 0, e.controller.log_path.read_text())
        records = [json.loads(line) for line in e.controller.log_path.read_text().splitlines()]
        self.assertTrue(any(record.get("component") == "HTTP" for record in records), records)
        self.assertTrue(any(record.get("component") == "BRIDGE_SKIPPER"
                            and record.get("event") == "startup" for record in records), records)

    async def test_observe_is_read_only_and_manual_execute_is_single_use(self):
        e = await self.make(mode="observe")
        state = await e.healthy()
        self.assertEqual(state["mode"], "observe")
        self.assertEqual(e.controller.persistent()["mode"], "observe")
        self.assertEqual(e.controller.persistent()["state_schema"], 3)
        self.assertNotIn("mode", e.controller.config)
        generation = await e.generation()
        await e.blame("b")
        state = await e.controller.until(lambda state: state["decision"]["kind"] == "READY", "Observe Ready plan")
        mode_revision = state["mode_revision"]
        for method in ("GET", "HEAD"):
            status, _, headers = await asyncio.to_thread(http_request, e.controller.url + "/?mode=auto", method)
            self.assertEqual(status, 200)
            self.assertEqual(headers.get("Cache-Control"), "no-store")
            observed = await e.controller.state()
            self.assertEqual((observed["mode"], observed["mode_revision"]), ("observe", mode_revision))
        status, exported, headers = await asyncio.to_thread(
            http_request, e.controller.url + "/api/logs/export?level=error&search=definitely-no-matching-record")
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("Cache-Control"), "no-store")
        # The retained-log export starts with UTC/cursor coverage metadata and
        # contains no records when the filter matches none.
        self.assertIsInstance(exported, dict, exported)
        self.assertIn("next_cursor", exported)
        self.assertNotIn("rows", exported)
        await e.quiet(1)
        self.assertEqual(e.mutations(), [])
        await e.controller.set_mode("manual")
        plan = await e.controller.plan("failover")
        self.assertEqual(plan["plan"]["update"], {"b": "DISCONNECTED"})
        self.assertEqual(plan["plan"]["expected_generation"], str(generation))
        execute = {"plan_id": plan["plan_id"], "request_id": uuid.uuid4().hex}
        first = await e.controller.post("/api/operations", execute, expected=202)
        replay = await e.controller.post("/api/operations", execute, expected=202)
        self.assertEqual(first["operation_id"], replay["operation_id"])
        await e.mutation(generation, {"b": "DISCONNECTED"})
        refused = await e.controller.post("/api/operations", dict(execute, request_id=uuid.uuid4().hex), expected=409)
        self.assertIn(refused["error"], ("PLAN_CONSUMED", "PLAN_UNKNOWN"))
        await e.quiet(2)
        self.assertEqual(len(e.mutations()), 1)

    async def test_manual_lifecycle_take_offline_and_rejoin_exact_overlays(self):
        e = await self.make(mode="manual")
        await e.healthy()
        generation = await e.generation()
        plan = await e.controller.plan("take_offline", "b")
        self.assertEqual(plan["plan"]["update"], {"b": "SUSPENDED"})
        await e.controller.execute(plan)
        await e.mutation(generation, {"b": "SUSPENDED"})
        await e.controller.until(lambda state: state["view"] and
                                 dict((pile["name"], pile["state"]) for pile in state["view"]["piles"])["b"] == "DISCONNECTED",
                                 "takedown follow-up observed")
        await e.healthy()
        generation = await e.generation()
        marker = max(event["ts"] for event in e.mutations()) + 0.000001
        plan = await e.controller.plan("rejoin", "b")
        self.assertEqual(plan["plan"]["update"], {"b": "NOT_SYNCHRONIZED"})
        await e.controller.execute(plan)
        await e.mutation(generation, {"b": "NOT_SYNCHRONIZED"}, after=marker)
        await e.controller.until(lambda state: state["pending_mutation"] is None, "rejoin reconciled")
        self.assertEqual(len(e.mutations()), 2)

    async def test_mode_revision_race_restore_and_boot_fencing(self):
        e = await self.make(mode="observe")
        state = await e.healthy()
        body = {"mode": "manual", "expected_boot_id": state["controller_boot_id"],
                "expected_mode_revision": state["mode_revision"], "request_id": uuid.uuid4().hex}
        results = await asyncio.gather(
            e.controller.raw_post("/api/mode", body),
            e.controller.raw_post("/api/mode", dict(body, request_id=uuid.uuid4().hex)))
        self.assertEqual(sorted(status for status, _, _ in results), [200, 409])
        await e.controller.stop()
        await e.controller.start()
        restored = await e.controller.until(lambda value: value["controller_ready"], "restored controller")
        self.assertEqual(restored["mode"], "manual")
        self.assertNotEqual(restored["controller_boot_id"], state["controller_boot_id"])
        await e.controller.post("/api/mode", dict(body, request_id=uuid.uuid4().hex), expected=409)
        self.assertEqual(e.mutations(), [])

    async def test_restored_auto_resends_identical_pending_once_without_mode_activation(self):
        e = await self.make()
        state = await e.healthy()
        generation = int(state["view"]["generation"])
        await e.control("a", "/failpoints", all=True, method=BRIDGE_UPDATE,
                        stage="after_acceptance", action="hold", once=True)
        await e.blame("b")
        await e.mutation(generation, {"b": "DISCONNECTED"})
        before = await e.controller.until(
            lambda value: value["pending_mutation"] is not None, "pending attempt before restart")
        await e.controller.stop()
        persisted = e.controller.persistent()
        self.assertEqual(persisted["mode"], "auto")
        self.assertEqual(persisted["pending_mutation"]["expected_generation"], str(generation))
        self.assertEqual(persisted["pending_mutation"]["update"], {"b": "DISCONNECTED"})
        marker = max(event["ts"] for event in e.mutations()) + 0.000001

        await e.controller.start()
        restored = await e.controller.until(lambda value: value["controller_ready"], "restored Auto ready")
        self.assertEqual(restored["mode"], "auto")
        self.assertNotEqual(restored["controller_boot_id"], before["controller_boot_id"])
        # No mode POST after restart: durable Auto alone authorizes this resend.
        await e.mutation(generation, {"b": "DISCONNECTED"}, after=marker)
        await e.control("a", "/failpoints", all=True, method=BRIDGE_UPDATE,
                        stage="after_acceptance", action="release")
        await e.controller.until(
            lambda value: value["pending_mutation"] is None and value["view"]
            and int(value["view"]["generation"]) > generation, "restored Auto update reconciled")
        await e.quiet(3)
        self.assertEqual(len(e.mutations()), 2)
        self.assertEqual(len([event for event in e.mutations() if event["ts"] >= marker]), 1)

    async def test_e1_dead_synchronized_confirmed_once(self):
        e = await self.make()
        await e.healthy()
        health_requests = [event for event in e.events() if event["kind"] == "rpc.start"
                           and event["method"].endswith("/SelfCheck")]
        self.assertTrue(health_requests)
        self.assertTrue(all(event["summary"] == "verbose=False merge=False cache=False"
                            for event in health_requests))
        generation = await e.generation()
        # down_since is observation time; confirmation spans RPC evidence send times.
        down_at = time.time()
        await e.control("b", "/control", op="down", all=True)
        state = await e.controller.until(
            lambda state: state["decision"]["kind"] == "CONFIRMING"
            and piles(state).get("b", {}).get("verdict") == "FAILED", "unconfirmed failure")
        self.assertFalse(piles(state)["b"]["confirmed"])
        metrics = await self.scrape(e)
        self.assertEqual(metric(metrics, "skipper_pile_verdict", pile="b", verdict="FAILED"), 1)
        self.assertEqual(e.mutations(), [])
        events = await e.mutation(generation, {"b": "DISCONNECTED"})
        self.assertGreaterEqual(events[0]["ts"] - down_at, e.controller.config["timing"]["confirm_seconds"])
        await e.controller.until(lambda state: state["pending_mutation"] is None and state["last_mutation"]
                                 and state["last_mutation"]["outcome"] == "SUCCESS", "successful failover")
        metrics = await self.scrape(e)
        self.assertEqual(metric(metrics, "skipper_mutation_total", outcome="SUCCESS"), 1)
        self.assertGreaterEqual(
            metric(metrics, "skipper_rpc_total", method="update_cluster_state", status="SUCCESS"), 1)
        await e.quiet(3)
        self.assertEqual(len(e.mutations()), 1)
        self.assertEqual(e.controller.persistent()["pending_mutation"], None)
        self.assertGreaterEqual(int(e.controller.persistent()["generation_high_water"]), generation + 1)

    async def test_e2_primary_loss_uses_fallback_and_connectivity(self):
        e = await self.make()
        generation = await e.generation()
        await e.cluster.kill("a")
        state = await e.controller.until(
            lambda state: any(endpoint["selfcheck_mode"] == "FALLBACK"
                              for endpoint in state["endpoints"]) and "a" in piles(state)
            and piles(state)["a"]["verdict"] == "LOST", "primary lost in fallback")
        self.assertFalse(piles(state)["a"]["self_quorum"])
        await e.mutation(generation, {"a": "DISCONNECTED", "b": "PRIMARY"})
        await e.quiet(3)
        self.assertEqual(len(e.mutations()), 1)

    async def test_e4_lost_commit_reply_is_resolved_from_new_view(self):
        e = await self.make()
        await e.healthy()
        generation = await e.generation()
        await e.control("a", "/failpoints", all=True, method=BRIDGE_UPDATE,
                        stage="after_commit", action="unavailable", once=True)
        await e.blame("b")
        await e.mutation(generation, {"b": "DISCONNECTED"})
        state = await e.controller.until(
            lambda state: state["pending_mutation"] is None
            and state["view"] and int(state["view"]["generation"]) > generation, "pending cleared at G+1")
        self.assertTrue(state["last_mutation"])
        await e.quiet(5)
        self.assertEqual(len(e.mutations()), 1)

    async def test_e5_generation_race_aborts_and_replans(self):
        e = await self.make(timing={"mutation_operation_timeout_seconds": 10})
        await e.healthy()
        generation = await e.generation()
        await e.control("a", "/failpoints", all=True, method=BRIDGE_UPDATE,
                        stage="after_acceptance", action="hold", once=True)
        await e.blame("b")
        await wait(lambda: any(event["kind"] == "failpoint.hit" for event in e.events()), "held accepted mutation")
        first = e.mutations()
        self.assertEqual(len(first), 1)
        assert_mutation(first[0], generation, {"b": "DISCONNECTED"})
        # Different endpoint: the first guarded proposal is still paused.
        await e.update({"a": "PRIMARY"}, source="b")
        after_external = max(event["ts"] for event in e.mutations()) + 0.000001
        moved = await e.controller.until(
            lambda state: state["view"]
            and int(state["view"]["generation"]) == generation + 1, "new decision view")
        next_generation = int(moved["view"]["generation"])
        await e.control("a", "/failpoints", all=True, method=BRIDGE_UPDATE,
                        stage="after_acceptance", action="release")

        # A replanned invocation may already be the last one; count the outcome.
        async def aborted():
            return metric(await self.scrape(e), "skipper_mutation_total", outcome="GENERATION_MOVED") >= 1
        await wait(aborted, "ABORTED outcome")
        await e.mutation(next_generation, {"b": "DISCONNECTED"}, after=after_external)
        await e.quiet(3)
        # One skipper invocation per decision generation, plus the external noop.
        self.assertEqual(len(e.mutations()), 3)

    async def test_e6_unavailable_resends_identical_to_another_endpoint(self):
        e = await self.make()
        await e.healthy()
        generation = await e.generation()
        preferred = min(e.cluster.topo.pile("a").nodes, key=lambda node: "grpc://" + node.endpoint)
        await e.control("a", "/rpc-override", label=preferred.label, method="UpdateClusterState",
                        response={"operation": {"ready": True, "status": "UNAVAILABLE"}}, once=True)
        await e.blame("b")
        events = await e.mutation(generation, {"b": "DISCONNECTED"}, count=2)
        self.assertEqual(events[0]["node"], preferred.label)
        self.assertNotEqual(events[0]["node"], events[1]["node"])
        await e.quiet(3)
        self.assertEqual(len(e.mutations()), 2)

    async def test_e8_dead_stage2_is_disconnected(self):
        e = await self.make(start=False)
        await e.all_control("/followups", hold=["sync_step"])
        await e.update({"b": "DISCONNECTED"})
        await e.update({"b": "NOT_SYNCHRONIZED"})
        await wait(lambda: e.cluster.inst("a").state.stored(1) == model.Stored.NOT_SYNCHRONIZED_2,
                   "stage2 fixture")
        await e.cluster.kill("b")
        marker = max(event["ts"] for event in e.mutations()) + 0.000001
        await e.controller.start()
        state = await e.controller.until(lambda state: state["decision"]["kind"] == "READY", "stage2 Ready activation review")
        generation = int(state["view"]["generation"])
        await e.controller.set_mode("auto")
        await e.mutation(generation, {"b": "DISCONNECTED"}, after=marker)

    async def test_e10_dead_suspended_is_blocked(self):
        e = await self.make(start=False)
        await e.all_control("/followups", hold=["suspend"])
        await e.update({"b": "SUSPENDED"})
        await e.cluster.kill("b")
        await e.control("a", "/followups", release=["suspend"])
        await e.controller.start()
        state = await e.controller.until(lambda state: state["decision"]["blocker"] == "SUSPENDED_PILE_LOST",
                                         "suspended maintenance dead end")
        before = e.mutations()
        await e.controller.set_mode("auto")
        self.assertEqual(state["view"]["piles"], [
            {"name": "a", "state": "PRIMARY"}, {"name": "b", "state": "SUSPENDED"}])
        await e.quiet(4)
        self.assertEqual(e.mutations(), before)

    async def test_e11_manual_publishes_guarded_plan_and_cli(self):
        e = await self.make(mode="manual")
        await e.healthy()
        generation = await e.generation()
        await e.blame("b")
        state = await e.controller.until(lambda state: state["decision"]["kind"] == "READY", "manual plan")
        self.assertEqual(state["decision"]["plan"]["update"], {"b": "DISCONNECTED"})
        self.assertEqual(int(state["decision"]["plan"]["expected_generation"]), generation)
        command = state["decision"]["cli_command"]
        self.assertIn("admin cluster bridge update", command)
        arguments = shlex.split(command)
        self.assertEqual(arguments[arguments.index("--expected-generation") + 1], str(generation))
        self.assertEqual([arguments[index + 1] for index, argument in enumerate(arguments)
                          if argument == "--set"], ["b:DISCONNECTED"])
        await e.quiet(4)
        self.assertEqual(e.mutations(), [])

    async def test_e15_partition_with_bsc_on_primary_side_disconnects_other_side(self):
        e = await self.make()
        await e.healthy()
        generation = await e.generation()
        await e.partition()
        state = await e.controller.until(
            lambda state: piles(state).get("b", {}).get("verdict") == "FAILED"
            and any(endpoint["pile"] == "b" and endpoint["selfcheck_mode"] == "FALLBACK"
                    for endpoint in state["endpoints"]), "b failed in BSC mode while b answers in fallback")
        self.assertTrue(piles(state)["a"]["self_quorum"])
        self.assertTrue(piles(state)["b"]["self_quorum"])
        events = await e.mutation(generation, {"b": "DISCONNECTED"})
        self.assertEqual(events[0]["pile"], "a")
        await e.quiet(3)
        self.assertEqual(len(e.mutations()), 1)
        state = await e.controller.until(
            lambda state: int(state["generation_high_water"]) > generation, "committed high-water")
        self.assertEqual({endpoint["state_generation"] for endpoint in state["endpoints"]
                          if endpoint["pile"] == "b"}, {str(generation)})

    async def test_e18_staggered_failures_batch_at_same_generation(self):
        # B2: c fails while {b}@G is unresolved; the same-primary superset
        # {b, c}@G replaces the pending record instead of waiting behind it.
        e = await self.make(names=("a", "b", "c"), timing={"mutation_operation_timeout_seconds": 10},
                            mock_heartbeat_timeout=1.0)
        await e.healthy()
        generation = await e.generation()
        preferred = min(e.cluster.topo.pile("a").nodes, key=lambda node: "grpc://" + node.endpoint)
        await e.control("a", "/rpc-override", label=preferred.label, method="UpdateClusterState",
                        response={"operation": {"ready": True, "status": "INTERNAL_ERROR"}}, once=True)
        await e.control("b", "/control", op="down", all=True)
        first = await e.mutation(generation, {"b": "DISCONNECTED"})
        state = await e.controller.until(
            lambda state: state["last_mutation"] and state["last_mutation"]["outcome"] == "AMBIGUOUS",
            "ambiguous first outcome")
        self.assertEqual(state["last_mutation"]["status"], "INTERNAL_ERROR")
        self.assertEqual(state["pending_mutation"]["update"], {"b": "DISCONNECTED"})
        self.assertTrue(state["pending_mutation"]["may_apply_elsewhere"])
        await e.control("c", "/control", op="down", all=True)
        await e.mutation(generation, {"b": "DISCONNECTED", "c": "DISCONNECTED"}, after=first[0]["ts"] + 0.000001)
        state = await e.controller.until(
            lambda state: state["pending_mutation"] is None and state["view"]
            and int(state["view"]["generation"]) > generation, "batched failover committed")
        self.assertEqual({pile["name"]: pile["state"] for pile in state["view"]["piles"]},
                         {"a": "PRIMARY", "b": "DISCONNECTED", "c": "DISCONNECTED"})
        await e.quiet(3)
        self.assertEqual(len(e.mutations()), 2)

    async def test_e17_black_holed_pile_with_slow_selfcheck(self):
        # SelfCheck answers 5 s after the call (operation timeout 6 s, client deadline 8 s, round 1 s,
        # TTL 10 s), so the slot is busy at four of every five round boundaries and the deferred start
        # keeps the sends 5 s apart. Every connectivity report says binding while b lacks its static quorum.
        e = await self.make(timing={"selfcheck_operation_timeout_seconds": 6, "evidence_ttl_seconds": 10})
        await e.all_control("/control", op="delay", all=True, seconds=5, methods=["SelfCheck"])
        await e.all_control("/control", op="binding_flap", all=True, on=True, duty=1.0)
        await e.controller.until(lambda state: state["piles"] and all(
            pile["verdict"] == "HEALTHY" for pile in state["piles"]), "warm-up with slow SelfCheck", timeout=60)
        generation = await e.generation()
        await e.control("b", "/control", op="down", all=True)
        # The settled reports sent before b went down stay current until their TTL.
        await e.controller.until(
            lambda state: piles(state).get("b", {}).get("verdict") == "FAILED"
            and piles(state)["a"]["connectivity"]
            and all(item["binding_in_progress"] for item in piles(state)["a"]["connectivity"]),
            "b failed with only binding reports")
        events = await e.mutation(generation, {"b": "DISCONNECTED"})
        self.assertEqual(events[0]["pile"], "a")
        await e.controller.until(lambda state: state["pending_mutation"] is None and state["last_mutation"]
                                 and state["last_mutation"]["outcome"] == "SUCCESS", "successful failover")
        await e.quiet(3)
        self.assertEqual(len(e.mutations()), 1)

    async def test_hung_selfcheck_does_not_stall_polls_or_sigterm(self):
        # Deliberately no preload: run this executable case under ASan too.
        # The derived client deadline is 32 s, so the default 8 s stop bound
        # fails if Stop() waits out the hung call instead of cancelling it.
        e = await self.make(timing={"selfcheck_operation_timeout_seconds": 30, "evidence_ttl_seconds": 40})
        await e.control("a", "/control", label=e.cluster.label("a"), op="silence", methods=["SelfCheck"])
        marker = time.time()
        await wait(
            lambda: any(
                event["kind"] == "rpc.start" and event["node"] == e.cluster.label("a")
                and event["ts"] >= marker and event["method"].endswith("/SelfCheck") for event in e.events()),
            "hung SelfCheck sent")
        generation = await e.generation()
        before = e.mutations()
        await e.update({"a": "PRIMARY"})
        state = await e.controller.until(
            lambda state: state["view"]
            and int(state["view"]["generation"]) > generation, "polls continue during hung health RPC")
        self.assertLess(state["loop_age_ms"], 1000)
        self.assertEqual(len(e.mutations()), len(before) + 1)  # external update only
        await e.controller.stop()
        self.assertEqual(e.controller.process.returncode, 0, e.controller.log_path.read_text())

    async def test_startup_reports_state_directory_error(self):
        e = await self.make(start=False)
        e.controller.state_dir.mkdir()
        lock = (e.controller.state_dir / "skipper.lock").open("w")
        self.addCleanup(lock.close)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        await e.controller.start(ready=False)
        code = await asyncio.to_thread(e.controller.process.wait, 10)
        await e.controller.stop()
        self.assertEqual(code, 1)
        log = e.controller.log_path.read_text()
        self.assertIn('"event":"startup_failed"', log)
        self.assertIn('"category":"state"', log)
        self.assertIn("state directory is locked or lock unavailable", log)
