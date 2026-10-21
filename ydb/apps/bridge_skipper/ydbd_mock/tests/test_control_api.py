"""The HTTP control API and the peer API of a live instance."""
import asyncio
import argparse
import os
import tempfile
import time
import unittest
from unittest import mock

from ydb.apps.bridge_skipper.ydbd_mock.lib import model
from ydb.apps.bridge_skipper.ydbd_mock.lib import supervisor
from ydb.apps.bridge_skipper.ydbd_mock.lib.testkit import LiveCluster, wait_until


class ApiCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.c = LiveCluster()
        await self.c.start()

    async def asyncTearDown(self):
        await self.c.stop()

    async def post(self, pile, path, body, expect=200):
        status, out = await self.c.http(pile, "POST", path, body)
        self.assertEqual(status, expect, out)
        return out

    async def get(self, pile, path, expect=200, **params):
        status, out = await self.c.http(pile, "GET", path, params=params)
        self.assertEqual(status, expect, out)
        return out

    def node_faults(self, state, label):
        return next(n["faults"] for n in state["nodes"] if n["label"] == label)


class StateViewTest(ApiCase):
    async def test_state_shape(self):
        s = await self.get("pile1", "/state")
        self.assertEqual((s["pile"], s["generation"], s["primary"], s["promoted"]), ("pile1", 1, "pile1", "pile1"))
        self.assertEqual((s["ready"], s["detached"], s["blocked"], s["prepared"]), (True, False, [], None))
        self.assertEqual([(p["name"], p["public"], p["stored"]) for p in s["piles"]],
                         [("pile1", "PRIMARY", "SYNCHRONIZED"), ("pile2", "SYNCHRONIZED", "SYNCHRONIZED")])
        self.assertEqual([n["label"] for n in s["nodes"]], ["mock-pile1-1", "mock-pile1-2", "mock-pile1-3"])
        self.assertEqual([n["id"] for n in s["nodes"]], [1, 2, 3])
        self.assertNotIn("admin_token", s["policies"])
        self.assertIn("promote_delay", s["timings"])
        self.assertEqual(s["pid"], os.getpid(), "the in-process instance reports this process")

    async def test_peers_become_connected(self):
        await wait_until(lambda: self.c.inst("pile1").connected(1), message="pile2 seen from pile1")
        s = await self.get("pile1", "/state")
        self.assertTrue(next(p for p in s["piles"] if p["name"] == "pile2")["connected"])


class EventsViewTest(ApiCase):
    async def test_filters(self):
        inst = self.c.inst("pile1")
        inst.events.append("scenario", note="alpha")
        inst.events.append("scenario", note="beta")
        inst.events.append("fault", node="x")
        all_events = (await self.get("pile1", "/events"))["events"]
        self.assertEqual(all_events[0]["kind"], "start")
        self.assertEqual([e["seq"] for e in all_events], sorted(e["seq"] for e in all_events))
        scenarios = (await self.get("pile1", "/events", kind="scenario"))["events"]
        self.assertEqual([e["note"] for e in scenarios], ["alpha", "beta"])
        grep = (await self.get("pile1", "/events", grep="beta"))["events"]
        self.assertEqual([e["note"] for e in grep], ["beta"])
        since = scenarios[0]["seq"]
        later = (await self.get("pile1", "/events", since=since))["events"]
        self.assertTrue(all(e["seq"] > since for e in later))
        self.assertEqual([e["kind"] for e in later][:2], ["scenario", "fault"])
        limited = (await self.get("pile1", "/events", limit=1))["events"]
        self.assertEqual(len(limited), 1)
        out = await self.get("pile1", "/events")
        self.assertEqual(out["last_seq"], all_events[-1]["seq"])
        ts = scenarios[1]["ts"]
        newer = (await self.get("pile1", "/events", after_ts=ts))["events"]
        self.assertTrue(all(e["ts"] > ts for e in newer))

    async def test_bad_numbers_are_rejected(self):
        for params in ({"since": "x"}, {"limit": "y"}, {"after_ts": "z"}):
            await self.get("pile1", "/events", expect=400, **params)

    async def test_every_event_has_the_common_fields(self):
        await self.post("pile1", "/scenario", {"name": "demo"})
        for e in (await self.get("pile1", "/events"))["events"]:
            self.assertEqual({"seq", "ts", "pile", "kind"} - set(e), set())
            self.assertEqual(e["pile"], "pile1")


class ControlOpsTest(ApiCase):
    async def test_every_op_is_reflected_in_state_and_cleared(self):
        label = self.c.label("pile1", 2)
        cases = [
            (dict(op="storage", piles=["pile2"]), {"storage": ["pile2"]}),
            (dict(op="connectivity", piles=["pile2"]), {"connectivity": ["pile2"]}),
            (dict(op="unavailable", on=True), {"unavailable": True}),
            (dict(op="silence", on=True, max=30), {"silence": True, "silence_max": 30.0}),
            (dict(op="delay", seconds=0.5, methods=["GetClusterState"]),
             {"delay": 0.5, "delay_methods": ["GetClusterState"]}),
            (dict(op="self_check", value="DEGRADED"), {"self_check": "DEGRADED"}),
        ]
        for body, expected in cases:
            with self.subTest(body=body):
                state = await self.post("pile1", "/control", dict(body, label=label))
                self.assertEqual(self.node_faults(state, label), expected)
                self.assertEqual(self.node_faults(state, self.c.label("pile1", 1)), {})
                state = await self.post("pile1", "/control", {"label": label, "op": "clear"})
                self.assertEqual(self.node_faults(state, label), {})

    async def test_targets_by_node_id_label_and_all(self):
        state = await self.post("pile1", "/control", {"node": 3, "op": "unavailable"})
        self.assertEqual(self.node_faults(state, "mock-pile1-3"), {"unavailable": True})
        state = await self.post("pile1", "/control", {"all": True, "op": "unavailable", "on": True})
        self.assertTrue(all(n["faults"] == {"unavailable": True} for n in state["nodes"]))
        state = await self.post("pile1", "/control", {"all": True, "op": "clear"})
        self.assertTrue(all(n["faults"] == {} for n in state["nodes"]))

    async def test_each_control_call_is_logged_as_a_fault_event(self):
        await self.post("pile1", "/control", {"label": "mock-pile1-1", "op": "delay", "seconds": 0.2})
        faults = (await self.get("pile1", "/events", kind="fault"))["events"]
        self.assertEqual(faults[-1]["node"], "mock-pile1-1")
        self.assertEqual(faults[-1]["op"], "delay")
        self.assertEqual(faults[-1]["args"], {"seconds": 0.2})

    async def test_down_and_clear_bring_the_listener_back(self):
        label = self.c.label("pile1", 1)
        node = self.c.topo.node_by_label(label)
        state = await self.post("pile1", "/control", {"label": label, "op": "down"})
        self.assertEqual(self.node_faults(state, label), {"down": True})
        self.assertFalse(await self._listening(node.grpc_port))
        await self.post("pile1", "/control", {"label": label, "op": "clear"})
        self.assertTrue(await self._listening(node.grpc_port))

    async def _listening(self, port) -> bool:
        try:
            _, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", port), 1.0)
        except (OSError, asyncio.TimeoutError):
            return False
        writer.close()
        return True

    async def test_losing_the_whole_pile_flips_ready_and_the_peer_notices(self):
        await wait_until(lambda: self.c.inst("pile2").connected(0), message="pile1 seen from pile2")
        await self.post("pile1", "/control", {"all": True, "op": "down"})
        s = await self.get("pile1", "/state")
        self.assertFalse(s["ready"])
        await wait_until(lambda: not self.c.inst("pile2").connected(0), message="pile1 not ready for pile2")
        await self.post("pile1", "/control", {"all": True, "op": "clear"})
        await wait_until(lambda: self.c.inst("pile2").connected(0), message="pile1 back for pile2")

    async def test_bad_requests(self):
        label = self.c.label("pile1")
        bad = [
            ({"label": label}, 400),
            ({"label": label, "op": "explode"}, 400),
            ({"op": "clear"}, 400),
            ({"label": "nope", "op": "clear"}, 404),
            ({"node": 99, "op": "clear"}, 404),
            ({"label": label, "op": "storage", "piles": ["pile9"]}, 404),
            ({"label": label, "op": "storage", "piles": "pile2"}, 400),
            ({"label": label, "op": "self_check", "value": "FINE"}, 400),
            ({"label": label, "op": "delay", "seconds": "slow"}, 400),
            ({"label": label, "op": "delay", "seconds": -1}, 400),
            ({"label": label, "op": "delay", "seconds": 1, "methods": "x"}, 400),
            ({"label": label, "op": "unavailable", "on": "yes"}, 400),
            ({"label": label, "op": "silence", "on": True, "max": "soon"}, 400),
        ]
        for body, status in bad:
            with self.subTest(body=body):
                out = await self.post("pile1", "/control", body, expect=status)
                self.assertIn("error", out)
        state = await self.get("pile1", "/state")
        self.assertTrue(all(n["faults"] == {} for n in state["nodes"]))

    async def test_body_must_be_a_json_object(self):
        import aiohttp
        url = self.c.topo.control_url("pile1") + "/control"
        async with aiohttp.ClientSession() as session:
            for data in ("not json", "[1, 2]", ""):
                async with session.post(url, data=data) as response:
                    self.assertEqual(response.status, 400, data)

    async def test_unknown_routes_are_404(self):
        await self.get("pile1", "/nope", expect=404)


class LinkTest(ApiCase):
    async def test_block_unblock_set(self):
        s = await self.post("pile1", "/link", {"block": ["pile2"]})
        self.assertEqual(s["blocked"], ["pile2"])
        await wait_until(lambda: not self.c.inst("pile1").connected(1), message="blocked peer is disconnected")
        s = await self.post("pile1", "/link", {"unblock": ["pile2"]})
        self.assertEqual(s["blocked"], [])
        await wait_until(lambda: self.c.inst("pile1").connected(1), message="peer back")
        s = await self.post("pile1", "/link", {"set": ["pile2"]})
        self.assertEqual(s["blocked"], ["pile2"])
        s = await self.post("pile1", "/link", {"set": []})
        self.assertEqual(s["blocked"], [])
        links = (await self.get("pile1", "/events", kind="link"))["events"]
        self.assertEqual([e["blocked"] for e in links], [["pile2"], [], ["pile2"], []])

    async def test_blocking_oneself_is_ignored(self):
        s = await self.post("pile1", "/link", {"block": ["pile1"]})
        self.assertEqual(s["blocked"], [])

    async def test_bad_requests(self):
        await self.post("pile1", "/link", {}, expect=400)
        await self.post("pile1", "/link", {"block": ["pile9"]}, expect=404)
        await self.post("pile1", "/link", {"block": "pile2"}, expect=400)

    async def test_one_sided_block_makes_the_peer_api_answer_503(self):
        await self.post("pile1", "/link", {"block": ["pile2"]})
        status, _ = await self.c.http("pile1", "GET", "/peer/ping", params={"from": "pile2"})
        self.assertEqual(status, 503)
        status, _ = await self.c.http("pile2", "GET", "/peer/ping", params={"from": "pile1"})
        self.assertEqual(status, 200)


class ReplicaOverrideTest(ApiCase):
    async def test_override_detach_and_clear(self):
        s = await self.post("pile1", "/replica", {
            "states": {"pile2": "DISCONNECTED"}, "generation": 7, "detach": True})
        self.assertEqual((s["generation"], s["detached"]), (7, True))
        self.assertEqual([p["public"] for p in s["piles"]], ["PRIMARY", "DISCONNECTED"])
        self.assertEqual((await self.get("pile2", "/state"))["generation"], 1, "the other replica is untouched")
        kinds = [e["kind"] for e in (await self.get("pile1", "/events"))["events"]]
        self.assertIn("replica.override", kinds)
        s = await self.post("pile1", "/replica", {"clear": True})
        self.assertFalse(s["detached"])
        kinds = [e["kind"] for e in (await self.get("pile1", "/events"))["events"]]
        self.assertIn("replica.reattach", kinds)

    async def test_override_without_generation_keeps_it(self):
        s = await self.post("pile2", "/replica", {"states": {"pile1": "PRIMARY", "pile2": "PROMOTED"}})
        self.assertEqual(s["generation"], 1)
        self.assertEqual((s["primary"], s["promoted"]), ("pile1", "pile2"))

    async def test_detached_replica_refuses_to_prepare(self):
        await self.post("pile2", "/replica", {"detach": True})
        snapshot = self.c.inst("pile2").replica.state.copy()
        snapshot.generation += 1
        reply = await self.post("pile2", "/peer/prepare", {
            "from": "pile1", "proposal_id": "p1", "snapshot": snapshot.to_json()})
        self.assertEqual((reply["ok"], reply["reason"]), (False, "detached"))

    async def test_bad_requests(self):
        bad = [
            ({"states": {"pile9": "PRIMARY"}}, 404),
            ({"states": {"pile2": "BOGUS"}}, 400),
            ({"states": ["pile2"]}, 400),
            ({"generation": -1}, 400),
            ({"generation": "x"}, 400),
            ({"states": {"pile1": "SYNCHRONIZED"}}, 400),  # no primary left
            ({"states": {"pile2": "PRIMARY"}}, 400),       # two primaries
        ]
        for body, status in bad:
            with self.subTest(body=body):
                await self.post("pile1", "/replica", body, expect=status)
        self.assertEqual((await self.get("pile1", "/state"))["generation"], 1)


class TimingsAndPoliciesTest(ApiCase):
    async def test_timings_update_and_validation(self):
        s = await self.post("pile1", "/timings", {"promote_delay": 3, "followup_stagger": "0.5"})
        self.assertEqual((s["timings"]["promote_delay"], s["timings"]["followup_stagger"]), (3.0, 0.5))
        await self.post("pile1", "/timings", {"nope": 1}, expect=400)
        await self.post("pile1", "/timings", {"promote_delay": -2}, expect=400)
        await self.post("pile1", "/timings", {"promote_delay": "x"}, expect=400)
        self.assertEqual((await self.get("pile1", "/state"))["timings"]["promote_delay"], 3.0)
        self.assertEqual((await self.get("pile2", "/state"))["timings"]["promote_delay"], 0.05,
                         "timings are per instance")

    async def test_policies_update_and_the_token_stays_secret(self):
        s = await self.post("pile1", "/policies", {"auto_blame_dead_piles": False, "admin_token": "s3"})
        self.assertFalse(s["policies"]["auto_blame_dead_piles"])
        self.assertNotIn("admin_token", s["policies"])
        events = (await self.get("pile1", "/events", kind="policies"))["events"]
        self.assertEqual(events[-1]["values"], {"auto_blame_dead_piles": False})
        await self.post("pile1", "/policies", {"nope": True}, expect=400)
        await self.post("pile1", "/policies", {"issue_style": "weird"}, expect=400)
        await self.post("pile1", "/policies", {"anti_entropy": "maybe"}, expect=400)


class ResetAndScenarioTest(ApiCase):
    async def test_reset_local_restores_a_clean_node(self):
        op = await self._update_pile2_down()
        self.assertTrue(op)
        await self.post("pile1", "/control", {"label": "mock-pile1-1", "op": "down"})
        await self.post("pile1", "/control", {"label": "mock-pile1-2", "op": "unavailable"})
        await self.post("pile1", "/link", {"block": ["pile2"]})
        s = await self.post("pile1", "/reset-local", {"generation": 20})
        self.assertEqual(s["generation"], 20)
        self.assertEqual([p["public"] for p in s["piles"]], ["PRIMARY", "SYNCHRONIZED"])
        self.assertTrue(all(n["faults"] == {} for n in s["nodes"]))
        self.assertEqual(s["blocked"], [])
        node = self.c.topo.node_by_label("mock-pile1-1")
        _, writer = await asyncio.open_connection("127.0.0.1", node.grpc_port)
        writer.close()
        resets = (await self.get("pile1", "/events", kind="reset"))["events"]
        self.assertEqual(resets[-1]["generation"], 20)

    async def _update_pile2_down(self):
        from ydb.public.api.protos.draft import ydb_bridge_pb2
        req = ydb_bridge_pb2.UpdateClusterStateRequest()
        req.updates.add(pile_name="pile2", state=model.PUB_DISCONNECTED)
        resp = await self.c.bridge(self.c.label("pile1")).UpdateClusterState(req)
        return resp.operation.status == model.SUCCESS

    async def test_reset_local_needs_a_positive_generation(self):
        for body in ({}, {"generation": 0}, {"generation": "5"}):
            await self.post("pile1", "/reset-local", body, expect=400)

    async def test_failed_persisted_reset_preserves_runtime_faults_and_links(self):
        inst = self.c.inst("pile1")
        with tempfile.TemporaryDirectory() as root:
            inst.replica.path = os.path.join(root, "replica.json")
            inst.replica.save()
            inst.node_faults[0].down = True
            inst.node_faults[0].unavailable = True
            inst.set_blocked([1])
            before = inst.state.copy()
            with mock.patch("ydb.apps.bridge_skipper.ydbd_mock.lib.replica.os.replace",
                            side_effect=OSError("injected replace failure")):
                await self.post("pile1", "/reset-local", {"generation": 9}, expect=500)
            self.assertEqual(inst.state, before)
            self.assertTrue(inst.node_faults[0].down)
            self.assertTrue(inst.node_faults[0].unavailable)
            self.assertEqual(inst.reach.blocked, {1})

    async def test_reset_samples_generation_after_pending_commits_are_fenced(self):
        loop = asyncio.get_running_loop()
        with tempfile.TemporaryDirectory() as root:
            topology_path = os.path.join(root, "topology.yaml")
            self.c.topo.save(topology_path)
            cluster = supervisor.Cluster(root)
            cluster._topo = self.c.topo
            cluster.alive = lambda pile: True
            cluster.pid = lambda pile: os.getpid()

            for name in self.c.topo.pile_names:
                self.c.inst(name).timings.promote_delay = 60.0

            delivery_entered = asyncio.Event()
            release_delivery = asyncio.Event()
            source = self.c.inst("pile1")
            transport = source.transport
            original_call = transport.call

            async def gated_call(src, dst, op, payload, timeout):
                snapshot = payload.get("snapshot", {})
                if src == 0 and dst == 1 and op == "commit" and snapshot.get("generation") == 3:
                    delivery_entered.set()
                    await release_delivery.wait()
                return await original_call(src, dst, op, payload, timeout)

            transport.call = gated_call
            proposal_tasks = []
            state_reads = 0
            injection_attempts = []
            original_request = cluster.request

            async def inject_commits():
                injection_attempts.append("g2")
                first = await source.admin_update([("pile2", model.PUB_PROMOTED)])
                injection_attempts.append("g3")
                second_task = asyncio.create_task(source.admin_update([
                    ("pile1", model.PUB_SYNCHRONIZED), ("pile2", model.PUB_PRIMARY)]))
                proposal_tasks.append(second_task)
                while not delivery_entered.is_set() and not second_task.done():
                    await asyncio.sleep(0.005)
                return first

            def intercepted_request(pile, method, path, body=None, timeout=5.0):
                nonlocal state_reads
                result = original_request(pile, method, path, body, timeout)
                if method == "GET" and path == "/state":
                    state_reads += 1
                    if state_reads == len(self.c.topo.pile_names):
                        asyncio.run_coroutine_threadsafe(inject_commits(), loop).result(timeout=10.0)
                return result

            cluster.request = intercepted_request
            printed = []
            try:
                with mock.patch.object(supervisor, "_signal"), mock.patch.object(
                        supervisor, "_scenario"), mock.patch.object(supervisor, "_print", printed.append):
                    await asyncio.wait_for(asyncio.to_thread(
                        supervisor.cmd_reset, cluster, argparse.Namespace()), timeout=15.0)
                self.assertTrue(printed)
                reset_generation = printed[-1]["generation"]
                self.assertEqual(reset_generation, 2)
                self.assertEqual(injection_attempts, ["g2", "g3"])
                release_delivery.set()
                if proposal_tasks:
                    await asyncio.wait_for(proposal_tasks[-1], timeout=5.0)
                await asyncio.sleep(0.1)
                for pile in self.c.topo.pile_names:
                    inst = self.c.inst(pile)
                    self.assertEqual(inst.state.generation, reset_generation)
                    self.assertEqual(inst.state.per_pile,
                                     [model.Stored.SYNCHRONIZED, model.Stored.SYNCHRONIZED])
                    self.assertEqual((inst.state.primary, inst.state.promoted), (0, 0))
            finally:
                release_delivery.set()
                for task in proposal_tasks:
                    if not task.done():
                        await asyncio.gather(task, return_exceptions=True)

    async def test_scenario_markers_land_in_the_event_log(self):
        out = await self.post("pile1", "/scenario", {
            "name": "S4", "step": "kill pile2", "kind": "ignored", "pile": "pile2"})
        self.assertEqual(out, {"ok": True})
        events = (await self.get("pile1", "/events", kind="scenario"))["events"]
        self.assertEqual((events[-1]["name"], events[-1]["step"]), ("S4", "kill pile2"))
        self.assertEqual((events[-1]["pile"], events[-1]["field_pile"]), ("pile1", "pile2"),
                         "the event always names the instance that logged it")


class PeerApiTest(ApiCase):
    async def test_ping_and_state(self):
        ping = await self.get("pile1", "/peer/ping", **{"from": "pile2"})
        self.assertEqual(ping, {"pile": "pile1", "generation": 1, "revision": 0, "ready": True})
        state = await self.get("pile1", "/peer/state", **{"from": "pile2"})
        self.assertEqual((state["generation"], state["per_pile"], state["primary"]), (1, [3, 3], 0))

    async def test_unknown_op_and_sender(self):
        await self.get("pile1", "/peer/nope", expect=404, **{"from": "pile2"})
        await self.get("pile1", "/peer/ping", expect=400, **{"from": "pile9"})
        await self.get("pile1", "/peer/ping", expect=400)
        await self.post("pile1", "/peer/push", {"snapshot": {}}, expect=400)

    async def test_prepare_then_abort(self):
        snapshot = self.c.inst("pile1").replica.state.copy()
        snapshot.generation += 1
        snapshot.revision += 1
        reply = await self.post("pile1", "/peer/prepare", {
            "from": "pile2", "proposal_id": "p1", "snapshot": snapshot.to_json()})
        self.assertEqual(reply["ok"], True)
        self.assertEqual((await self.get("pile1", "/state"))["prepared"], {"proposal_id": "p1", "generation": 2})
        other = await self.post("pile1", "/peer/prepare", {
            "from": "pile2", "proposal_id": "p2", "snapshot": snapshot.to_json()})
        self.assertEqual((other["ok"], other["reason"]), (False, "busy"))
        await self.post("pile1", "/peer/abort", {"from": "pile2", "proposal_id": "p1"})
        self.assertIsNone((await self.get("pile1", "/state"))["prepared"])

    async def test_prepare_for_a_wrong_generation_is_refused(self):
        snapshot = self.c.inst("pile1").replica.state.copy()
        snapshot.generation += 5
        reply = await self.post("pile1", "/peer/prepare", {
            "from": "pile2", "proposal_id": "p1", "snapshot": snapshot.to_json()})
        self.assertEqual((reply["ok"], reply["reason"]), (False, "generation"))
        refused = (await self.get("pile1", "/events", kind="prepare.refused"))["events"]
        self.assertEqual(refused[-1]["reason"], "generation")

    async def test_prepared_state_expires(self):
        snapshot = self.c.inst("pile1").replica.state.copy()
        snapshot.generation += 1
        snapshot.revision += 1
        await self.post("pile1", "/peer/prepare", {
            "from": "pile2", "proposal_id": "p1", "snapshot": snapshot.to_json()})
        started = time.monotonic()
        await wait_until(lambda: self.c.inst("pile1").replica.prepared is None, timeout=3.0,
                         message="prepared state to expire")
        self.assertLess(time.monotonic() - started, 3.0)

    async def test_push_installs_only_newer_generations(self):
        snapshot = self.c.inst("pile1").replica.state.copy()
        snapshot.generation = 5
        snapshot.per_pile[1] = model.Stored.DISCONNECTED
        reply = await self.post("pile1", "/peer/push", {"from": "pile2", "snapshot": snapshot.to_json()})
        self.assertEqual((reply["installed"], reply["generation"]), (True, 5))
        snapshot.generation = 3
        reply = await self.post("pile1", "/peer/push", {"from": "pile2", "snapshot": snapshot.to_json()})
        self.assertEqual((reply["installed"], reply["generation"]), (False, 5))
        commits = (await self.get("pile1", "/events", kind="commit"))["events"]
        self.assertEqual((commits[-1]["via"], commits[-1]["generation"], commits[-1]["origin"]),
                         ("push", 5, "pile2"))

    async def test_malformed_snapshots_are_a_client_error_and_change_nothing(self):
        good = self.c.inst("pile1").replica.state.copy()
        good.generation = 5
        bad = good.to_json()
        bad["per_pile"] = bad["per_pile"] + [3]
        for op in ("push", "commit", "prepare"):
            with self.subTest(op=op):
                await self.post("pile1", "/peer/" + op, {
                    "from": "pile2", "proposal_id": "p", "snapshot": bad, "cause": "admin"}, expect=400)
        state = await self.get("pile1", "/state")
        self.assertEqual((state["generation"], len(state["piles"]), state["prepared"]), (1, 2, None))

    async def test_heartbeat_timeout_posted_at_runtime_is_used_by_the_instance(self):
        await self.post("pile1", "/timings", {"heartbeat_timeout": 0.07})
        self.assertAlmostEqual(self.c.inst("pile1").reach.timeout, 0.07)


if __name__ == "__main__":
    unittest.main()
