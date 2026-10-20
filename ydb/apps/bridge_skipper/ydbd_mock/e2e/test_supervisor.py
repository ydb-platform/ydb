"""`ydbd_mock cluster ...` against real instance processes (no ydb CLI needed)."""
import json
import os
import socket
import subprocess
import tempfile
import time
import unittest

from harness import E2ECase, MockCluster
from ydb.apps.bridge_skipper.ydbd_mock.lib import supervisor
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Topology


REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", ".."))
MOCK_BINARY = os.path.join(REPO, "ydb", "apps", "bridge_skipper", "ydbd_mock", "ydbd_mock")


def read_json(path):
    with open(path) as f:
        return json.load(f)


def listening(port: int) -> bool:
    s = socket.socket()
    s.settimeout(1.0)
    try:
        s.connect(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        s.close()


class SupervisorTest(E2ECase):
    def test_up_wrote_the_run_directory(self):
        run = self.cluster.run_dir
        for name in ("topology.yaml", "endpoint_map.json", "cluster.json", "macros.json"):
            self.assertTrue(os.path.exists(os.path.join(run, name)), name)
        endpoint_map = read_json(os.path.join(run, "endpoint_map.json"))
        self.assertEqual(sorted(endpoint_map), ["mock-pile1-1", "mock-pile1-2", "mock-pile1-3",
                                                "mock-pile2-1", "mock-pile2-2", "mock-pile2-3"])
        self.assertEqual(self.cluster.ctl("endpoints").json(), endpoint_map)
        for pile in ("pile1", "pile2"):
            self.assertTrue(os.path.exists(os.path.join(run, "pids", "%s.pid" % pile)))
            self.assertTrue(os.path.exists(os.path.join(run, "logs", "%s.events.jsonl" % pile)))

    def test_status_text_and_json(self):
        text = self.cluster.ctl("status").out.splitlines()
        self.assertEqual(len(text), 2)
        self.assertRegex(text[0], r"^pile1 pid=\d+ alive gen=\d+ pile1=PRIMARY pile2=SYNCHRONIZED$")
        rows = self.cluster.ctl("status", "--json").json()
        self.assertEqual([(r["pile"], r["alive"], r["responsive"], r["ready"]) for r in rows],
                         [("pile1", True, True, True), ("pile2", True, True, True)])

    def test_state_of_one_and_of_all(self):
        one = self.cluster.ctl("state", "pile2").json()
        self.assertEqual(one["pile"], "pile2")
        everything = self.cluster.ctl("state").json()
        self.assertEqual(sorted(everything), ["pile1", "pile2"])
        self.assertEqual(everything["pile1"]["generation"], everything["pile2"]["generation"])

    def test_errors_exit_with_code_1_and_a_message(self):
        for args, text in [
            (["kill", "nope"], "unknown pile 'nope'"),
            (["state", "nope"], "unknown pile"),
            (["fault", "nosuch", "--op", "clear"], "unknown target"),
            (["partition", "pile1"], "at least two groups"),
            (["partition", "pile1|pile9"], "unknown pile 'pile9'"),
            (["partition", "pile1,pile2|pile2"], "appears in two groups"),
            (["replica", "pile1", "--states", "garbage"], "pile=STATE"),
            (["timings", "nope=1"], "HTTP 400"),
            (["node-down", "nosuch"], "unknown node label"),
            (["up", "--topology", "/nonexistent.yaml"], "No such file"),
        ]:
            with self.subTest(args=args):
                result = self.cluster.ctl(*args)
                if result.code == 1:
                    self.assertIn(text, result.err)
                else:  # `up` with a missing file raises OSError (not a topology error): any failure is fine
                    self.assertNotEqual(result.code, 0, result.err)

    def test_up_refuses_when_already_running(self):
        topo = os.path.join(self.cluster.run_dir, "topology.in.yaml")
        result = self.cluster.ctl("up", "--topology", topo)
        self.assertEqual(result.code, 1)
        self.assertIn("already running", result.err)

    def test_kill_keeps_the_event_log_and_revive_keeps_the_replica(self):
        # the event log outlives `cluster reset`, so earlier tests of the class leave scenario events behind
        seen = {(e["pile"], e["seq"]) for e in self.cluster.events("--kind", "scenario")}
        want = self.cluster.generation("pile2") + 10
        self.cluster.ctl("replica", "pile2", "--generation", str(want))
        self.assertEqual(self.cluster.generation("pile2"), want)
        self.assertEqual(self.cluster.ctl("kill", "pile2").code, 0)
        rows = self.cluster.ctl("status", "--json").json()
        self.assertEqual([r["alive"] for r in rows], [True, False])
        self.assertEqual(self.cluster.ctl("status").out.splitlines()[1].split()[-1], "dead")
        self.assertEqual(self.cluster.ctl("kill", "pile2").code, 1, "killing a dead pile is an error")
        kinds = [e["kind"] for e in self.cluster.events("--pile", "pile2")]
        self.assertIn("start", kinds)
        self.assertEqual(self.cluster.ctl("revive", "pile2").code, 0)
        self.assertEqual(self.cluster.generation("pile2"), want, "the persisted replica came back")
        starts = [e for e in self.cluster.events("--pile", "pile2") if e["kind"] == "start"]
        self.assertGreaterEqual(len(starts), 2, "the log of the killed process is still there")
        self.assertTrue(starts[-1]["loaded"], "the new process loaded the persisted replica")
        scenario = [e for e in self.cluster.events("--kind", "scenario")]
        mine = [e for e in scenario if e["name"] in ("kill", "revive") and (e["pile"], e["seq"]) not in seen]
        self.assertEqual([(e["name"], e["field_target"] if "field_target" in e else e["target"]) for e in mine],
                         [("kill", "pile2"), ("revive", "pile2")])
        self.assertEqual(mine[0]["pile"], "pile1", "a dead pile cannot log: a live one does")

    def test_stop_and_cont_freeze_a_process(self):
        self.assertEqual(self.cluster.ctl("stop", "pile2").code, 0)
        rows = self.cluster.ctl("status", "--json").json()
        self.assertEqual((rows[1]["alive"], rows[1]["responsive"]), (True, False))
        text = self.cluster.ctl("status").out.splitlines()[1]
        self.assertIn("alive unresponsive", text)
        self.assertEqual(self.cluster.ctl("cont", "pile2").code, 0)
        self.cluster.wait(lambda: self.cluster.ctl("status", "--json").json()[1].get("responsive"),
                          message="pile2 responsive after cont")

    def test_partition_and_heal(self):
        self.assertEqual(self.cluster.ctl("partition", "pile1|pile2").code, 0)
        for pile, other in (("pile1", "pile2"), ("pile2", "pile1")):
            self.assertEqual(self.cluster.state(pile)["blocked"], [other])
        self.assertEqual(read_json(os.path.join(self.cluster.run_dir, "macros.json")),
                         [{"kind": "partition", "spec": "pile1|pile2"}])
        self.assertEqual(self.cluster.ctl("heal").code, 0)
        for pile in ("pile1", "pile2"):
            self.assertEqual(self.cluster.state(pile)["blocked"], [])
        self.assertEqual(read_json(os.path.join(self.cluster.run_dir, "macros.json")), [])

    def test_keeper_split_and_heal(self):
        self.assertEqual(self.cluster.ctl("keeper-split", "pile1").code, 0)
        for node in self.cluster.state("pile1")["nodes"]:
            self.assertEqual(node["faults"], {"unavailable": True})
        self.assertEqual(self.cluster.state("pile2")["nodes"][0]["faults"], {})
        self.assertEqual(self.cluster.ctl("heal").code, 0)
        for node in self.cluster.state("pile1")["nodes"]:
            self.assertEqual(node["faults"], {})

    def test_heal_keeps_a_manual_unavailable_fault_on_the_split_pile(self):
        c = self.cluster
        self.assertEqual(c.ctl("fault", "mock-pile1-2", "--op", "unavailable").code, 0)
        self.assertEqual(c.ctl("keeper-split", "pile1").code, 0)
        self.assertEqual(c.ctl("heal").code, 0)
        faults = [n["faults"] for n in c.state("pile1")["nodes"]]
        self.assertEqual(faults, [{}, {"unavailable": True}, {}], "heal undoes the macro, not the manual fault")

    def test_revive_applies_the_partition_again(self):
        c = self.cluster
        self.assertEqual(c.ctl("partition", "pile1|pile2").code, 0)
        self.assertEqual(c.ctl("kill", "pile2").code, 0)
        self.assertEqual(c.ctl("revive", "pile2").code, 0)
        self.assertEqual(c.state("pile2")["blocked"], ["pile1"], "a revived pile is still on its side of the cut")
        self.assertEqual(c.state("pile1")["blocked"], ["pile2"])
        self.assertEqual(c.ctl("heal").code, 0)
        self.assertEqual(c.state("pile2")["blocked"], [])

    def test_revive_applies_the_keeper_split_again(self):
        c = self.cluster
        self.assertEqual(c.ctl("keeper-split", "pile2").code, 0)
        self.assertEqual(c.ctl("kill", "pile2").code, 0)
        self.assertEqual(c.ctl("revive", "pile2").code, 0)
        for node in c.state("pile2")["nodes"]:
            self.assertEqual(node["faults"], {"unavailable": True})
        self.assertEqual(c.ctl("heal").code, 0)
        self.assertTrue(all(n["faults"] == {} for n in c.state("pile2")["nodes"]))

    def test_fault_targets_a_pile_or_a_node(self):
        c = self.cluster
        self.assertEqual(c.ctl("fault", "pile1", "--op", "delay", "--seconds", "2", "--methods",
                               "GetClusterState,ListEndpoints").code, 0)
        for node in c.state("pile1")["nodes"]:
            self.assertEqual(node["faults"], {"delay": 2.0, "delay_methods": ["GetClusterState", "ListEndpoints"]})
        self.assertEqual(c.ctl("fault", "pile1", "--op", "delay", "--off").code, 0)
        self.assertEqual(c.ctl("fault", "mock-pile2-3", "--op", "silence", "--max", "30").code, 0)
        faults = [n["faults"] for n in c.state("pile2")["nodes"]]
        self.assertEqual(faults, [{}, {}, {"silence": True, "silence_max": 30.0}])
        self.assertEqual(c.ctl("fault", "mock-pile2-3", "--op", "silence", "--off").code, 0)
        self.assertEqual(c.ctl("fault", "pile2", "--op", "storage", "--piles", "pile1").code, 0)
        self.assertEqual(c.state("pile2")["nodes"][1]["faults"], {"storage": ["pile1"]})
        self.assertEqual(c.ctl("fault", "pile2", "--op", "self_check", "--value", "DEGRADED").code, 0)
        self.assertEqual(c.state("pile2")["nodes"][1]["faults"]["self_check"], "DEGRADED")
        self.assertEqual(c.ctl("fault", "pile2", "--op", "clear").code, 0)
        self.assertTrue(all(n["faults"] == {} for n in c.state("pile2")["nodes"]))

    def test_node_down_and_up_close_and_reopen_one_listener(self):
        node = self.cluster.topology.node_by_label("mock-pile1-2")
        self.assertTrue(listening(node.grpc_port))
        self.assertEqual(self.cluster.ctl("node-down", "mock-pile1-2").code, 0)
        self.assertFalse(listening(node.grpc_port))
        self.assertTrue(listening(self.cluster.topology.node_by_label("mock-pile1-1").grpc_port))
        self.assertEqual(self.cluster.ctl("node-up", "mock-pile1-2").code, 0)
        self.assertTrue(listening(node.grpc_port))

    def test_timings_and_policies_reach_every_live_instance(self):
        self.assertEqual(self.cluster.ctl("timings", "promote_delay=7", "followup_stagger=0.25").code, 0)
        self.assertEqual(self.cluster.ctl("policies", "auto_blame_dead_piles=false").code, 0)
        for pile in ("pile1", "pile2"):
            state = self.cluster.state(pile)
            self.assertEqual((state["timings"]["promote_delay"], state["timings"]["followup_stagger"]), (7.0, 0.25))
            self.assertFalse(state["policies"]["auto_blame_dead_piles"])
        self.assertEqual(self.cluster.ctl("timings", "promote_delay=0.5", "followup_stagger=0.05").code, 0)
        self.assertEqual(self.cluster.ctl("policies", "auto_blame_dead_piles=true").code, 0)

    def test_replica_command_overrides_and_clears(self):
        c = self.cluster
        self.assertEqual(c.ctl("replica", "pile2", "--generation", "9", "--states",
                               "pile1=PRIMARY,pile2=DISCONNECTED", "--detach").code, 0)
        state = c.state("pile2")
        self.assertEqual((state["generation"], state["detached"]), (9, True))
        self.assertEqual(c.vector("pile2"), ["PRIMARY", "DISCONNECTED"])
        self.assertEqual(c.ctl("replica", "pile2", "--clear").code, 0)
        self.assertFalse(c.state("pile2")["detached"])

    def test_events_are_merged_and_filterable(self):
        c = self.cluster
        t = c.now()
        c.ctl("kill", "pile2")
        c.ctl("revive", "pile2")
        events = c.events("--after-ts", repr(t))
        self.assertEqual(events, sorted(events, key=lambda e: (e["ts"], e["pile"], e["seq"])))
        self.assertEqual({e["pile"] for e in events}, {"pile1", "pile2"})
        self.assertTrue(all(e["ts"] > t for e in events))
        only2 = c.events("--after-ts", repr(t), "--pile", "pile2")
        self.assertEqual({e["pile"] for e in only2}, {"pile2"})
        self.assertEqual(len(c.events("--after-ts", repr(t), "--limit", "2")), 2)
        self.assertTrue(all(e["kind"].startswith("rpc") for e in c.events("--kind", "rpc")))
        self.assertTrue(all("revive" in json.dumps(e) for e in c.events("--grep", "revive")))

    def test_reset_revives_clears_and_picks_a_new_generation(self):
        c = self.cluster
        base = max(c.generation("pile1"), c.generation("pile2")) + 10
        c.ctl("replica", "pile1", "--generation", str(base))
        c.ctl("kill", "pile2")
        c.ctl("partition", "pile1|pile2")  # only pile1 is alive: it is blocked alone
        c.ctl("fault", "mock-pile1-1", "--op", "unavailable")
        result = c.ctl("reset")
        self.assertEqual(result.code, 0, result.err)
        self.assertEqual(result.json(), {"reset": True, "generation": base + 1})
        for pile in ("pile1", "pile2"):
            state = c.state(pile)
            self.assertEqual(state["generation"], base + 1)
            self.assertEqual(state["blocked"], [])
            self.assertTrue(all(n["faults"] == {} for n in state["nodes"]))
            self.assertEqual([p["public"] for p in state["piles"]], ["PRIMARY", "SYNCHRONIZED"])
        self.assertTrue(all(r["alive"] for r in c.ctl("status", "--json").json()))
        self.assertEqual(read_json(os.path.join(c.run_dir, "macros.json")), [])

    def test_sigterm_is_a_clean_stop(self):
        c = self.cluster
        c.ctl("down")
        for pile in ("pile1", "pile2"):
            self.assertFalse(os.path.exists(os.path.join(c.run_dir, "pids", "%s.pid" % pile)))
        ports = [p.control_port for p in c.topology.piles] + [n.grpc_port for n in c.topology.all_nodes()]
        self.assertFalse(any(listening(p) for p in ports))
        self.assertEqual(c.ctl("down").code, 0, "down is idempotent")
        topo = os.path.join(c.run_dir, "topology.in.yaml")
        self.assertEqual(c.ctl("up", "--topology", topo).code, 0)
        self.assertEqual(c.generation("pile1"), c.generation("pile2"))


class EventLogLifecycleTest(unittest.TestCase):
    def test_up_keeps_the_event_log_unless_fresh(self):
        with MockCluster() as c:
            topo = os.path.join(c.run_dir, "topology.in.yaml")

            def starts():
                return [e for e in c.events("--pile", "pile1") if e["kind"] == "start"]

            self.assertEqual(len(starts()), 1)
            self.assertEqual(c.ctl("down").code, 0)
            self.assertEqual(c.ctl("up", "--topology", topo).code, 0)
            self.assertEqual(len(starts()), 2, "the log of the first run is still there")
            seqs = [e["seq"] for e in c.events("--pile", "pile1")]
            self.assertEqual(seqs, sorted(set(seqs)), "seq keeps growing across runs")
            self.assertEqual(c.ctl("down").code, 0)
            self.assertEqual(c.ctl("up", "--topology", topo, "--fresh").code, 0)
            self.assertEqual(len(starts()), 1, "--fresh starts the log over")


class ThreePilesTest(unittest.TestCase):
    def test_three_pile_cluster_comes_up_and_partitions_by_groups(self):
        with MockCluster(piles=3) as c:
            rows = c.ctl("status", "--json").json()
            self.assertEqual([r["pile"] for r in rows], ["pile1", "pile2", "pile3"])
            self.assertEqual(c.vector("pile3"), ["PRIMARY", "SYNCHRONIZED", "SYNCHRONIZED"])
            c.ctl("partition", "pile1,pile2|pile3")
            self.assertEqual(c.state("pile1")["blocked"], ["pile3"])
            self.assertEqual(c.state("pile3")["blocked"], ["pile1", "pile2"])
            c.ctl("heal")
            self.assertEqual(c.state("pile3")["blocked"], [])


class PortConflictTest(unittest.TestCase):
    def test_second_cluster_on_the_same_ports_fails_fast(self):
        with MockCluster() as first:
            second = MockCluster()
            second.topology = first.topology
            second.names = first.names
            try:
                started = time.monotonic()
                with self.assertRaises(RuntimeError) as ctx:
                    second.start()
                self.assertLess(time.monotonic() - started, 20)
                self.assertIn("exited with code", str(ctx.exception))
            finally:
                second.stop()
            # the first cluster is unaffected
            self.assertEqual(first.vector("pile1"), ["PRIMARY", "SYNCHRONIZED"])


@unittest.skipUnless(os.path.isfile(MOCK_BINARY) and os.access(MOCK_BINARY, os.X_OK),
                     "the ydbd_mock standalone binary has not been built")
class StandaloneBinaryTest(unittest.TestCase):
    def test_cluster_up_uses_the_standalone_binary_for_children(self):
        with tempfile.TemporaryDirectory(prefix="ydbd_mock_standalone_") as root:
            run_dir = os.path.join(root, "run")
            topology = Topology.with_free_ports(["pile1", "pile2"], 1)
            topology_path = os.path.join(root, "topology.yaml")
            topology.save(topology_path)
            env = dict(os.environ)
            env.pop("YDBD_MOCK_EXE", None)
            env["YDBD_MOCK_RUN_DIR"] = run_dir
            env["YDBD_MOCK_WORK"] = root
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            argv = [MOCK_BINARY, "cluster", "--run-dir", run_dir]
            pids = []
            try:
                up = subprocess.run(argv + ["up", "--topology", topology_path, "--fresh"],
                                    env=env, capture_output=True, text=True, timeout=45)
                self.assertEqual(up.returncode, 0, up.stderr)
                status = subprocess.run(argv + ["status", "--json"], env=env,
                                        capture_output=True, text=True, timeout=15)
                self.assertEqual(status.returncode, 0, status.stderr)
                rows = json.loads(status.stdout)
                pids = [row["pid"] for row in rows]
                self.assertEqual([(row["pile"], row["alive"], row["responsive"]) for row in rows],
                                 [("pile1", True, True), ("pile2", True, True)])
                self.assertEqual(len(set(pids)), 2, "each pile needs its own process")
                prefix = read_json(os.path.join(run_dir, "cluster.json"))["argv_prefix"]
                self.assertEqual(len(prefix), 1)
                self.assertTrue(os.path.samefile(prefix[0], MOCK_BINARY))
            finally:
                down = subprocess.run(argv + ["down"], env=env, capture_output=True, text=True, timeout=20)
                self.assertEqual(down.returncode, 0, down.stderr)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and any(supervisor.pid_alive(pid) for pid in pids):
                time.sleep(0.05)
            self.assertFalse(any(supervisor.pid_alive(pid) for pid in pids),
                             "cluster down must stop both standalone child processes")
            ports = [p.control_port for p in topology.piles] + [n.grpc_port for n in topology.all_nodes()]
            self.assertFalse(any(listening(port) for port in ports))


if __name__ == "__main__":
    unittest.main()
