"""The real ydb CLI (through the guarded shim) against real mock instances.

Needs the ydb CLI at ~/ydb/bin/ydb (or YDBD_MOCK_REAL_YDB). The CLI only ever talks to the
mock: the shim refuses every other endpoint.
"""
import subprocess
import sys
import unittest

import yaml

import harness
from harness import E2ECase, MockCluster, Result, fast_timings

NEEDS_CLI = unittest.skipUnless(harness.real_cli_available(), "the real ydb CLI is not installed")

PRIMARY_ERROR = "Request must result in a state with one primary pile"


def vector(listing: dict):
    return [(p["pile_name"], p["state"]) for p in listing["piles"]]


@NEEDS_CLI
class CliSmokeTest(E2ECase):
    def test_config_fetch_is_one_document_with_the_hosts(self):
        result = self.cluster.cli("admin", "cluster", "config", "fetch")
        self.assertEqual(result.code, 0, result.err)
        documents = list(yaml.safe_load_all(result.out))
        self.assertEqual(len(documents), 1)
        doc = documents[0]
        self.assertEqual(doc["metadata"]["kind"], "MainConfig")
        hosts = doc["config"]["hosts"]
        self.assertEqual([h["host"] for h in hosts],
                         ["mock-pile1-1", "mock-pile1-2", "mock-pile1-3",
                          "mock-pile2-1", "mock-pile2-2", "mock-pile2-3"])
        self.assertEqual([h["location"]["bridge_pile_name"] for h in hosts], ["pile1"] * 3 + ["pile2"] * 3)

    def test_bridge_list_in_three_formats(self):
        c = self.cluster
        listing = c.bridge("list", "--format=json", assume_yes=False).json()
        generation = listing["generation"]
        self.assertEqual(vector(listing), [("pile1", "PRIMARY"), ("pile2", "SYNCHRONIZED")])
        csv = c.bridge("list", "--format=csv", assume_yes=False).out.splitlines()
        self.assertEqual(csv, ["pile_name,state,generation",
                               "pile1,PRIMARY,%d" % generation, "pile2,SYNCHRONIZED,%d" % generation])
        pretty = c.bridge("list", assume_yes=False).out.splitlines()
        self.assertEqual(pretty, ["Pile pile1: PRIMARY", "Pile pile2: SYNCHRONIZED", "Generation: %d" % generation])

    def test_every_node_of_every_pile_answers(self):
        for pile in self.cluster.names:
            for node in self.cluster.topology.pile(pile).nodes:
                result = self.cluster.cli("admin", "cluster", "bridge", "list", "--format=json",
                                          endpoint="grpc://%s:2135" % node.label)
                self.assertEqual(result.code, 0, "%s: %s" % (node.label, result.err))
                self.assertEqual(vector(result.json()), [("pile1", "PRIMARY"), ("pile2", "SYNCHRONIZED")])

    def test_healthcheck_as_the_demo_calls_it(self):
        result = self.cluster.cli("-d", "", "monitoring", "healthcheck", "-v", "--format=json", "--no-merge",
                                  "--no-cache", "--timeout", "9000")
        self.assertEqual(result.code, 0, result.err)
        data = result.json()
        self.assertEqual(data["self_check_result"], "GOOD")
        self.assertFalse(data.get("issue_log"))
        self.assertEqual(data["location"]["host"], "mock-pile1-1")

    def test_healthcheck_blames_a_pile_through_the_demo_issue_filter(self):
        c = self.cluster
        c.ctl("fault", "mock-pile1-1", "--op", "storage", "--piles", "pile2")
        data = c.cli("-d", "", "monitoring", "healthcheck", "-v", "--format=json", "--no-merge", "--no-cache",
                     "--timeout", "9000").json()
        self.assertEqual(data["self_check_result"], "MAINTENANCE_REQUIRED")
        issues = [i for i in data["issue_log"]
                  if "GROUP" in i["type"] and i["status"] == "RED"
                  and i["location"]["storage"]["pool"]["group"]["pile"]["name"]]
        self.assertEqual([i["location"]["storage"]["pool"]["group"]["pile"]["name"] for i in issues], ["pile2"])

    def test_the_help_of_the_healthcheck_lists_no_merge(self):
        result = _run_shim("monitoring", "healthcheck", "--help", env=self.cluster.env())
        self.assertEqual(result.code, 0, result.err)
        self.assertIn("--no-merge", result.out)

    def test_unimplemented_calls_are_recorded(self):
        t = self.cluster.now()
        result = self.cluster.cli("-d", "/Root", "discovery", "whoami")
        self.assertNotEqual(result.code, 0)
        self.assertIn("UNIMPLEMENTED", result.out + result.err)
        found = self.cluster.events("--after-ts", repr(t), "--kind", "rpc.unimplemented")
        self.assertEqual([(e["method"], e["registered"]) for e in found],
                         [("/Ydb.Discovery.V1.DiscoveryService/WhoAmI", True)])
        self.cluster.cli("-d", "/Root", "scheme", "ls")
        found = self.cluster.events("--after-ts", repr(t), "--kind", "rpc.unimplemented")
        self.assertEqual(found[-1]["method"], "/Ydb.Scheme.V1.SchemeService/ListDirectory")
        self.assertFalse(found[-1]["registered"])

    def test_calls_are_fast(self):
        for args in (("admin", "cluster", "config", "fetch"), ("admin", "cluster", "bridge", "list")):
            result = self.cluster.cli(*args)
            self.assertEqual(result.code, 0, result.err)
            self.assertLess(result.seconds, 3.0, args)

    def test_the_shim_refuses_everything_but_the_mock(self):
        env = self.cluster.env()
        for argv in (
            ["-e", "grpc://example.com:2135", "admin", "cluster", "bridge", "list"],
            ["-e", "grpc://127.0.0.1:2135", "admin", "cluster", "bridge", "list"],
            ["admin", "cluster", "bridge", "list"],
            ["-p", "default", "admin", "cluster", "bridge", "list"],
            ["-e", "http://mock-pile1-1:2135", "admin", "cluster", "bridge", "list"],
            ["--endpoint=grpc://[::1]:2135", "admin", "cluster", "bridge", "list"],
        ):
            with self.subTest(argv=argv):
                result = _run_shim(*argv, env=env)
                self.assertEqual(result.code, 3, result.out + result.err)
                self.assertIn("refusing", result.err)

    def test_the_shim_accepts_label_and_explicit_mock_endpoints(self):
        node = self.cluster.topology.node_by_label("mock-pile2-3")
        for endpoint in ("grpc://mock-pile2-3:2135", "grpc://mock-pile2-3", "mock-pile2-3:2135", node.endpoint,
                         "grpc://" + node.endpoint):
            with self.subTest(endpoint=endpoint):
                result = self.cluster.cli("admin", "cluster", "bridge", "list", "--format=json", endpoint=endpoint)
                self.assertEqual(result.code, 0, result.err)


@NEEDS_CLI
class CliStateMachineTest(E2ECase):
    """Scenario S2 of MANUAL_TEST_SCENARIO.ai.md, automated."""

    def settle(self, pile="pile2", state="SYNCHRONIZED"):
        self.cluster.wait(lambda: dict(vector(self.cluster.bridge_list())).get(pile) == state, 20,
                          message="%s %s" % (pile, state))

    def test_failover_twice_then_rejoin_resyncs(self):
        c = self.cluster
        g = c.bridge_list()["generation"]
        t = c.now()
        self.assertEqual(c.bridge("failover", "--pile", "pile2").code, 0)
        listing = c.bridge_list()
        self.assertEqual((listing["generation"], vector(listing)), (g + 1, [("pile1", "PRIMARY"), ("pile2", "DISCONNECTED")]))
        self.assertEqual(c.bridge("failover", "--pile", "pile2").code, 0)
        self.assertEqual(c.bridge_list()["generation"], g + 2, "a no-op commit still advances the generation")
        self.assertEqual(c.bridge("rejoin", "--pile", "pile2").code, 0)
        self.assertEqual(vector(c.bridge_list()), [("pile1", "PRIMARY"), ("pile2", "NOT_SYNCHRONIZED")])
        self.settle()
        causes = [(e["cause"], e["generation"]) for e in c.commits(t) if e["pile"] == "pile1"]
        self.assertEqual(causes, [("admin", g + 1), ("admin", g + 2), ("admin", g + 3),
                                  ("connect", g + 4), ("synchronized", g + 5)])
        self.assertEqual(c.bridge_list()["generation"], g + 5)
        self.assertEqual(c.bridge_list(pile="pile2")["generation"], g + 5)

    def test_error_cases_do_not_change_the_generation(self):
        c = self.cluster
        g = c.bridge_list()["generation"]
        for args in (("takedown", "--pile", "pile1", "--new-primary", "pile2"),
                     ("switchover", "--new-primary", "pile1"),
                     ("failover", "--pile", "pile1")):
            with self.subTest(args=args):
                result = c.bridge(*args)
                self.assertNotEqual(result.code, 0)
                self.assertIn(PRIMARY_ERROR, result.out + result.err)
        self.assertEqual(c.bridge_list()["generation"], g)

    def test_switchover_there_and_back(self):
        c = self.cluster
        g = c.bridge_list()["generation"]
        self.assertEqual(c.bridge("switchover", "--new-primary", "pile2").code, 0)
        self.assertEqual(vector(c.bridge_list()), [("pile1", "PRIMARY"), ("pile2", "PROMOTED")])
        c.wait(lambda: vector(c.bridge_list()) == [("pile1", "SYNCHRONIZED"), ("pile2", "PRIMARY")], 10,
               message="promote follow-up")
        self.assertEqual(c.bridge_list()["generation"], g + 2)
        self.assertEqual(c.bridge("switchover", "--new-primary", "pile1").code, 0)
        c.wait(lambda: vector(c.bridge_list()) == [("pile1", "PRIMARY"), ("pile2", "SYNCHRONIZED")], 10,
               message="promote back")
        self.assertEqual(c.bridge_list()["generation"], g + 4)

    def test_takedown_suspends_then_disconnects_and_a_suspended_pile_cannot_be_failed_over(self):
        c = self.cluster
        g = c.bridge_list()["generation"]
        self.assertEqual(c.bridge("takedown", "--pile", "pile2").code, 0)
        self.assertEqual(vector(c.bridge_list()), [("pile1", "PRIMARY"), ("pile2", "SUSPENDED")])
        result = c.bridge("failover", "--pile", "pile2")
        self.assertNotEqual(result.code, 0)
        self.assertIn("cannot update suspended pile", result.out + result.err)
        c.wait(lambda: vector(c.bridge_list()) == [("pile1", "PRIMARY"), ("pile2", "DISCONNECTED")], 10,
               message="suspend follow-up")
        self.assertEqual(c.bridge_list()["generation"], g + 2)

    def test_without_assume_yes_nothing_is_sent(self):
        c = self.cluster
        g = c.bridge_list()["generation"]
        t = c.now()
        result = c.bridge("failover", "--pile", "pile2", assume_yes=False, stdin="n\n")
        self.assertEqual(c.bridge_list()["generation"], g)
        self.assertEqual(c.updates(t), [], result.out + result.err)

    def test_updates_are_logged_with_their_summary(self):
        c = self.cluster
        t = c.now()
        c.bridge("failover", "--pile", "pile2")
        started = c.updates(t)
        self.assertEqual(len(started), 1)
        self.assertIn("pile2=DISCONNECTED", started[0]["summary"])
        self.assertEqual(started[0]["node"], "mock-pile1-1")

    def test_one_instance_sees_the_commit_of_the_other(self):
        c = self.cluster
        g = c.bridge_list()["generation"]
        c.bridge("failover", "--pile", "pile1", "--new-primary", "pile2", pile="pile2")
        c.wait(lambda: c.generation("pile1") == g + 1, 10, message="pile1 follows")
        self.assertEqual(c.vector("pile1"), ["DISCONNECTED", "PRIMARY"])
        self.assertEqual(vector(c.bridge_list(pile="pile2")), [("pile1", "DISCONNECTED"), ("pile2", "PRIMARY")])
        # a node of a DISCONNECTED pile serves no gRPC (run.cpp), so the CLI cannot ask pile1 any more
        c.wait(lambda: c.bridge("list", "--format=json", pile="pile1", assume_yes=False, timeout=30).code != 0,
               10, message="pile1 stops serving")

    def test_dead_pile_is_refused_quickly(self):
        c = self.cluster
        c.ctl("kill", "pile2")
        result = c.bridge("list", "--format=json", pile="pile2", assume_yes=False, timeout=60)
        self.assertNotEqual(result.code, 0)
        self.assertLess(result.seconds, 5.0, "a closed port is refused at once: %.2fs" % result.seconds)
        self.assertEqual(c.bridge("list", "--format=json", pile="pile1", assume_yes=False).code, 0)


@NEEDS_CLI
class ThreePilesCliTest(unittest.TestCase):
    """Scenario S3: omitted synchronization state survives unrelated admin changes."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = MockCluster(piles=3, timings=fast_timings(sync_step_delay=1.5)).start()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.stop()

    def stored(self, pile="pile3"):
        state = self.cluster.state("pile1")
        return next(p["stored"] for p in state["piles"] if p["name"] == pile)

    def test_second_state_change_while_a_pile_resyncs(self):
        c = self.cluster
        self.assertEqual(c.ctl("reset").code, 0)
        self.assertEqual(c.bridge("failover", "--pile", "pile3").code, 0)
        self.assertEqual(c.bridge("rejoin", "--pile", "pile3").code, 0)
        c.wait(lambda: self.stored() == "NOT_SYNCHRONIZED_2", 10, step=0.05, message="pile3 stored _2")
        g = c.generation("pile1")
        result = c.bridge("failover", "--pile", "pile2")
        self.assertEqual(result.code, 0, result.out + result.err)
        self.assertEqual(self.stored(), "NOT_SYNCHRONIZED_2")
        self.assertEqual(c.generation("pile1"), g + 1)
        c.wait(lambda: self.stored() == "SYNCHRONIZED", 30, message="pile3 synchronized")
        self.assertEqual(c.bridge("failover", "--pile", "pile2").code, 0)
        self.assertEqual(vector(c.bridge_list()),
                         [("pile1", "PRIMARY"), ("pile2", "DISCONNECTED"), ("pile3", "SYNCHRONIZED")])

    def test_switchover_preserves_an_omitted_resyncing_pile(self):
        c = self.cluster
        self.assertEqual(c.ctl("reset").code, 0)
        c.bridge("failover", "--pile", "pile3")
        c.bridge("rejoin", "--pile", "pile3")
        c.wait(lambda: self.stored() == "NOT_SYNCHRONIZED_2", 10, step=0.05, message="pile3 stored _2")
        self.assertEqual(c.bridge("switchover", "--new-primary", "pile2").code, 0)
        self.assertEqual(self.stored(), "NOT_SYNCHRONIZED_2")
        c.wait(lambda: self.stored() == "SYNCHRONIZED", 40, message="pile3 synchronized again")
        c.wait(lambda: c.vector("pile1") == ["SYNCHRONIZED", "PRIMARY", "SYNCHRONIZED"], 10, message="promoted")


def _run_shim(*argv: str, env: dict) -> Result:
    import os
    import time
    shim = os.path.join(harness.SCRIPTS, "ydb_shim.py")
    started = time.monotonic()
    proc = subprocess.run([sys.executable, shim] + list(argv), env=env, capture_output=True, text=True, timeout=60)
    return Result(proc.returncode, proc.stdout, proc.stderr, time.monotonic() - started)


if __name__ == "__main__":
    unittest.main()
