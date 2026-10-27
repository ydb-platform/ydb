"""The unmodified bridge skipper demo against real mock instances (scenarios S4-S9, S11).

The demo is started through scripts/demo.sh, which passes `--ydb scripts/ydb_shim.py`. Its sources
are never touched; one test checks that. Each scenario takes 40-90 s because the demo waits 10 s
at start and its health data has a 10 s TTL.
"""
import os
import re
import signal
import subprocess
import time
import unittest

import harness
from harness import E2ECase, MockCluster

NEEDS_DEMO = unittest.skipUnless(
    harness.real_cli_available() and os.path.exists(harness.demo_python()),
    "needs the real ydb CLI and the virtualenv (scripts/setup_venv.sh)")

STARTUP = 40.0     # demo start (10 s sleep) until the first decision, with margin
DETECT = 90.0      # failure until the failover command
QUIET = 20.0       # how long "nothing happens" is observed


class Demo:
    def __init__(self, cluster: MockCluster, extra=(), endpoint="mock-pile1-1"):
        self.cluster = cluster
        self.log_path = os.path.join(cluster.run_dir, "logs", "demo.log")
        self.state_path = os.path.join(cluster.run_dir, "skipper_state.json")
        self.extra = list(extra)
        self.endpoint = endpoint
        self.proc = None

    def start(self, fresh=True) -> "Demo":
        argv = [os.path.join(harness.SCRIPTS, "demo.sh")]
        if fresh:
            argv.append("--fresh")
        argv += ["-s", self.state_path, "-e", self.endpoint] + self.extra
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
        log = open(self.log_path, "w")
        self.proc = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                     env=self.cluster.env(), start_new_session=True)
        log.close()
        return self

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            try:
                os.killpg(self.proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                os.killpg(self.proc.pid, signal.SIGKILL)
                self.proc.wait()

    def log(self) -> str:
        try:
            with open(self.log_path) as f:
                return f.read()
        except OSError:
            return ""

    def info(self) -> str:
        return "\n".join(line[:300] for line in self.log().splitlines() if " DEBUG " not in line)

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def wait_log(self, pattern: str, timeout: float) -> re.Match:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            m = re.search(pattern, self.log())
            if m:
                return m
            if self.proc.poll() is not None:
                break
            time.sleep(0.5)
        raise AssertionError("demo log never matched %r:\n%s" % (pattern, self.info()[-3000:]))


@NEEDS_DEMO
class DemoCase(E2ECase):
    demo: Demo

    def setUp(self):
        super().setUp()
        self.demo = Demo(self.cluster)

    def tearDown(self):
        self.demo.stop()

    def steady(self):
        self.demo.start()
        self.demo.wait_log(r"All is good", STARTUP)

    def quiet_updates(self, t, seconds=QUIET):
        time.sleep(seconds)
        return self.cluster.updates(t)


class DemoSteadyTest(DemoCase):
    def test_s4_steady_state_sends_nothing(self):
        t = self.cluster.now()
        self.steady()
        self.assertIn("Piles host counts: pile1: 3, pile2: 3", self.demo.log())
        self.assertEqual(self.quiet_updates(t, 15), [], self.demo.info())
        self.assertTrue(self.demo.alive())
        self.assertNotIn("CRITICAL", self.demo.log())
        self.assertNotIn(" ERROR ", self.demo.log())

    def test_demo_sources_are_untouched(self):
        repo = harness.REPO
        for args in (["diff", "--stat"], ["status", "--short"]):
            out = subprocess.run(["git", "-C", repo] + args + ["--", "ydb/apps/bridge_skipper_demo"],
                                 capture_output=True, text=True).stdout
            self.assertEqual(out.strip(), "", out)


class DemoFailureTest(DemoCase):
    def test_s5_killed_follower_is_failed_over_once_then_resynced_by_rejoin(self):
        c = self.cluster
        g = c.generation("pile1")
        self.steady()
        t = c.now()
        c.ctl("kill", "pile2")
        self.demo.wait_log(r"failover command executed successfully", DETECT)
        updates = c.updates(t)
        self.assertEqual(len(updates), 1, updates)
        self.assertEqual(updates[0]["pile"], "pile1")
        self.assertIn("updates=[pile2=DISCONNECTED]", updates[0]["summary"])
        commits = [e for e in c.commits(t) if e["pile"] == "pile1"]
        self.assertEqual([(e["generation"], e["cause"]) for e in commits], [(g + 1, "admin")])
        self.assertEqual(self.quiet_updates(t), updates, "the demo is quiet after the failover")
        listing = c.bridge_list()
        self.assertEqual((listing["generation"], [p["state"] for p in listing["piles"]]),
                         (g + 1, ["PRIMARY", "DISCONNECTED"]))
        self.assertIn("--pile pile2", self.demo.log())
        self.assertIn("ydb_shim.py -e grpc://mock-pile1-", self.demo.log(), "the failover went through the shim")
        self.assertNotIn("--new-primary", self.demo.log().split("failover command executed")[0].split("Executing command")[-1])

        # the dead pile comes back with its old replica; it must not be pulled forward
        t2 = c.now()
        c.ctl("revive", "pile2")
        c.wait(lambda: c.generation("pile2") == g, 15, message="pile2 with its stale replica")
        time.sleep(8)
        self.assertEqual(c.generation("pile2"), g, "a DISCONNECTED pile is never pulled forward")
        self.assertEqual(c.generation("pile1"), g + 1)
        self.assertEqual(c.updates(t2), [], "the demo ignores the stale pile")
        self.assertTrue(c.events("--after-ts", repr(t2), "--kind", "pull.refused"))

        # an operator rejoins it, and the demo does not interfere with the resync
        self.assertEqual(c.bridge("rejoin", "--pile", "pile2").code, 0)
        c.wait(lambda: c.generation("pile2") == g + 4 and c.vector("pile2") == ["PRIMARY", "SYNCHRONIZED"], 30,
               message="pile2 resynced")
        self.assertEqual([e["summary"] for e in c.updates(t2)].__len__(), 1, "only the operator's rejoin")
        self.assertTrue(self.demo.alive())

    def test_s6_killed_primary_is_replaced(self):
        c = self.cluster
        g = c.generation("pile1")
        self.steady()
        t = c.now()
        c.ctl("kill", "pile1")
        self.demo.wait_log(r"failover command executed successfully", DETECT)
        updates = c.updates(t)
        self.assertEqual(len(updates), 1, updates)
        self.assertEqual(updates[0]["pile"], "pile2")
        self.assertIn("pile1=DISCONNECTED", updates[0]["summary"])
        self.assertIn("pile2=PRIMARY", updates[0]["summary"])
        listing = c.bridge_list(pile="pile2")
        self.assertEqual((listing["generation"], [(p["pile_name"], p["state"]) for p in listing["piles"]]),
                         (g + 1, [("pile1", "DISCONNECTED"), ("pile2", "PRIMARY")]))
        self.assertEqual(self.quiet_updates(t), updates)

    def test_s7_partition_makes_the_demo_trust_the_primary(self):
        c = self.cluster
        g = c.generation("pile1")
        self.steady()
        t = c.now()
        c.ctl("partition", "pile1|pile2")
        self.demo.wait_log(r"failover command executed successfully", DETECT)
        updates = c.updates(t)
        self.assertEqual(len(updates), 1, updates)
        self.assertEqual(updates[0]["pile"], "pile1")
        self.assertIn("updates=[pile2=DISCONNECTED]", updates[0]["summary"])
        self.assertEqual(c.generation("pile1"), g + 1)
        self.assertEqual(c.generation("pile2"), g, "the cut link keeps pile2 stale")
        self.assertTrue(c.events("--after-ts", repr(t), "--kind", "push.failed"))
        c.ctl("heal")
        time.sleep(QUIET)
        self.assertEqual(c.generation("pile2"), g, "a DISCONNECTED pile stays stale after the heal")
        self.assertEqual(c.updates(t), updates)
        self.assertTrue(self.demo.alive())

    def test_s8_keeper_split_is_not_a_cluster_failure(self):
        c = self.cluster
        self.steady()
        t = c.now()
        c.ctl("keeper-split", "pile1")
        time.sleep(40)
        self.assertEqual(c.updates(t), [], self.demo.info())
        self.assertTrue(self.demo.alive())
        self.assertEqual(c.vector("pile1"), ["PRIMARY", "SYNCHRONIZED"])
        c.ctl("heal")

    def test_s9_an_older_replica_is_ignored_and_a_newer_one_is_adopted(self):
        c = self.cluster
        self.assertEqual(c.bridge("switchover", "--new-primary", "pile2").code, 0)
        c.wait(lambda: c.vector("pile1") == ["SYNCHRONIZED", "PRIMARY"], 10, message="switched")
        self.assertEqual(c.bridge("switchover", "--new-primary", "pile1").code, 0)
        c.wait(lambda: c.vector("pile1") == ["PRIMARY", "SYNCHRONIZED"], 10, message="switched back")
        g = c.generation("pile1")
        t = c.now()
        self.steady()
        c.ctl("replica", "pile2", "--generation", "2", "--states", "pile1=PRIMARY,pile2=SYNCHRONIZED", "--detach")
        time.sleep(25)
        self.assertEqual(c.bridge_list(pile="pile2")["generation"], 2)
        self.assertEqual(c.bridge_list(pile="pile1")["generation"], g)
        self.assertEqual(c.updates(t), [])
        self.assertTrue(self.demo.alive())
        c.ctl("replica", "pile2", "--clear")
        c.ctl("replica", "pile2", "--generation", str(g + 50), "--states", "pile1=DISCONNECTED,pile2=PRIMARY",
              "--detach")
        self.demo.wait_log(r"gen=%d, primary=pile2" % (g + 50), 40)
        time.sleep(10)
        self.assertEqual(c.updates(t), [], "pile1 is already DISCONNECTED in the adopted map")
        self.assertTrue(self.demo.alive())

    def test_s10_a_silent_node_stalls_the_demo_but_does_not_make_it_fail_over(self):
        c = self.cluster
        self.steady()
        t = c.now()
        c.ctl("fault", "pile1", "--op", "silence", "--max", "25", "--methods", "GetClusterState")
        time.sleep(45)
        self.assertEqual(c.updates(t), [], self.demo.info())
        self.assertTrue(self.demo.alive())
        self.assertTrue(c.events("--after-ts", repr(t), "--kind", "rpc.cancelled") or
                        c.events("--after-ts", repr(t), "--kind", "rpc.end"))


@NEEDS_DEMO
class DemoThreePilesTest(unittest.TestCase):
    def test_s11_three_piles_the_demo_refuses_to_start(self):
        with MockCluster(piles=3) as c:
            demo = Demo(c).start()
            try:
                demo.proc.wait(60)
            finally:
                demo.stop()
            self.assertEqual(demo.proc.returncode, 2, demo.info())
            self.assertIn("more than 2 piles is not supported", demo.log())
            self.assertEqual(c.updates(), [])

    def test_the_demo_exits_when_nothing_resolves(self):
        with MockCluster() as c:
            demo = Demo(c, endpoint="mock-pile1-1")
            c.ctl("kill", "pile1")
            c.ctl("kill", "pile2")
            demo.start()
            try:
                demo.proc.wait(60)
            finally:
                demo.stop()
            self.assertEqual(demo.proc.returncode, 2, demo.info())
            self.assertIn("No piles resolved", demo.log())


if __name__ == "__main__":
    unittest.main()
