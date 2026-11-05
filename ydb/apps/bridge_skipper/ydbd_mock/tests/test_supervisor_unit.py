"""Supervisor process bookkeeping that needs no running cluster."""
import argparse
import os
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

from ydb.apps.bridge_skipper.ydbd_mock.lib import supervisor
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Topology


class PidIdentityTest(unittest.TestCase):
    """A stale pid file must never make the supervisor signal (or trust) somebody else's process."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.run_dir = os.path.join(self._tmp.name, "run")
        os.makedirs(os.path.join(self.run_dir, "pids"))
        Topology(["pile1", "pile2"], 3).save(os.path.join(self.run_dir, "topology.yaml"))
        self.cluster = supervisor.Cluster(self.run_dir)
        self._procs = []
        self.addCleanup(self._reap)

    def _reap(self):
        for proc in self._procs:
            if proc.poll() is None:
                proc.kill()
            proc.wait()

    def spawn(self, *argv):
        env = dict(os.environ, Y_PYTHON_ENTRY_POINT=":main")
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"] + list(argv),
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
        self._procs.append(proc)
        time.sleep(0.1)
        return proc

    def write_pid(self, pile, pid):
        with open(os.path.join(self.run_dir, "pids", "%s.pid" % pile), "w") as f:
            f.write("%d\n" % pid)

    def mock_argv(self, pile):
        return ["run", "--topology", os.path.join(self.run_dir, "topology.yaml"), "--pile", pile]

    def test_a_stranger_behind_a_pid_file_is_not_a_running_pile(self):
        stranger = self.spawn()
        self.write_pid("pile1", stranger.pid)
        self.assertFalse(self.cluster.alive("pile1"))

    def test_a_process_of_this_cluster_and_pile_is_alive(self):
        mine = self.spawn(*self.mock_argv("pile1"))
        self.write_pid("pile1", mine.pid)
        self.assertTrue(self.cluster.alive("pile1"))

    def test_the_other_piles_process_does_not_count(self):
        other = self.spawn(*self.mock_argv("pile2"))
        self.write_pid("pile1", other.pid)
        self.assertFalse(self.cluster.alive("pile1"))

    def test_a_process_of_another_run_dir_does_not_count(self):
        elsewhere = self.spawn("run", "--topology", "/somewhere/else/topology.yaml", "--pile", "pile1")
        self.write_pid("pile1", elsewhere.pid)
        self.assertFalse(self.cluster.alive("pile1"))

    def test_down_leaves_a_stranger_alone_and_removes_the_stale_pid_file(self):
        stranger = self.spawn()
        self.write_pid("pile1", stranger.pid)
        supervisor.cmd_down(self.cluster, argparse.Namespace())
        self.assertIsNone(stranger.poll(), "down signalled a process that is not ours")
        self.assertFalse(os.path.exists(os.path.join(self.run_dir, "pids", "pile1.pid")))

    def test_down_stops_our_own_process(self):
        mine = self.spawn(*self.mock_argv("pile1"))
        self.write_pid("pile1", mine.pid)
        supervisor.cmd_down(self.cluster, argparse.Namespace())
        deadline = time.monotonic() + 5
        while supervisor.pid_alive(mine.pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertFalse(supervisor.pid_alive(mine.pid), "down must stop the pile's own process")

    def test_kill_stop_and_cont_refuse_a_stranger(self):
        stranger = self.spawn()
        self.write_pid("pile1", stranger.pid)
        for fn in (supervisor.cmd_kill, supervisor.cmd_stop, supervisor.cmd_cont):
            with self.subTest(fn=fn.__name__):
                with self.assertRaises(supervisor.SupervisorError):
                    fn(self.cluster, argparse.Namespace(pile="pile1"))
        self.assertIsNone(stranger.poll())


class SupervisorRegressionTest(unittest.TestCase):
    def test_up_rejects_empty_csv_pile_segments_before_side_effects(self):
        parser = supervisor.build_parser()
        with tempfile.TemporaryDirectory() as root:
            run = os.path.join(root, "run")
            cluster = supervisor.Cluster(run)
            for piles in ("", ",named", "named,", "named,,other", ","):
                with self.subTest(piles=piles):
                    args = parser.parse_args(["up", "--fresh", "--piles", piles])
                    with mock.patch.object(cluster, "alive") as alive, mock.patch.object(cluster, "spawn") as spawn:
                        with self.assertRaisesRegex(supervisor.SupervisorError, "explicit empty pile name.*--topology"):
                            args.fn(cluster, args)
                        alive.assert_not_called()
                        spawn.assert_not_called()
                    self.assertFalse(os.path.exists(run))

    def test_up_empty_csv_rejection_preserves_an_existing_run(self):
        with tempfile.TemporaryDirectory() as run:
            topology_path = os.path.join(run, "topology.yaml")
            Topology(["named", "other"], 3).save(topology_path)
            with open(topology_path) as source:
                original = source.read()
            cluster = supervisor.Cluster(run)
            args = supervisor.build_parser().parse_args(["up", "--fresh", "--piles", "named,"])
            with self.assertRaises(supervisor.SupervisorError):
                args.fn(cluster, args)
            self.assertEqual(os.listdir(run), ["topology.yaml"])
            with open(topology_path) as source:
                self.assertEqual(source.read(), original)

    def test_up_topology_preserves_an_explicit_empty_pile_name(self):
        with tempfile.TemporaryDirectory() as root:
            topology_path = os.path.join(root, "input.yaml")
            Topology(["named", ""], 3).save(topology_path)
            cluster = supervisor.Cluster(os.path.join(root, "run"))
            args = supervisor.build_parser().parse_args(["up", "--topology", topology_path, "--piles", ""])
            with mock.patch.object(cluster, "alive", return_value=True) as alive, mock.patch.object(
                    cluster, "pid", return_value=42):
                with self.assertRaisesRegex(supervisor.SupervisorError, "already running"):
                    args.fn(cluster, args)
                alive.assert_called_once_with("")
            self.assertFalse(os.path.exists(cluster.run))

    def test_empty_pile_selectors_do_not_mean_all_piles(self):
        cluster = mock.Mock()
        cluster.pile.side_effect = lambda name: name
        cluster.request.return_value = {"pile": ""}
        with mock.patch.object(supervisor, "_print") as output:
            supervisor.cmd_state(cluster, argparse.Namespace(pile=""))
            output.assert_called_once_with({"pile": ""})
        cluster.request.assert_called_once_with("", "GET", "/state")
        cluster.live_piles.assert_not_called()
        with mock.patch.object(supervisor, "read_events", return_value=[]) as read, mock.patch.object(
                supervisor, "_print"):
            supervisor.cmd_events(cluster, argparse.Namespace(
                pile="", after_ts=None, since=None, kind=None, grep=None, limit=None))
            read.assert_called_once_with(cluster, "")
        self.assertEqual(supervisor._groups("|named", ["", "named"]), [[""], ["named"]])
        with self.assertRaises(supervisor.SupervisorError):
            supervisor._groups("|named", ["named"])

    def test_partition_blanks_are_ambiguous_only_with_an_empty_pile(self):
        piles = ["", "named", "other"]
        self.assertEqual(supervisor._groups("named|other|", piles), [["named"], ["other"], [""]])
        for spec in ("named, |other", "named,|other", " |named", "named,,other|"):
            with self.subTest(spec=spec):
                with self.assertRaisesRegex(supervisor.SupervisorError, "ambiguous blank pile name"):
                    supervisor._groups(spec, piles)
        self.assertEqual(supervisor._groups("named, |other", ["named", "other"]), [["named"], ["other"]])
        self.assertEqual(supervisor._groups("named||other", ["named", "other"]), [["named"], ["other"]])

    def test_process_identity_retains_an_explicit_empty_pile(self):
        cluster = supervisor.Cluster("/tmp/mock-empty")
        with mock.patch.object(cluster, "pid", return_value=42), mock.patch.object(
                supervisor, "pid_alive", return_value=True), mock.patch.object(
                supervisor, "command_line", return_value="mock run --topology /tmp/mock-empty/topology.yaml --pile= --state-dir /tmp/mock-empty/state"):
            self.assertTrue(cluster.alive(""))
            self.assertFalse(cluster.alive("named"))

    def test_standalone_binary_argv_does_not_use_python_module_switch(self):
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(
                supervisor.sys, "is_standalone_binary", True, create=True), mock.patch.object(
                supervisor.sys, "executable", "/tmp/ydbd_mock"):
            self.assertEqual(supervisor.argv_prefix(), ["/tmp/ydbd_mock"])

    def test_replacing_topology_names_refuses_before_writing_metadata(self):
        with tempfile.TemporaryDirectory() as root:
            run = os.path.join(root, "run")
            os.makedirs(os.path.join(run, "pids"))
            old_topology = Topology(["pile1", "pile2"], 3)
            old_topology.save(os.path.join(run, "topology.yaml"))
            with open(os.path.join(run, "cluster.json"), "w") as f:
                f.write('{"sentinel": true}')
            cluster = supervisor.Cluster(run)
            # Alive is intentionally keyed to the old topology; both existing processes must be managed.
            cluster.alive = lambda name: name in ("pile1", "pile2")
            incoming = Topology(["alpha", "beta"], 3)
            incoming_path = os.path.join(root, "incoming.yaml")
            incoming.save(incoming_path)
            args = argparse.Namespace(topology=incoming_path, piles="", nodes_per_pile=3,
                                      cluster_name=None, control_base_port=None, grpc_base_port=None,
                                      set=[], fresh=False, log_level="INFO")

            def unexpected_spawn(pile, truncate_logs=False):
                raise supervisor.SupervisorError("unexpected child launch")

            cluster.spawn = unexpected_spawn
            with self.assertRaises(supervisor.SupervisorError):
                supervisor.cmd_up(cluster, args)
            with open(os.path.join(run, "topology.yaml")) as f:
                self.assertEqual(Topology.load(f.name).pile_names, ["pile1", "pile2"])
            with open(os.path.join(run, "cluster.json")) as f:
                self.assertEqual(f.read(), '{"sentinel": true}')

    def test_revived_keeper_split_reapplies_fault_to_every_node(self):
        class FakeCluster:
            topology = Topology(["pile1", "pile2"], 3)

            def _macros(self):
                return [{"kind": "keeper_split", "pile": "pile1", "already": ["mock-pile1-2"]}]

            def request(self, pile, method, path, body):
                calls.append((pile, body))

        calls = []
        supervisor._reapply_macros(FakeCluster(), "pile1")
        if calls and calls[0][1].get("all"):
            self.assertEqual(calls[0][1], {"all": True, "op": "unavailable", "on": True})
        else:
            self.assertEqual([body["label"] for _, body in calls],
                             ["mock-pile1-1", "mock-pile1-2", "mock-pile1-3"])

    def test_failed_heal_preserves_macro_record_for_retry(self):
        with tempfile.TemporaryDirectory() as root:
            run = os.path.join(root, "run")
            os.makedirs(run)
            Topology(["pile1", "pile2"], 3).save(os.path.join(run, "topology.yaml"))
            cluster = supervisor.Cluster(run)
            cluster._save_macros([{"kind": "keeper_split", "pile": "pile1", "already": []}])
            cluster.live_piles = lambda: ["pile1"]
            cluster.alive = lambda pile: True
            attempts = []

            def request(pile, method, path, body):
                if path == "/control":
                    attempts.append(body)
                    if len(attempts) == 1:
                        raise supervisor.SupervisorError("temporarily unavailable")
                return {}

            cluster.request = request
            with mock.patch.object(supervisor, "_scenario"):
                try:
                    supervisor.cmd_heal(cluster, argparse.Namespace())
                except supervisor.SupervisorError:
                    pass
                self.assertTrue(cluster._macros(), "failed undo must stay recorded for a later heal")
                attempts.clear()
                cluster.request = lambda *args, **kwargs: {}
                supervisor.cmd_heal(cluster, argparse.Namespace())
            self.assertEqual(cluster._macros(), [])

    def test_reset_cancels_every_attempted_barrier_and_preserves_prepare_error(self):
        class FakeCluster:
            def piles(self):
                return ["pile1", "pile2"]

            def alive(self, pile):
                return True

            def pid(self, pile):
                return 123

            def request(self, pile, method, path, body=None, timeout=5.0):
                calls.append((pile, path))
                if path == "/reset-prepare" and pile == "pile2":
                    raise supervisor.SupervisorError("lost prepare reply")
                if path == "/reset-cancel" and pile == "pile1":
                    raise supervisor.SupervisorError("cancel also failed")
                return {}

        calls = []
        cluster = FakeCluster()
        with mock.patch.object(supervisor, "_signal"), mock.patch.object(supervisor, "_scenario"):
            with self.assertRaisesRegex(supervisor.SupervisorError, "lost prepare reply"):
                supervisor.cmd_reset(cluster, argparse.Namespace())
        self.assertEqual([pile for pile, path in calls if path == "/reset-cancel"], ["pile1", "pile2"])

    def test_reset_surfaces_resume_failure_after_successful_local_resets(self):
        class FakeCluster:
            def piles(self):
                return ["pile1", "pile2"]

            def alive(self, pile):
                return True

            def pid(self, pile):
                return 123

            def request(self, pile, method, path, body=None, timeout=5.0):
                calls.append((pile, path, body))
                if path == "/state":
                    return {"generation": 4, "max_generation": 9}
                if path == "/reset-cancel" and pile == "pile2":
                    raise supervisor.SupervisorError("cancel unavailable")
                return {}

            def _save_macros(self, macros):
                pass

        calls = []
        cluster = FakeCluster()
        with mock.patch.object(supervisor, "_signal"), mock.patch.object(supervisor, "_scenario"), mock.patch.object(
                supervisor, "_print"):
            with self.assertRaisesRegex(supervisor.SupervisorError, "could not resume piles"):
                supervisor.cmd_reset(cluster, argparse.Namespace())
        reset_bodies = [body for _, path, body in calls if path == "/reset-local"]
        self.assertEqual(reset_bodies, [{"generation": 10}, {"generation": 10}])
        self.assertEqual([pile for pile, path, _ in calls if path == "/reset-cancel"], ["pile1", "pile2"])


if __name__ == "__main__":
    unittest.main()
