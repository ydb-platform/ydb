"""Helpers for the e2e tests: a real multi-process cluster, the supervisor CLI, the guarded ydb CLI.

The e2e tests start real `ydbd_mock run` processes through `ydbd_mock cluster up`, in a temporary
run directory and on free ports, so they never collide with a cluster you started by hand.
They run from the virtualenv (`scripts/test.sh -e2e`); set YDBD_MOCK_EXE to test the ya binary.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import typing
import unittest

from ydb.apps.bridge_skipper.ydbd_mock.lib import supervisor
from ydb.apps.bridge_skipper.ydbd_mock.lib.topology import Policies, Timings, Topology

HERE = os.path.dirname(os.path.abspath(__file__))
MOCK_DIR = os.path.dirname(HERE)
SCRIPTS = os.path.join(MOCK_DIR, "scripts")
REPO = os.path.abspath(os.path.join(MOCK_DIR, "..", "..", "..", ".."))
WORK = os.environ.get("YDBD_MOCK_WORK") or os.path.join(os.path.expanduser("~"), "ydbd_mock_work")

REAL_YDB = os.environ.get("YDBD_MOCK_REAL_YDB") or os.path.expanduser("~/ydb/bin/ydb")


def real_cli_available() -> bool:
    return os.path.isfile(REAL_YDB) and os.access(REAL_YDB, os.X_OK)


def demo_python() -> str:
    return os.path.join(WORK, "venv", "bin", "python")


def fast_timings(**over) -> Timings:
    """Follow-ups in half a second: the CLI and the demo do not care about the exact values."""
    values = dict(promote_delay=0.5, suspend_delay=0.5, connect_delay=0.5, sync_step_delay=0.5)
    values.update(over)
    return Timings(**values)


class Result(typing.NamedTuple):
    code: int
    out: str
    err: str
    seconds: float

    def json(self):
        return json.loads(self.out)


class MockCluster:
    """A cluster in a temp run dir. Use as a context manager or call start()/stop()."""

    def __init__(self, piles: int = 2, nodes: int = 3, timings: typing.Optional[Timings] = None,
                 policies: typing.Optional[Policies] = None) -> None:
        self.names = ["pile%d" % (i + 1) for i in range(piles)]
        self.topology = Topology.with_free_ports(
            self.names, nodes, timings=timings or fast_timings(), policies=policies or Policies())
        self.run_dir = tempfile.mkdtemp(prefix="ydbd_mock_e2e_")
        self.started = False

    # ----- lifecycle -----

    def start(self) -> "MockCluster":
        topo_file = os.path.join(self.run_dir, "topology.in.yaml")
        self.topology.save(topo_file)
        result = self.ctl("up", "--topology", topo_file, "--fresh")
        if result.code != 0:
            raise RuntimeError("cluster up failed: %s %s" % (result.out, result.err))
        self.started = True
        return self

    def stop(self) -> None:
        if os.path.isdir(self.run_dir):
            self.ctl("down")
            shutil.rmtree(self.run_dir, ignore_errors=True)
        self.started = False

    def __enter__(self) -> "MockCluster":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    # ----- commands -----

    def env(self) -> dict:
        env = dict(os.environ)
        env["YDBD_MOCK_RUN_DIR"] = self.run_dir
        env["YDBD_MOCK_REAL_YDB"] = REAL_YDB
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        env.setdefault("PYTHONPATH", os.pathsep.join([REPO, os.path.join(WORK, "gen")]))
        return env

    def ctl(self, *args: str, timeout: float = 60.0) -> Result:
        argv = supervisor.argv_prefix() + ["cluster", "--run-dir", self.run_dir] + list(args)
        return _run(argv, self.env(), timeout)

    def cli(self, *args: str, endpoint: typing.Optional[str] = None, timeout: float = 60.0,
            stdin: typing.Optional[str] = None) -> Result:
        """The guarded ydb CLI. `endpoint` defaults to the first node of pile1."""
        endpoint = endpoint or "grpc://%s:2135" % self.topology.pile(0).nodes[0].label
        argv = [sys.executable, os.path.join(SCRIPTS, "ydb_shim.py"), "-e", endpoint] + list(args)
        return _run(argv, self.env(), timeout, stdin)

    def bridge(self, *args: str, pile: str = "pile1", assume_yes: bool = True, timeout: float = 60.0,
               stdin: typing.Optional[str] = None) -> Result:
        endpoint = "grpc://%s:2135" % self.topology.pile(pile).nodes[0].label
        pre = ["--assume-yes"] if assume_yes else []
        return self.cli(*pre, "admin", "cluster", "bridge", *args, endpoint=endpoint, timeout=timeout, stdin=stdin)

    def bridge_list(self, pile: str = "pile1") -> dict:
        result = self.bridge("list", "--format=json", pile=pile, assume_yes=False)
        if result.code != 0:
            raise RuntimeError("bridge list failed: %s" % result.err)
        return result.json()

    # ----- state -----

    def state(self, pile: str) -> dict:
        return json.loads(self.ctl("state", pile).out)

    def vector(self, pile: str) -> typing.List[str]:
        return supervisor.model_vector(self.state(pile))

    def generation(self, pile: str) -> int:
        return self.state(pile)["generation"]

    def events(self, *args: str) -> typing.List[dict]:
        result = self.ctl("events", *args)
        return [json.loads(line) for line in result.out.splitlines() if line.strip()]

    def commits(self, after_ts: typing.Optional[float] = None) -> typing.List[dict]:
        args = ["--kind", "commit"]
        if after_ts is not None:
            args += ["--after-ts", repr(after_ts)]
        return self.events(*args)

    def updates(self, after_ts: typing.Optional[float] = None) -> typing.List[dict]:
        args = ["--kind", "rpc.start", "--grep", "UpdateClusterState"]
        if after_ts is not None:
            args += ["--after-ts", repr(after_ts)]
        return self.events(*args)

    def now(self) -> float:
        return float(self.ctl("now").out)

    def wait(self, predicate, timeout: float = 30.0, step: float = 0.2, message: str = "condition"):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                last = predicate()
                if last:
                    return last
            except Exception as e:  # the cluster may be mid-change
                last = e
            time.sleep(step)
        raise AssertionError("timed out waiting for %s (last: %r)" % (message, last))

    def wait_vector(self, pile: str, expected: typing.List[str], timeout: float = 30.0) -> None:
        self.wait(lambda: self.vector(pile) == expected, timeout, message="%s vector %s" % (pile, expected))


def _run(argv, env, timeout, stdin=None) -> Result:
    started = time.monotonic()
    try:
        proc = subprocess.run(argv, env=env, capture_output=True, text=True, timeout=timeout, input=stdin)
        return Result(proc.returncode, proc.stdout, proc.stderr, time.monotonic() - started)
    except subprocess.TimeoutExpired as e:
        out = e.stdout.decode() if isinstance(e.stdout, bytes) else (e.stdout or "")
        err = e.stderr.decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
        return Result(-9, out, err + "\n(timeout after %.0fs)" % timeout, time.monotonic() - started)


class E2ECase(unittest.TestCase):
    """One cluster per test class (setUpClass), reset between tests."""

    PILES = 2
    NODES = 3
    TIMINGS: typing.Optional[Timings] = None
    POLICIES: typing.Optional[Policies] = None
    cluster: MockCluster

    @classmethod
    def setUpClass(cls):
        cls.cluster = MockCluster(cls.PILES, cls.NODES, cls.TIMINGS, cls.POLICIES)
        cls.cluster.start()

    @classmethod
    def tearDownClass(cls):
        cls.cluster.stop()

    def setUp(self):
        result = self.cluster.ctl("reset")
        self.assertEqual(result.code, 0, result.err)
