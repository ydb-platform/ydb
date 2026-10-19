"""Small Linux process-lifecycle regressions; no compiler or large allocation."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest


RUNNER = Path(__file__).with_name("run_limited.py")


class BuildGuardTest(unittest.TestCase):
    def test_child_receives_limits(self):
        code = """
import json, os, resource
print(json.dumps({
    'cpus': len(os.sched_getaffinity(0)),
    'data': resource.getrlimit(resource.RLIMIT_DATA),
    'address': resource.getrlimit(resource.RLIMIT_AS),
    'core': resource.getrlimit(resource.RLIMIT_CORE),
}))
"""
        result = subprocess.run(
            [sys.executable, str(RUNNER), sys.executable, "-c", code],
            env={**os.environ, "RBO_BUILD_CPUS": "1"},
            capture_output=True, text=True, timeout=5, check=True,
        )
        limits = next(json.loads(line) for line in result.stdout.splitlines() if line.startswith("{"))
        self.assertEqual(limits["cpus"], 1)
        self.assertEqual(limits["data"], [8 * 1024**3] * 2)
        self.assertEqual(limits["address"], [64 * 1024**3] * 2)
        self.assertEqual(limits["core"], [0, 0])

    def test_preserves_failure_status(self):
        result = subprocess.run(
            [sys.executable, str(RUNNER), sys.executable, "-c", "raise SystemExit(7)"],
            capture_output=True, timeout=5,
        )
        self.assertEqual(result.returncode, 7)

    def check_descendant_cleanup(self, interrupt):
        with tempfile.TemporaryDirectory(prefix="rbo-build-guard-") as directory:
            pidfile = Path(directory) / "child.pid"
            logfile = Path(directory) / "output.log"
            child = (
                "import os,pathlib,signal,time; "
                "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                f"pathlib.Path({str(pidfile)!r}).write_text(str(os.getpid())); "
                "time.sleep(30)"
            )
            parent = (
                "import subprocess,sys,time; "
                f"subprocess.Popen([sys.executable, '-c', {child!r}]); "
                f"time.sleep({30 if interrupt else 0.7})"
            )
            with logfile.open("w") as stream:
                process = subprocess.Popen(
                    [sys.executable, str(RUNNER), sys.executable, "-c", parent],
                    stdout=stream, stderr=subprocess.STDOUT,
                )
            child_pid = None
            try:
                deadline = time.monotonic() + 4
                while not pidfile.exists() and time.monotonic() < deadline:
                    time.sleep(0.02)
                self.assertTrue(pidfile.exists(), logfile.read_text())
                child_pid = int(pidfile.read_text())
                if interrupt:
                    # Give the guard two samples to discover the descendant.
                    time.sleep(0.25)
                    process.send_signal(signal.SIGINT)
                process.wait(timeout=5)
                self.assertEqual(process.returncode, 130 if interrupt else 0, logfile.read_text())
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline:
                    try:
                        state = Path(f"/proc/{child_pid}/stat").read_text().rsplit(")", 1)[1].split()[0]
                    except FileNotFoundError:
                        return
                    if state == "Z":
                        return
                    time.sleep(0.02)
                self.fail("guard left a live descendant")
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait()
                if child_pid is not None:
                    try:
                        os.kill(child_pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_interrupt_cleans_uncooperative_child(self):
        self.check_descendant_cleanup(interrupt=True)

    def test_normal_driver_exit_cleans_uncooperative_child(self):
        self.check_descendant_cleanup(interrupt=False)


if __name__ == "__main__":
    unittest.main()
