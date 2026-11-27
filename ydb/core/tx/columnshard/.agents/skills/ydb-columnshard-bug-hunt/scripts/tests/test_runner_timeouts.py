#!/usr/bin/env python3
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest

SKILLS = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(SKILLS / "ydb-columnshard-bug-hunt/scripts"))
sys.path.insert(0, str(SKILLS / "ydb-columnshard-bug-hunt-setup/scripts"))

import detect_env  # noqa: E402
import second_opinion  # noqa: E402

FAKE_HARNESS = """
from pathlib import Path
import subprocess
import sys
import time

mode, directory = sys.argv[1:]
root = Path(directory)
if mode == "child":
    (root / "ready").touch()
    deadline = time.monotonic() + 10
    while not (root / "release").exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    if (root / "release").exists():
        (root / "finished").touch()
else:
    child = subprocess.Popen([sys.executable, __file__, "child", directory])
    if mode == "wait":
        child.wait()
"""


def runners():
    return [
        ("review", lambda spec, cwd, timeout: second_opinion.ask(spec, cwd, "hello", timeout), "ok", "failed"),
        ("setup", lambda spec, cwd, timeout: detect_env.run_prompt(spec, "fake", cwd, "hello", timeout), 0, 124),
    ]


class RunnerTest(unittest.TestCase):
    def test_completed_reviewers_preserve_output(self):
        for name, run, success, _ in runners():
            with self.subTest(runner=name):
                spec = {"model": "fake", "stdin": True, "final": "stdout",
                        "argv": [sys.executable, "-c",
                                 "import sys; print(sys.stdin.read()); sys.stderr.write('diagnostic')"]}
                status, final, diagnostic = run(spec, os.getcwd(), 10)
                self.assertEqual((status, final), (success, "hello"))
                self.assertIn("diagnostic", diagnostic)

    @unittest.skipUnless(os.name == "posix", "process-group cleanup requires POSIX")
    def test_timeout_stops_children_even_after_leader_exits(self):
        for name, run, _, failure in runners():
            for mode in ("wait", "exit"):
                with self.subTest(runner=name, leader=mode), tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    harness = root / "harness.py"
                    harness.write_text(FAKE_HARNESS)
                    spec = {"model": "fake", "stdin": False, "final": "stdout",
                            "argv": [sys.executable, str(harness), mode, tmp]}
                    try:
                        status, _, diagnostic = run(spec, tmp, 1)
                        self.assertEqual(status, failure)
                        self.assertIn("timeout", diagnostic)
                        self.assertTrue((root / "ready").exists(), "the child must start before the timeout")
                        (root / "release").touch()
                        deadline = time.monotonic() + 1
                        while not (root / "finished").exists() and time.monotonic() < deadline:
                            time.sleep(0.01)
                        self.assertFalse((root / "finished").exists(), "child kept running after the timeout")
                    finally:
                        # Let any surviving child exit even when the regression assertion fails.
                        (root / "release").touch()


if __name__ == "__main__":
    unittest.main()
