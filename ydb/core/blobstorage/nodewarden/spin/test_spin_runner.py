#!/usr/bin/env python3
"""Check result classification and timeout cleanup without invoking SPIN or a compiler."""

import os
from pathlib import Path
import signal
import sys
import tempfile
import time
import unittest

from spin_runner import classify_search, execute


SEARCH = "State-vector 40 byte, depth reached 7, errors: 0\n1.2e+03 states, stored\n"


class SpinRunnerTest(unittest.TestCase):
    def test_search_classification(self):
        cases = [
            (SEARCH, False, 0, "PASS"),
            (SEARCH, True, 0, "INCOMPLETE"),
            (SEARCH + "Search not completed", False, 0, "INCOMPLETE"),
            (SEARCH + "max search depth too small", False, 0, "INCOMPLETE"),
            (SEARCH + "out of memory", False, 1, "INCOMPLETE"),
            (SEARCH + "VECTORSZ too small", False, 0, "INCOMPLETE"),
            (SEARCH + "NFAIR too small", False, 0, "INCOMPLETE"),
            (SEARCH + "MODEL_BOUND", False, 0, "INCOMPLETE"),
            (SEARCH.replace("errors: 0", "errors: 1") +
             "assertion violated (len(queue) < QUEUE_CAPACITY)", False, 0, "INCOMPLETE"),
            (SEARCH.replace("errors: 0", "errors: 1") +
             "assertion violated safety\nSearch not completed", False, 0, "FAIL"),
            (SEARCH.replace("errors: 0", "errors: 1") + "acceptance cycle", False, 0, "FAIL"),
            (SEARCH, False, 1, "SETUP_FAILED"),
            ("errors: 0", False, 0, "SETUP_FAILED"),
        ]
        for output, timed_out, code, expected in cases:
            with self.subTest(output=output, timed_out=timed_out, code=code):
                status, _, stats = classify_search({"timed_out": timed_out, "returncode": code}, output)
                self.assertEqual(status, expected)
                self.assertEqual(stats["states_stored"], 1200 if "states, stored" in output else None)

    def test_command_logs_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            run, output = execute([sys.executable, "-c", "print('failed'); raise SystemExit(7)"],
                                  directory, "command.txt", 5)
            self.assertFalse(run["timed_out"])
            self.assertEqual(run["returncode"], 7)
            self.assertEqual(output, "failed\n")
            self.assertEqual((directory / "command.txt").read_text(), output)

    def check_timeout(self, verifier):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            script = directory / ("pan" if verifier else "worker.py")
            script.write_text(f"#!{sys.executable}\n" + '''
import os, signal, sys, time

def interrupted(signum, frame):
    print("Search not completed", flush=True)
    raise SystemExit(0)

signal.signal(signal.SIGINT, interrupted)
if os.fork() == 0:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    print(f"CHILD={os.getpid()}", flush=True)
while True:
    time.sleep(60)
''')
            script.chmod(0o700)
            command = ["./pan"] if verifier else [sys.executable, script.name]
            run, output = execute(command, directory, "timeout.txt", 3 if verifier else 1)
            child = int(output.split("CHILD=", 1)[1].split()[0])
            try:
                self.assertTrue(run["timed_out"])
                if verifier:
                    self.assertIn("Search not completed", output)
                for _ in range(100):
                    try:
                        state = Path(f"/proc/{child}/stat").read_text().rsplit(")", 1)[1].split()[0]
                    except FileNotFoundError:
                        break
                    if state == "Z":
                        break
                    time.sleep(0.01)
                else:
                    self.fail("The timed-out command left a running child")
            finally:
                try:
                    os.kill(child, signal.SIGKILL)
                except ProcessLookupError:
                    pass

    def test_compiler_timeout_kills_children(self):
        self.check_timeout(False)

    def test_verifier_timeout_keeps_partial_log_and_kills_children(self):
        self.check_timeout(True)


if __name__ == "__main__":
    unittest.main()
