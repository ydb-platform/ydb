#!/usr/bin/env python3
"""Smoke tests for the real scheduler behind the simulator's CLI and C API.

Run with: python3 test_simulator.py --cli /path/to/kqp_scheduler_sim \
    --api /path/to/libkqp_scheduler_sim_api.so
"""

import argparse
from contextlib import contextmanager
import ctypes
from pathlib import Path
import subprocess
import unittest


class SimulatorCli:
    def __init__(self, path):
        self.path = path

    def run(self, commands, seed):
        result = subprocess.run(
            [str(self.path), str(seed)],
            input="\n".join((*commands, "quit", "")),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=30,
        )
        if result.returncode:
            raise AssertionError(f"CLI exited with {result.returncode}:\n{result.stderr}")
        return result.stdout.splitlines()


class SimulatorApi:
    def __init__(self, path):
        self.lib = ctypes.CDLL(str(path))
        self.lib.kqp_sim_new.argtypes = [ctypes.c_ulonglong]
        self.lib.kqp_sim_new.restype = ctypes.c_void_p
        self.lib.kqp_sim_exec.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
        self.lib.kqp_sim_exec.restype = ctypes.c_char_p
        self.lib.kqp_sim_free.argtypes = [ctypes.c_void_p]
        self.lib.kqp_sim_free.restype = None

    @contextmanager
    def instance(self, seed):
        handle = self.lib.kqp_sim_new(seed)
        if not handle:
            raise AssertionError("kqp_sim_new returned a null handle")

        def execute(command):
            reply = self.lib.kqp_sim_exec(handle, command.encode())
            if reply is None:
                raise AssertionError(f"kqp_sim_exec returned null for {command!r}")
            return reply.decode()

        try:
            yield execute
        finally:
            self.lib.kqp_sim_free(handle)

    def run(self, commands, seed):
        with self.instance(seed) as execute:
            return [execute(command) for command in commands]


class SimulatorTest(unittest.TestCase):
    def compare_transports(self, commands, seed=17):
        cli = self.cli.run(commands, seed)
        api = self.api.run(commands, seed)
        self.assertEqual(len(cli), len(commands), cli)
        self.assertEqual(len(api), len(commands), api)
        for step, (command, cli_reply, api_reply) in enumerate(zip(commands, cli, api), 1):
            self.assertEqual(cli_reply, api_reply, f"step {step}: {command}")
        return cli

    def check_transcript(self, script):
        commands, expected = zip(*script)
        for step, (command, reply, pattern) in enumerate(
            zip(commands, self.compare_transports(commands), expected), 1
        ):
            with self.subTest(step=step, command=command):
                self.assertRegex(reply, rf"^(?:{pattern})$")

    def test_memory_quota(self):
        self.check_transcript(
            [
                ("limits 4 1000", "ok"),
                ("pool batch 500", "ok"),
                ("query 1 batch 80", "ok"),
                ("update", "ok"),
                ("avail 1", "400"),
                ("mem 1 401 1", "no"),
                ("avail 1", "400"),
                ("mem 1 400 1", "ok"),
                ("avail 1", "0"),
                ("mem 1 1 1", "no"),
                ("mem 1 100 0", "ok"),
                ("avail 1", "-100"),
                ("mem 1 1 0", "no"),
                ("state", r"ok batch:\d+:500:0"),
                ("update", "ok"),
                ("state", r"ok batch:\d+:500:500"),
                ("free 1 100", "ok"),
                ("limits 4 350", "ok"),
                ("avail 1", "-120"),
                ("mem 1 1 0", "no"),
                ("free 1 100", "ok"),
                ("avail 1", "-20"),
                ("free 1 300", "ok"),
                ("update", "ok"),
                ("avail 1", "280"),
                ("state", r"ok batch:\d+:350:0"),
                ("query_end 1", "ok"),
                ("avail 1", "error .*Unknown query 1.*"),
            ]
        )

    def test_throttle_and_wake(self):
        self.check_transcript(
            [
                ("limits 1 1000", "ok"),
                ("pool batch 500", "ok"),
                ("query 1 batch 100", "ok"),
                ("task 10 1", "ok"),
                ("task 20 1", "ok"),
                ("update", "ok"),
                ("start 10", "ok"),
                ("at 100", "ok"),
                ("start 20", r"delay [1-9]\d*"),
                ("at 300", "ok"),
                ("stop 10", r"ok \| 20"),
                ("at 299", "error .*clock goes back.*"),
                ("resumed 20 1", "ok"),
                ("start 20", "ok"),
                ("at 450", "ok"),
                ("stop 20", "ok"),
                ("task_end 10", "ok"),
                ("task_end 20", "ok"),
                ("query_end 1", "ok"),
            ]
        )

    def test_seed_and_virtual_execution_time(self):
        def delays(duration, seed):
            commands = [
                "limits 1 1000", "pool batch 500", "query 1 batch 100",
                "task 10 1", "task 20 1", "update", "start 10",
                f"at {duration}", "stop 10", "start 10",
            ]
            replies = self.compare_transports(commands + ["start 20"] * 4, seed)
            self.assertEqual(replies[:len(commands)], ["ok"] * len(commands))
            result = replies[len(commands):]
            for reply in result:
                self.assertRegex(reply, r"^delay [1-9]\d*$")
            return [int(reply.split()[1]) for reply in result]

        first = delays(300, 17)
        self.assertEqual(delays(300, 17), first)
        self.assertNotEqual(delays(300, 23), first)
        # One CPU: a task that used 600 us more adds exactly 600 us to each delay.
        self.assertEqual(delays(900, 17), [delay + 600 for delay in first])

    def test_api_handles_are_isolated(self):
        with self.api.instance(17) as first, self.api.instance(17) as second:
            self.assertEqual(first("limits 1 1000"), "ok")
            self.assertEqual(first("pool batch 500"), "ok")
            self.assertEqual(first("query 1 batch 100"), "ok")
            self.assertEqual(first("at 100"), "ok")
            self.assertEqual(second("at 50"), "ok")
            self.assertRegex(second("avail 1"), "^error .*Unknown query 1.*$")
            self.assertEqual(first("avail 1"), "500")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cli", required=True, type=Path)
    parser.add_argument("--api", required=True, type=Path)
    args = parser.parse_args()
    SimulatorTest.cli = SimulatorCli(args.cli.resolve(strict=True))
    SimulatorTest.api = SimulatorApi(args.api.resolve(strict=True))
    unittest.main(argv=[__file__], verbosity=2)


if __name__ == "__main__":
    main()
