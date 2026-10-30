# -*- coding: utf-8 -*-
"""
E2E test: TPC-H Q15 under node restarts.

1. Start an 8-node cluster (block-4-2, file-backed pdisks), load TPC-H scale 1.
2. Run Q15 with several parallel calls, all must succeed.
3. Restart / kill random nodes while Q15 keeps running: some calls may fail,
   but some must succeed.
4. Stop restarts, let the cluster recover, run Q15 again: all must succeed.
"""
import logging
import os
import random
import re
import threading
import time
from collections import namedtuple

import pytest
import yatest.common

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness import daemon
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


def _param(name, default):
    value = yatest.common.get_param(name)
    return type(default)(value) if value is not None else default


TPCH_PATH = "tpch/s1"
TPCH_SCALE = 1
QUERY_NUM = 15
DATABASE = "/Root"

THREADS = _param("threads", 4)
ITERATIONS_PER_ROUND = _param("iterations_per_round", 8)
REQUEST_TIMEOUT_S = _param("request_timeout_s", 60)
GLOBAL_TIMEOUT_S = _param("global_timeout_s", 150)
ROUND_EXEC_TIMEOUT_S = GLOBAL_TIMEOUT_S + 60

CHAOS_CYCLES = _param("chaos_cycles", 3)
NODE_DOWN_S = _param("node_down_s", 10)
STABILIZE_S = _param("stabilize_s", 20)
CHAOS_DEADLINE_S = CHAOS_CYCLES * 240 + 120
CHAOS_SEED = _param("chaos_seed", 1234)

RECOVERY_TIMEOUT_S = _param("recovery_timeout_s", 300)
PROBE_ATTEMPTS = _param("probe_attempts", 10)
BASELINE_ROUNDS = _param("baseline_rounds", 1)
RECOVERY_ROUNDS = _param("recovery_rounds", 2)

SETUP_CLI_TIMEOUT_S = 1200

RoundResult = namedtuple("RoundResult", "ok failed exit_code stdout_path stderr_path")

ITERATION_RE = re.compile(r"^\titeration (\d+):\t(ok|failed)\t", re.M)


class _ChaosThread(threading.Thread):
    """Thread that stores an exception raised in run() and re-raises it in join()."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.daemon = True
        self.exc = None

    def run(self):
        try:
            super().run()
        except BaseException as e:  # noqa: B902
            logger.exception("chaos thread failed")
            self.exc = e

    def join(self, timeout=None):
        super().join(timeout)
        if self.exc is not None:
            raise self.exc


class TestTpchRestarts:
    cluster = None

    @classmethod
    def setup_class(cls):
        configurator = KikimrConfigGenerator(
            erasure=Erasure.BLOCK_4_2,
            use_in_memory_pdisks=False,
        )
        cls.cluster = KiKiMR(configurator=configurator)
        cls.cluster.start()
        assert len(cls.cluster.nodes) == 8, list(cls.cluster.nodes)

        cls.entry_node = cls.cluster.nodes[1]
        cls.endpoint = "grpc://%s:%d" % (cls.entry_node.host, cls.entry_node.grpc_port)
        cls.victim_node_ids = [node_id for node_id in cls.cluster.nodes if node_id != cls.entry_node.node_id]

        cls._wait_cluster_usable(120)
        cls._cli(
            ["workload", "tpch", "-p", TPCH_PATH, "init", "--store=column", "--datetime-types=dt64"],
            tag="init", timeout=SETUP_CLI_TIMEOUT_S,
        )
        cls._cli(
            ["workload", "tpch", "-p", TPCH_PATH, "import", "generator", "--scale=%s" % TPCH_SCALE],
            tag="import", timeout=SETUP_CLI_TIMEOUT_S,
        )
        cls._wait_cluster_usable(120)

    @classmethod
    def teardown_class(cls):
        if cls.cluster is None:
            return
        cls._ensure_all_nodes_started()
        cls.cluster.stop()

    # ---------------------------------------------------------------- helpers

    @staticmethod
    def _out_dir():
        # resolved at call time so every parametrized test keeps its own artifacts
        return yatest.common.test_output_path()

    @classmethod
    def _cli(cls, argv, tag, check_exit_code=True, timeout=None):
        cmd = [
            yatest.common.binary_path(os.getenv("YDB_CLI_BINARY")),
            "-e", cls.endpoint,
            "-d", DATABASE,
        ] + argv
        out_dir = cls._out_dir()
        stdout_path = os.path.join(out_dir, "%s.stdout" % tag)
        stderr_path = os.path.join(out_dir, "%s.stderr" % tag)
        logger.info("run cli [%s]: %s", tag, " ".join(cmd))
        with open(stdout_path, "wt") as out, open(stderr_path, "wt") as err:
            execution = yatest.common.execute(
                cmd,
                check_exit_code=check_exit_code,
                timeout=timeout,
                stdout=out,
                stderr=err,
                cwd=out_dir,
            )
        return execution, stdout_path, stderr_path

    @staticmethod
    def _parse_iterations(text):
        ok = failed = 0
        for _, status in ITERATION_RE.findall(text):
            if status == "ok":
                ok += 1
            else:
                failed += 1
        return ok, failed

    @classmethod
    def _run_round(cls, tag, iterations=ITERATIONS_PER_ROUND, threads=THREADS):
        argv = [
            "workload", "tpch", "-p", TPCH_PATH, "run",
            "--include", str(QUERY_NUM),
            "--iterations", str(iterations),
            "--threads", str(threads),
            "--scale", str(TPCH_SCALE),
            "--retries", "0",
            "--request-timeout", "%ds" % REQUEST_TIMEOUT_S,
            "--global-timeout", "%ds" % GLOBAL_TIMEOUT_S,
            "--output", os.path.join(cls._out_dir(), "%s.results" % tag),
        ]
        exit_code = None
        stdout_path = stderr_path = None
        started = time.time()
        try:
            execution, stdout_path, stderr_path = cls._cli(
                argv, tag=tag, check_exit_code=False, timeout=ROUND_EXEC_TIMEOUT_S,
            )
            exit_code = execution.exit_code
        except Exception as e:  # execution timeout or launch failure
            logger.error("round %s: cli execution failed: %s", tag, e)
            stdout_path = os.path.join(cls._out_dir(), "%s.stdout" % tag)
            stderr_path = os.path.join(cls._out_dir(), "%s.stderr" % tag)

        text = ""
        if stdout_path and os.path.exists(stdout_path):
            with open(stdout_path) as f:
                text = f.read()
        ok, failed = cls._parse_iterations(text)
        if ok + failed < iterations:
            # CLI could not connect, aborted, or global timeout cut the round
            failed = iterations - ok
        result = RoundResult(ok, failed, exit_code, stdout_path, stderr_path)
        logger.info(
            "round %s: ok=%d failed=%d exit_code=%s elapsed=%.1fs",
            tag, ok, failed, exit_code, time.time() - started,
        )
        return result

    @classmethod
    def _wait_cluster_usable(cls, timeout):
        endpoint = "%s:%d" % (cls.entry_node.host, cls.entry_node.grpc_port)
        deadline = time.time() + timeout
        last_error = None
        while time.time() < deadline:
            driver = ydb.Driver(ydb.DriverConfig(endpoint, DATABASE))
            try:
                driver.wait(timeout=5, fail_fast=True)
                with ydb.QuerySessionPool(driver) as pool:
                    pool.execute_with_retries(
                        "SELECT 1",
                        retry_settings=ydb.RetrySettings(max_retries=1),
                        settings=ydb.BaseRequestSettings().with_timeout(10).with_operation_timeout(10).with_cancel_after(10),
                    )
                logger.info("cluster is usable")
                return
            except Exception as e:
                last_error = e
                logger.info("cluster is not usable yet: %r", e)
            finally:
                driver.stop()
            time.sleep(2)
        raise AssertionError("cluster did not become usable within %ss; last error: %r" % (timeout, last_error))

    @classmethod
    def _wait_q15_ok(cls, mode):
        deadline = time.time() + RECOVERY_TIMEOUT_S
        attempts = []
        for i in range(PROBE_ATTEMPTS):
            if time.time() >= deadline:
                break
            result = cls._run_round("%s_probe_%d" % (mode, i), iterations=2, threads=2)
            attempts.append(result)
            if result.failed == 0:
                return
            time.sleep(5)
        raise AssertionError("Q15 did not recover after chaos (%s): %s" % (mode, attempts))

    @classmethod
    def _ensure_all_nodes_started(cls):
        for node_id, node in cls.cluster.nodes.items():
            if not node.is_alive():
                logger.warning("node %d is down, starting it", node_id)
                try:
                    node.start()
                except Exception:
                    logger.exception("failed to start node %d", node_id)

    @classmethod
    def _dead_nodes(cls):
        return [node_id for node_id, node in cls.cluster.nodes.items() if not node.is_alive()]

    @classmethod
    def _restart_node(cls, node, mode, abort_event):
        logger.info("chaos: taking node %d down (%s)", node.node_id, mode)
        if mode == "graceful":
            node.stop()
        elif mode == "kill":
            # KiKiMRNode.kill() restarts the node right away; use the base
            # Daemon.kill() to SIGKILL it and leave it down for a while.
            daemon.Daemon.kill(node)
        else:
            raise ValueError(mode)
        abort_event.wait(NODE_DOWN_S)
        logger.info("chaos: starting node %d", node.node_id)
        node.start()
        assert node.is_alive(), "node %d did not start" % node.node_id
        abort_event.wait(STABILIZE_S)

    @classmethod
    def _chaos_loop(cls, mode, stop_event, abort_event, victims):
        try:
            rnd = random.Random(CHAOS_SEED)
            logger.info("chaos: seed=%d cycles=%d candidates=%s", CHAOS_SEED, CHAOS_CYCLES, cls.victim_node_ids)
            for cycle in range(CHAOS_CYCLES):
                if abort_event.is_set():
                    logger.warning("chaos: aborted before cycle %d", cycle)
                    break
                node = cls.cluster.nodes[rnd.choice(cls.victim_node_ids)]
                victims.append((cycle, node.node_id))
                cls._restart_node(node, mode, abort_event)
        finally:
            stop_event.set()

    # ------------------------------------------------------------------- test

    @pytest.mark.parametrize("mode", ["graceful", "kill"])
    def test_q15_under_restarts(self, mode):
        self._ensure_all_nodes_started()
        assert not self._dead_nodes()
        self._wait_cluster_usable(120)

        # Phase 1: healthy cluster, everything must succeed
        for i in range(BASELINE_ROUNDS):
            result = self._run_round("%s_baseline_%d" % (mode, i))
            assert result.failed == 0 and result.ok == ITERATIONS_PER_ROUND, \
                "baseline round failed: %s" % (result,)

        # Phase 2: restart nodes while Q15 keeps running
        stop_event = threading.Event()
        abort_event = threading.Event()
        victims = []
        chaos = _ChaosThread(target=self._chaos_loop, args=(mode, stop_event, abort_event, victims))
        chaos.start()
        ok = failed = rounds = 0
        deadline = time.time() + CHAOS_DEADLINE_S
        try:
            while not stop_event.is_set() and time.time() < deadline:
                result = self._run_round("%s_chaos_%d" % (mode, rounds))
                rounds += 1
                ok += result.ok
                failed += result.failed
        finally:
            abort_event.set()
            chaos.join(timeout=300)
        assert not chaos.is_alive(), "chaos thread is stuck"
        logger.info(
            "chaos summary mode=%s rounds=%d ok=%d failed=%d victims=%s",
            mode, rounds, ok, failed, victims,
        )
        assert len(victims) == CHAOS_CYCLES, "chaos did not complete: victims=%s" % victims
        assert ok > 0, "no Q15 call succeeded during chaos (rounds=%d failed=%d)" % (rounds, failed)

        # Phase 3: restarts stopped, cluster must be fully healthy again
        self._ensure_all_nodes_started()
        assert not self._dead_nodes(), "dead nodes after chaos: %s" % self._dead_nodes()
        self._wait_cluster_usable(RECOVERY_TIMEOUT_S)
        self._wait_q15_ok(mode)
        for i in range(RECOVERY_ROUNDS):
            result = self._run_round("%s_recovery_%d" % (mode, i))
            assert result.failed == 0 and result.ok == ITERATIONS_PER_ROUND, \
                "recovery round failed: %s" % (result,)
        assert not self._dead_nodes(), "dead nodes after recovery: %s" % self._dead_nodes()
