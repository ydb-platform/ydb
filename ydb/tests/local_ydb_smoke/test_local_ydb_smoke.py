"""
Smoke test for the `local_ydb` deploy path that the local-ydb docker image
runs at startup.

IMPORTANT — paired with .github/docker/Dockerfile and
.github/docker/files/initialize_local_ydb. The env vars, command-line flags
and the set of ports below must match how the docker image starts local_ydb.
If you change one side, change the other.
"""
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time

import library.python.port_manager
import pytest
import yatest


# SIZE(MEDIUM) in ya.make gives a 600s hard limit. The internal budget is 10s
# shorter so the test has time to fail cleanly, dump logs and run
# finally-cleanup before ya make kills it.
OVERALL_BUDGET_SECONDS = 590
# `local_ydb stop` should always get some time even if the budget is gone,
# otherwise ydbd processes leak.
STOP_GRACE_SECONDS = 30

POLL_INTERVAL_SECONDS = 2
TCP_CONNECT_TIMEOUT_SECONDS = 10

# Ports that must accept TCP connections after the cluster is ready.
# GRPC_TLS_PORT is intentionally NOT here: YDB_GRPC_ENABLE_TLS=false below, so
# the TLS endpoint is not bound. If you turn TLS on, add "GRPC_TLS_PORT" here.
TCP_CHECK_PORTS = ["GRPC_PORT", "MON_PORT", "IC_PORT", "YDB_KAFKA_PROXY_PORT"]

# Health table is created and dropped on every retry, idempotent, just like
# .github/docker/files/health_check does inside the container.
HEALTH_TABLE = "`/local/test_table`"
HEALTH_QUERIES = [
    f"create table if not exists {HEALTH_TABLE} (key int32, value utf8, primary key(key))",
    f"drop table {HEALTH_TABLE}",
]


def _remaining(deadline: float) -> float:
    return max(0.0, deadline - time.monotonic())


def _run_query(ydb_cli: str, endpoint: str, sql: str) -> subprocess.CompletedProcess:
    # Shell out to the ydb CLI (not the Python SDK) to match the health probe
    # path that the docker image uses in .github/docker/files/health_check.
    return subprocess.run(
        [
            ydb_cli,
            "--endpoint", endpoint,
            "--database", "/local",
            "--no-discovery",
            "sql", "-s", sql,
        ],
        capture_output=True, text=True,
    )


def _try_health(ydb_cli: str, endpoint: str) -> subprocess.CompletedProcess:
    last = None
    for sql in HEALTH_QUERIES:
        last = _run_query(ydb_cli, endpoint, sql)
        if last.returncode != 0:
            return last
    return last


def _kill_residual_ydbd(workdir: str) -> None:
    # `local_ydb stop` is best-effort; if it failed or timed out, find any
    # ydbd processes whose command line still mentions our working dir and
    # SIGKILL them. PortManager-supplied ports keep parallel tests isolated,
    # but a leaked ydbd would still hog the CI runner.
    r = subprocess.run(
        ["pgrep", "-f", workdir],
        capture_output=True, text=True,
    )
    for line in r.stdout.split():
        try:
            os.kill(int(line), signal.SIGKILL)
        except (ProcessLookupError, ValueError, PermissionError):
            pass


def test_local_ydb_deploy_with_fixed_ports():
    deadline = time.monotonic() + OVERALL_BUDGET_SECONDS

    workdir = tempfile.mkdtemp(prefix="local-ydb-", dir=yatest.common.output_path())
    ydbd = yatest.common.binary_path("ydb/apps/ydbd/ydbd")
    ydb_cli = yatest.common.binary_path("ydb/apps/ydb/ydb")
    local_ydb = yatest.common.binary_path("ydb/public/tools/local_ydb/local_ydb")

    # PortManager coordinates with parallel ya make tests via PORT_SYNC_PATH and
    # avoids the OS ephemeral port range. Overrides defaults that
    # KikimrFixedNodePortAllocator reads from env (kikimr_port_allocator.py).
    pm = library.python.port_manager.PortManager()
    ports = {
        "GRPC_PORT": str(pm.get_port()),
        "GRPC_TLS_PORT": str(pm.get_port()),
        "MON_PORT": str(pm.get_port()),
        "IC_PORT": str(pm.get_port()),
        "YDB_KAFKA_PROXY_PORT": str(pm.get_port()),
    }
    env = {
        **os.environ,
        **ports,
        "YDB_TINY_MODE": "true",
        "YDB_GRPC_ENABLE_TLS": "false",
    }
    endpoint = f"grpc://localhost:{ports['GRPC_PORT']}"

    deployed = False
    try:
        deploy_budget = _remaining(deadline)
        if deploy_budget <= 0:
            pytest.fail("budget exhausted before local_ydb deploy")
        try:
            r = subprocess.run(
                [
                    local_ydb, "deploy",
                    "--ydb-working-dir", workdir,
                    "--ydb-binary-path", ydbd,
                    "--fixed-ports",
                ],
                capture_output=True, text=True, timeout=deploy_budget, env=env,
            )
        except subprocess.TimeoutExpired as e:
            pytest.fail(
                f"local_ydb deploy did not return within {deploy_budget:.0f}s\n"
                f"--- stdout ---\n{e.stdout or ''}\n"
                f"--- stderr ---\n{e.stderr or ''}"
            )
        if r.returncode != 0:
            pytest.fail(
                f"local_ydb deploy exited with {r.returncode}\n"
                f"--- stdout ---\n{r.stdout}\n"
                f"--- stderr ---\n{r.stderr}"
            )
        deployed = True

        # Same shape as .github/docker/files/health_check: retry create+drop
        # until ydb answers or the overall budget is hit.
        last = None
        while _remaining(deadline) > 0:
            last = _try_health(ydb_cli, endpoint)
            if last.returncode == 0:
                break
            time.sleep(POLL_INTERVAL_SECONDS)
        else:
            pytest.fail(
                f"ydb did not become ready within the {OVERALL_BUDGET_SECONDS}s budget. "
                f"last attempt:\n--- stdout ---\n{last.stdout if last else ''}\n"
                f"--- stderr ---\n{last.stderr if last else ''}"
            )

        # After SQL is reachable, every configured port should accept TCP.
        for name in TCP_CHECK_PORTS:
            port = int(ports[name])
            try:
                with socket.create_connection(
                    ("localhost", port), timeout=TCP_CONNECT_TIMEOUT_SECONDS
                ):
                    pass
            except OSError as e:
                pytest.fail(f"TCP connect to {name}={port} failed: {e}")
    finally:
        if deployed:
            stop_budget = max(_remaining(deadline), STOP_GRACE_SECONDS)
            try:
                subprocess.run(
                    [
                        local_ydb, "stop",
                        "--ydb-working-dir", workdir,
                        "--ydb-binary-path", ydbd,
                    ],
                    capture_output=True, text=True, timeout=stop_budget, env=env,
                )
            except subprocess.TimeoutExpired:
                pass
            _kill_residual_ydbd(workdir)
        shutil.rmtree(workdir, ignore_errors=True)
