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
import socket
import subprocess
import tempfile
import time

import library.python.port_manager
import pytest
import yatest


READY_TIMEOUT_SECONDS = 120
POLL_INTERVAL_SECONDS = 2
TCP_CONNECT_TIMEOUT_SECONDS = 5

# Ports that must accept TCP connections after the cluster is ready.
# GRPC_TLS_PORT is intentionally NOT here: YDB_GRPC_ENABLE_TLS=false below, so
# the TLS endpoint is not bound. If you turn TLS on, add "GRPC_TLS_PORT" here.
TCP_CHECK_PORTS = ["GRPC_PORT", "MON_PORT", "IC_PORT", "YDB_KAFKA_PROXY_PORT"]

# Health table is created and dropped on every retry, idempotent, just like
# .github/docker/files/health_check does inside the container.
HEALTH_TABLE = "`/local/.sys_health/test`"
HEALTH_QUERIES = [
    f"create table if not exists {HEALTH_TABLE} (key int32, value utf8, primary key(key))",
    f"drop table {HEALTH_TABLE}",
]


def _run_query(ydb_cli: str, endpoint: str, sql: str) -> subprocess.CompletedProcess:
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
    """Returns the first failing CompletedProcess, or the last (successful) one."""
    last = None
    for sql in HEALTH_QUERIES:
        last = _run_query(ydb_cli, endpoint, sql)
        if last.returncode != 0:
            return last
    return last


def test_local_ydb_deploy_with_fixed_ports():
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
        r = subprocess.run(
            [
                local_ydb, "deploy",
                "--ydb-working-dir", workdir,
                "--ydb-binary-path", ydbd,
                "--fixed-ports",
            ],
            capture_output=True, text=True, timeout=300, env=env,
        )
        if r.returncode != 0:
            pytest.fail(
                f"local_ydb deploy exited with {r.returncode}\n"
                f"--- stdout ---\n{r.stdout}\n"
                f"--- stderr ---\n{r.stderr}"
            )
        deployed = True

        # Same shape as .github/docker/files/health_check: retry create+drop
        # until ydb answers or the timeout is hit.
        deadline = time.monotonic() + READY_TIMEOUT_SECONDS
        last = None
        while time.monotonic() < deadline:
            last = _try_health(ydb_cli, endpoint)
            if last.returncode == 0:
                break
            time.sleep(POLL_INTERVAL_SECONDS)
        else:
            pytest.fail(
                f"ydb did not become ready in {READY_TIMEOUT_SECONDS}s. "
                f"last attempt:\n--- stdout ---\n{last.stdout}\n"
                f"--- stderr ---\n{last.stderr}"
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
            subprocess.run(
                [
                    local_ydb, "stop",
                    "--ydb-working-dir", workdir,
                    "--ydb-binary-path", ydbd,
                ],
                capture_output=True, text=True, timeout=60, env=env,
            )
        shutil.rmtree(workdir, ignore_errors=True)
