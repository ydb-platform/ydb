import os
import shutil
import subprocess
import tempfile
import time

import library.python.port_manager
import pytest
import yatest


READY_TIMEOUT_SECONDS = 120
POLL_INTERVAL_SECONDS = 2


def test_local_ydb_deploy_with_fixed_ports():
    workdir = tempfile.mkdtemp(prefix="local-ydb-", dir=yatest.common.output_path())
    ydbd = yatest.common.binary_path("ydb/apps/ydbd/ydbd")
    ydb_cli = yatest.common.binary_path("ydb/apps/ydb/ydb")
    local_ydb = yatest.common.binary_path("ydb/public/tools/local_ydb/local_ydb")

    # PortManager coordinates with parallel ya make tests via PORT_SYNC_PATH and
    # avoids the OS ephemeral port range. Override the defaults that
    # KikimrFixedNodePortAllocator reads from env (kikimr_port_allocator.py:217-225).
    pm = library.python.port_manager.PortManager()
    ports = {
        "GRPC_PORT": str(pm.get_port()),
        "GRPC_TLS_PORT": str(pm.get_port()),
        "MON_PORT": str(pm.get_port()),
        "IC_PORT": str(pm.get_port()),
        # Non-zero YDB_KAFKA_PROXY_PORT triggers the kafka_proxy code path:
        # lib/cmds/__init__.py:363-365 enables kafka_api_port in
        # KikimrConfigGenerator, which then makes kikimr_runner.py:83 read
        # port_allocator.kafka_api_port. This is exactly the path that the
        # 25-day nightly regression broke.
        "YDB_KAFKA_PROXY_PORT": str(pm.get_port()),
    }
    env = {
        **os.environ,
        **ports,
        "YDB_TINY_MODE": "true",
        "YDB_GRPC_ENABLE_TLS": "false",
    }

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

        # Equivalent of /health_check from the docker image.
        deadline = time.monotonic() + READY_TIMEOUT_SECONDS
        last = None
        while time.monotonic() < deadline:
            last = subprocess.run(
                [
                    ydb_cli,
                    "--endpoint", f"grpc://localhost:{ports['GRPC_PORT']}",
                    "--database", "/local",
                    "--no-discovery",
                    "sql", "-s", "select 1",
                ],
                capture_output=True, text=True,
            )
            if last.returncode == 0:
                return
            time.sleep(POLL_INTERVAL_SECONDS)

        pytest.fail(
            f"ydb did not become ready in {READY_TIMEOUT_SECONDS}s. "
            f"last attempt:\n--- stdout ---\n{last.stdout}\n"
            f"--- stderr ---\n{last.stderr}"
        )
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
