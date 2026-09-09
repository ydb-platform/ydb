#!/usr/bin/env python3
"""Bounded, idempotent bootstrap and SQL health checks; no Docker socket needed."""
import json
from pathlib import Path
import subprocess
import sys
import time
import urllib.request


def run(command, timeout=40):
    result = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
    if result.returncode or "ERROR:" in result.stdout:
        raise RuntimeError(f"{command!r}: {result.stdout[-3000:]}")
    return result.stdout


def sql(suffix, port, query, timeout=40):
    return run(["/opt/ydb/bin/ydb", "-e", f"grpc://localhost:{port}", "-d", f"/Root/block{suffix}",
                "sql", "--format", "json-unicode-array", "-s", query], timeout)


def retry(action, seconds=900):
    deadline = time.monotonic() + seconds
    while True:
        try:
            return action()
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            if time.monotonic() >= deadline:
                raise
            print(str(error)[-1500:], flush=True)
            time.sleep(5)


def check_row(suffix, port, timeout=10):
    rows = json.loads(sql(suffix, port, "SELECT id, value FROM smoke ORDER BY id;", timeout))
    assert rows == [{"id": 1, "value": f"block{suffix}-persistent"}], rows
    print(json.dumps(rows), flush=True)


def bootstrap_storage():
    marker = Path("/state/initialized")
    if marker.exists():
        return
    def initialize():
        # Persist generation 1 through distconf. BSC's config init alone leaves
        # generation 0, so static reassign and persisted config are not enabled.
        request = urllib.request.Request(
            "http://localhost:8765/actors/nodewarden?page=distconf",
            json.dumps({"BootstrapCluster": {"SelfAssemblyUUID": "block82-stage4-local-fixture"}}).encode(),
            {"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=40) as response:
                result = json.load(response)
        except OSError as error:
            raise RuntimeError(str(error)) from error
        if result.get("Status", "OK") != "OK":
            raise RuntimeError(str(result))
        # Automatic box management registers all 13 PDisks after distconf commits.
        pdisks = json.loads(run(["/opt/ydb/bin/ydb-dstool", "--endpoint", "http://localhost:8765",
                                "pdisk", "list", "--format", "json"]))
        if len(pdisks) != 13:
            raise RuntimeError(f"Waiting for 13 PDisks: {len(pdisks)}")

    retry(initialize)
    marker.write_text("storage initialized\n")


def bootstrap(suffix, port):
    marker = Path("/state/initialized")
    if marker.exists():
        # Compute health checks and acceptance verify persistence after restart.
        # A restarted one-shot may still share the compute's old network namespace.
        return
    database = f"/Root/block{suffix}"
    admin = ["/opt/ydb/bin/ydbd", "-s", "grpc://storage-1:2135", "admin", "database", database]

    def create_database():
        try:
            status = run(admin + ["status"])
            if f"ssd-block{suffix}: 2/2" in status:
                return
        except RuntimeError:
            pass
        try:
            run(admin + ["create", f"ssd-block{suffix}:2"])
        except RuntimeError as error:
            # Creation is asynchronous. An existing database is accepted only after
            # its status confirms exactly the requested storage resources.
            print(error, flush=True)
        status = run(admin + ["status"])
        if f"ssd-block{suffix}: 2/2" not in status:
            raise RuntimeError(status)

    retry(create_database)
    retry(lambda: sql(suffix, port, "CREATE TABLE IF NOT EXISTS smoke (id Uint64 NOT NULL, value Utf8, PRIMARY KEY (id));"))
    retry(lambda: sql(suffix, port, f'UPSERT INTO smoke (id, value) VALUES (1u, "block{suffix}-persistent");'))
    retry(lambda: check_row(suffix, port))
    marker.write_text(f"/Root/block{suffix}\n")


if __name__ == "__main__":
    mode, *args = sys.argv[1:]
    if mode == "ping":
        with urllib.request.urlopen("http://localhost:8765/ping", timeout=3) as response:
            assert response.status == 200
    elif mode == "check-row":
        check_row(*args)
    elif mode == "bootstrap":
        bootstrap(*args)
    elif mode == "bootstrap-storage":
        bootstrap_storage()
    else:
        raise ValueError(mode)
