import argparse
import logging
import os

from library.python.testing.recipe import declare_recipe, set_env
from library.recipes import common as recipes_common
from yatest.common.network import PortManager
import yatest


PID_FILENAME = "kqprun_daemon.pid"
KQPRUN_PATH = os.getenv("KQPRUN_EXECUTABLE") or "ydb/tests/tools/kqprun/kqprun"
INITIALIZATION_IMEOUT_RATIO = 2


def is_kqprun_daemon_ready() -> bool:
    with open(yatest.common.output_path("kqprun_daemon.out.log"), "r") as outFile:
        return "Initialization finished" in outFile.read()


def build_start_comand(argv: list[str], grpc_port: int) -> tuple[int, list[str]]:
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", action="append", type=str, default=[])
    parser.add_argument("--config", action='store', type=str, default="ydb/tests/tools/kqprun/kqprun/configuration/app_config.conf")
    parser.add_argument("--timeout-ms", action='store', type=int, default=30000)
    parsed, _ = parser.parse_known_args(argv)

    cmd = [
        yatest.common.binary_path(KQPRUN_PATH),
        "--log-file", yatest.common.output_path("kqprun_daemon.ydb.log"),
        "--app-config", yatest.common.source_path(parsed.config),
        "--grpc", str(grpc_port),
        "--timeout", str(parsed.timeout_ms)
    ]

    if parsed.query:
        cmd.append("--execution-case")
        cmd.append("query")

    for query in parsed.query:
        cmd.append("--script-query")
        cmd.append(yatest.common.source_path(query))

    return (parsed.timeout_ms, cmd)


def start(argv: list[str]):
    logging.debug("Starting kqprun daemon")

    portManager = PortManager()
    grpc_port = portManager.get_port()
    timeout_ms, cmd = build_start_comand(argv, grpc_port)

    recipes_common.start_daemon(
        command=cmd,
        environment=None,
        is_alive_check=is_kqprun_daemon_ready,
        pid_file_name=PID_FILENAME,
        timeout=INITIALIZATION_IMEOUT_RATIO * (timeout_ms // 1000),
        daemon_name="kqprun_daemon"
    )

    set_env("KQPRUN_ENDPOINT", f"grpc://localhost:{grpc_port}")
    logging.debug(f"kqprun daemon has been started on port: {grpc_port}")


def stop(argv: list[str]):
    logging.debug("Stop kqprun daemon")
    with open(PID_FILENAME, "r") as pidFile:
        pid = int(pidFile.read())
        recipes_common.stop_daemon(pid)


if __name__ == "__main__":
    declare_recipe(start, stop)
