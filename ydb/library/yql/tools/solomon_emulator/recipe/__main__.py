import argparse
import logging

from library.python.testing.recipe import declare_recipe, set_env
from library.recipes import common as recipes_common
import library.python.port_manager
import yatest.common as ya_common

DAEMON_NAME = "solomon_emulator"
PID_FILENAME = f"{DAEMON_NAME}_recipe.pid"
LOG_FILENAME = f"{DAEMON_NAME}.err.log"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def is_daemon_ready() -> bool:
    with open(ya_common.output_path(LOG_FILENAME), "r") as logFile:
        return "Started Solomon emulator on http port" in logFile.read()


def parse_args(argv):
    formatter = argparse.ArgumentDefaultsHelpFormatter
    parser = argparse.ArgumentParser(
        formatter_class=formatter,
    )

    parser.add_argument("--auth", type=str, required=False, help="Allowed value for Authorization header")
    parser.add_argument("--shard", type=str, required=False,
                        help="Allowed shard id in form $project_name/$service_name/$cluster_name")
    return parser.parse_args(argv)


def start(argv):
    logger.debug("Starting Solomon recipe")
    args = parse_args(argv)
    pm = library.python.port_manager.PortManager()
    http_port = pm.get_port()
    grpc_port = pm.get_port()
    binary_path = ya_common.binary_path(f"ydb/library/yql/tools/{DAEMON_NAME}/bin/{DAEMON_NAME}")
    assert binary_path
    cmd = [
        binary_path,
        "--http-port",
        str(http_port),
        "--grpc-port",
        str(grpc_port)
    ]

    if args.auth:
        cmd.extend(["--auth", args.auth])

    if args.shard:
        cmd.extend(["--shard", args.shard])

    recipes_common.start_daemon(
        command=cmd,
        environment=None,
        is_alive_check=is_daemon_ready,
        pid_file_name=PID_FILENAME,
        daemon_name=DAEMON_NAME
    )

    http_endpoint = f"localhost:{http_port}"
    grpc_endpoint = f"localhost:{grpc_port}"
    set_env("SOLOMON_HOST", "localhost")
    set_env("SOLOMON_HTTP_URL", "http://" + http_endpoint)
    set_env("SOLOMON_HTTP_ENDPOINT", http_endpoint)
    set_env("SOLOMON_GRPC_ENDPOINT", grpc_endpoint)
    set_env("SOLOMON_HTTP_PORT", str(http_port))
    set_env("SOLOMON_GRPC_PORT", str(grpc_port))

    logger.debug(f"Solomon recipe has been started, http_endpoint: {http_endpoint}, grpc_endpoint: {grpc_endpoint}")


def stop(argv):
    logger.debug("Stop Solomon recipe")
    with open(PID_FILENAME, "r") as pidFile:
        pid = int(pidFile.read())
        recipes_common.stop_daemon(pid)


if __name__ == "__main__":
    declare_recipe(start, stop)
