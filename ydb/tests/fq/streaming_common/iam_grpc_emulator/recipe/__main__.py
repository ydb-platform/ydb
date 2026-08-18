import logging

import library.python.port_manager
from library.recipes import common as recipes_common
from library.python.testing.recipe import declare_recipe, set_env
import yatest
import yatest.common as ya_common

HOSTNAME = "localhost"
DAEMON_NAME = "iam_grpc_emulator"
PID_FILENAME = f"{DAEMON_NAME}_recipe.pid"
LOG_FILENAME = f"{DAEMON_NAME}.err.log"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def is_daemon_ready() -> bool:
    with open(yatest.common.output_path(LOG_FILENAME), "r") as outFile:
        return "IAM gRPC emulator listening on port" in outFile.read()


def start(argv):
    logger.debug("Starting IAM gRPC emulator recipe")

    pm = library.python.port_manager.PortManager()
    grpc_port = pm.get_port()

    binary_path = ya_common.binary_path(f"ydb/tests/fq/streaming_common/{DAEMON_NAME}/bin/{DAEMON_NAME}")
    assert binary_path, f"{DAEMON_NAME} binary not found"

    cmd = [
        binary_path,
        "--port", str(grpc_port),
    ]

    recipes_common.start_daemon(
        command=cmd,
        environment=None,
        is_alive_check=is_daemon_ready,
        pid_file_name=PID_FILENAME,
        daemon_name=DAEMON_NAME
    )

    set_env("IAM_EMULATOR_ENDPOINT", f"{HOSTNAME}:{grpc_port}")

    logger.debug(f"IAM gRPC emulator recipe started on {HOSTNAME}:{grpc_port}")


def stop(argv):
    logger.debug("Stopping IAM gRPC emulator recipe")
    with open(PID_FILENAME, "r") as pidFile:
        pid = int(pidFile.read())
        recipes_common.stop_daemon(pid)


if __name__ == "__main__":
    declare_recipe(start, stop)
