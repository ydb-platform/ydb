import logging
import os
import signal

import library.python.port_manager
import yatest.common as ya_common
from library.python.testing.recipe import declare_recipe, set_env

PID_FILENAME = "iam_grpc_emulator_recipe.pid"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def start(argv):
    logger.debug("Starting IAM gRPC emulator recipe")

    pm = library.python.port_manager.PortManager()
    grpc_port = pm.get_port()

    binary_path = ya_common.binary_path("ydb/tests/fq/streaming_common/iam_grpc_emulator/bin/iam_grpc_emulator")
    assert binary_path, "iam_grpc_emulator binary not found"

    cmd = [
        binary_path,
        "--port", str(grpc_port),
    ]

    ya_common.execute(
        cmd,
        wait=False,
        stdout=ya_common.output_path("iam_grpc_emulator.stdout"),
        stderr=ya_common.output_path("iam_grpc_emulator.stderr"),
    )

    set_env("IAM_EMULATOR_ENDPOINT", f"localhost:{grpc_port}")

    pid = os.fork()
    if pid == 0:
        signal.pause()
    else:
        with open(PID_FILENAME, "w") as f:
            f.write(str(pid))

    logger.debug(f"IAM gRPC emulator recipe started on localhost:{grpc_port}")


def stop(argv):
    logger.debug("Stopping IAM gRPC emulator recipe")
    with open(PID_FILENAME, "r") as f:
        pid = int(f.read())
        os.kill(pid, 9)


if __name__ == "__main__":
    declare_recipe(start, stop)
