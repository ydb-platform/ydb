import logging
import os
import signal

import library.python.port_manager
import yatest.common as ya_common
from library.python.testing.recipe import declare_recipe, set_env

PID_FILENAME = "vm_metadata_emulator_recipe.pid"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def start(argv):
    logger.debug("Starting IAM emulator recipe")

    pm = library.python.port_manager.PortManager()
    http_port = pm.get_port()

    binary_path = ya_common.binary_path("ydb/tests/fq/streaming_common/vm_metadata_emulator/bin/vm_metadata_emulator")
    assert binary_path, "vm_metadata_emulator binary not found"

    cmd = [
        binary_path,
        "--port", str(http_port),
    ]

    res = ya_common.execute(
        cmd,
        wait=False,
        stdout=ya_common.output_path("vm_metadata_emulator.stdout"),
        stderr=ya_common.output_path("vm_metadata_emulator.stderr"),
    )

    set_env("VM_METADATA_EMULATOR_HOST", "localhost")
    set_env("VM_METADATA_EMULATOR_PORT", str(http_port))

    pid = os.fork()
    if pid == 0:
        signal.pause()
    else:
        with open(PID_FILENAME, "w") as f:
            f.write(str(pid))

    logger.debug(f"IAM emulator recipe started on localhost:{http_port}")


def stop(argv):
    logger.debug("Stopping IAM emulator recipe")
    with open(PID_FILENAME, "r") as f:
        pid = int(f.read())
        os.kill(pid, 9)


if __name__ == "__main__":
    declare_recipe(start, stop)
