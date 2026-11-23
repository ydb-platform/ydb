import os

import grpc
import yatest.common

from library.python.port_manager import PortManager
from library.python.testing.recipe import declare_recipe, set_env
from library.recipes import common as recipes_common


DAEMON_NAME = "federation_discovery"
PID_FILENAME = "federation_discovery_recipe.pid"


def start(argv):
    pm = PortManager()
    port = pm.get_port()
    endpoint = f"localhost:{port}"
    command = [
        yatest.common.binary_path("ydb/public/tools/federation_discovery_recipe/bin/federation_discovery"),
        "--port", str(port),
    ]

    with grpc.insecure_channel(endpoint) as channel:
        def is_ready():
            ready = grpc.channel_ready_future(channel)
            try:
                ready.result(timeout=1)
            except grpc.FutureTimeoutError:
                return False
            finally:
                ready.cancel()
            return True

        try:
            recipes_common.start_daemon(
                command=command,
                environment=None,
                is_alive_check=is_ready,
                pid_file_name=PID_FILENAME,
                timeout=30,
                daemon_name=DAEMON_NAME,
            )
            set_env("FEDERATION_DISCOVERY_ENDPOINT", endpoint)
        except Exception:
            stop(argv)
            raise


def stop(argv):
    if not os.path.exists(PID_FILENAME):
        return
    with open(PID_FILENAME) as pid_file:
        pid = int(pid_file.read())
    recipes_common.stop_daemon(pid)
    os.remove(PID_FILENAME)


if __name__ == "__main__":
    declare_recipe(start, stop)
