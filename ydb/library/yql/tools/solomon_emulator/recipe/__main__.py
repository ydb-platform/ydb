import argparse
import logging
import os
import signal

from library.python.testing.recipe import declare_recipe, set_env
from yatest.common.network import PortManager
import yatest.common as ya_common

PID_FILENAME = "solomon_recipe.pid"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


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
    pm = PortManager()
    http_port = pm.get_port()
    grpc_port = pm.get_port()
    binary_path = ya_common.binary_path("ydb/library/yql/tools/solomon_emulator/bin/solomon_emulator")
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

    res = ya_common.execute(
        cmd,
        wait=False,
        stdout=ya_common.output_path("solomon_emulator.stdout"),
        stderr=ya_common.output_path("solomon_emulator.stderr"),
    )
    set_env("SOLOMON_EMULATOR_PID", str(res.process.pid))

    http_endpoint = f"localhost:{http_port}"
    grpc_endpoint = f"localhost:{grpc_port}"
    set_env("SOLOMON_HOST", "localhost")
    set_env("SOLOMON_HTTP_ENDPOINT", http_endpoint)
    set_env("SOLOMON_GRPC_ENDPOINT", grpc_endpoint)

    pid = os.fork()
    if pid == 0:
        signal.pause()
    else:
        with open(PID_FILENAME, "w") as f:
            f.write(str(pid))

    logger.debug(f"Solomon recipe has been started, http_endpoint: {http_endpoint}, grpc_endpoint: {grpc_endpoint}")


def stop(argv):
    logger.debug("Stop Solomon recipe")
    with open(PID_FILENAME, "r") as f:
        pid = int(f.read())
        os.kill(pid, 9)


if __name__ == "__main__":
    declare_recipe(start, stop)
