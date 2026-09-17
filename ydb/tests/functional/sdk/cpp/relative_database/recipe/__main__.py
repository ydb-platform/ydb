import json
import os

import yatest.common
from library.python.testing.recipe import declare_recipe, set_env
from library.recipes.common import stop_daemon
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR

PID_FILE = "relative_database_pids.json"
DATABASE = "/Root/mydb"


def start(argv):
    config = KikimrConfigGenerator(
        binary_paths=[yatest.common.binary_path(os.getenv("YDB_DRIVER_BINARY", "ydb/apps/ydbd/ydbd"))],
        nodes=1,
        erasure=Erasure.NONE,
        domain_name="Root",
        extra_feature_flags=["enable_script_execution_operations"],
    )
    cluster = KiKiMR(config)
    try:
        cluster.start()
        cluster.create_database(DATABASE, storage_pool_units_count={"hdd": 1})
        slots = cluster.register_and_start_slots(DATABASE, count=1)
        cluster.wait_tenant_up(DATABASE)
        nodes = list(cluster.nodes.values()) + list(cluster.slots.values())
        with open(PID_FILE, "w") as output:
            json.dump([node.pid for node in nodes], output)
        set_env("YDB_ENDPOINT", f"localhost:{slots[0].grpc_port}")
        set_env("YDB_DATABASE", DATABASE)
    except Exception:
        cluster.stop()
        raise


def stop(argv):
    if os.path.exists(PID_FILE):
        with open(PID_FILE) as source:
            pids = json.load(source)
        for pid in reversed(pids):
            stop_daemon(pid)


if __name__ == "__main__":
    declare_recipe(start, stop)
