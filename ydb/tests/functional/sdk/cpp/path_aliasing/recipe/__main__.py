import os

from library.python.testing.recipe import declare_recipe
from ydb.public.tools.lib import cmds
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
import yatest.common


def save_processes(recipe, cluster):
    nodes = {}
    for kind, processes in (("static", cluster.nodes), ("slot", cluster.slots)):
        for node_id, node in processes.items():
            nodes["{}-{}".format(kind, node_id)] = {
                "pid": node.pid,
                "host": node.host,
                "grpc_port": node.grpc_port,
                "command": node.command,
                "cwd": node.cwd,
                "stderr_file": node.stderr_file_name,
                "stdout_file": node.stdout_file_name,
                "pdisks": [drive.get("pdisk_path") for drive in cluster.config.pdisks_info] if kind == "static" else [],
            }
    recipe.write_metafile({"nodes": nodes})


def start(args):
    del args
    recipe = cmds.Recipe(cmds.EmptyArguments())
    work_dir = recipe.generate_data_path()
    configuration = KikimrConfigGenerator(
        binary_paths=[yatest.common.binary_path("ydb/apps/ydbd/ydbd")],
        output_path=work_dir,
        domain_name="failover",
        nodes=1,
        default_clusteradmin="root@builtin",
        extra_feature_flags=["enable_fs_backups", "enable_export_filtering"],
    )
    # Add to the generated configuration; --config-path would replace it.
    configuration.yaml_config["resource_path_prefix_mapping"] = {
        "rules": [
            {"src": "/kfront", "dst": "/failover/kfront"},
            {"src": "/boundary", "dst": "/failover/k"},
            {
                "src": "/short-table",
                "dst": "/failover/kfront/TableResourcesAndRepeatedSourceDestinationOperands/table",
            },
        ]
    }
    cluster = KiKiMR(configuration)
    try:
        cluster.start()
        save_processes(recipe, cluster)
        database = "/failover/kfront"
        cluster.create_database(database, storage_pool_units_count={"hdd": 1}, token="root@builtin")
        cluster.register_and_start_slots(database, count=1)
        save_processes(recipe, cluster)
        cluster.wait_tenant_up(database, token="root@builtin")
        endpoint = "localhost:{}".format(cluster.nodes[1].grpc_port)
        recipe.write_endpoint(endpoint)
        recipe.write_database("/kfront")
        recipe.write_connection_string("grpc://{}?database=/kfront".format(endpoint))
        recipe.setenv("YDB_PATH_ALIAS_CANONICAL_DATABASE", "/failover/kfront")
        fs_dir = os.path.join(work_dir, "exports")
        os.makedirs(fs_dir, exist_ok=True)
        recipe.setenv("YDB_PATH_ALIAS_FS_DIR", fs_dir)
    except BaseException:
        cluster.stop()
        raise


if __name__ == "__main__":
    declare_recipe(start, cmds.stop_recipe)
