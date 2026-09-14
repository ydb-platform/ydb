import os

import grpc
import yatest.common

from library.python.port_manager import PortManager
from library.python.testing.recipe import declare_recipe, set_env
from library.recipes import common as recipes_common
from ydb.public.api.grpc import ydb_federation_discovery_v1_pb2_grpc
from ydb.public.api.protos import ydb_federation_discovery_pb2, ydb_status_codes_pb2


DAEMON_NAME = "federation_discovery"
PID_FILENAME = "federation_discovery_recipe.pid"


def start(argv):
    pm = PortManager()
    port = pm.get_port()
    endpoint = f"localhost:{port}"
    command = [
        yatest.common.binary_path("ydb/public/tools/federation_discovery_recipe/bin/federation_discovery"),
        "--port", str(port),
        "--cm-endpoint", f"localhost:{os.environ['CM_PORT']}",
        "--cluster-a-endpoint", f"localhost:{os.environ['cluster_a_port']}",
        "--cluster-b-endpoint", f"localhost:{os.environ['cluster_b_port']}",
    ]

    with grpc.insecure_channel(endpoint) as channel:
        stub = ydb_federation_discovery_v1_pb2_grpc.FederationDiscoveryServiceStub(channel)

        def is_ready():
            try:
                response = stub.ListFederationDatabases(
                    ydb_federation_discovery_pb2.ListFederationDatabasesRequest(),
                    metadata=(("x-ydb-database", "/logbroker-federation/prod"),),
                    timeout=1,
                )
            except grpc.RpcError:
                return False
            result = ydb_federation_discovery_pb2.ListFederationDatabasesResult()
            return (
                response.operation.ready
                and response.operation.status == ydb_status_codes_pb2.StatusIds.SUCCESS
                and response.operation.result.Unpack(result)
                and [db.name for db in result.federation_databases] == ["cluster_a", "cluster_b"]
            )

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
