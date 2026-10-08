"""Start HTTPS Keycloak and YDB requiring authenticated requests."""

import json
import os
import sys
from string import Template
import time
from pathlib import Path

import grpc
import yatest.common
import yaml
from ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc
from ydb.public.api.protos import ydb_discovery_pb2, ydb_status_codes_pb2
from library.python.port_manager import PortManager

from library.python.testing.recipe import declare_recipe, set_env
from ydb.public.tools.lib import cmds
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.functional.security.oidc.recipe import keycloak, tls
from ydb.tests.functional.security.oidc.lib.keycloak_client import approve_device, grant_counts


def export(name, value):
    os.environ[name] = str(value)
    set_env(name, str(value))


def wait_for_oidc(endpoint, token, sid, is_admin=False):
    # Server readiness does not imply that asynchronous discovery/JWKS loading
    # has finished. Wait for a real authenticated RPC before running clients.
    deadline = time.monotonic() + 30
    last_error = None
    expected_sid = f"{sid}@{os.environ['OIDC_AUTH_DOMAIN']}"
    with grpc.insecure_channel(endpoint) as channel:
        stub = ydb_discovery_v1_pb2_grpc.DiscoveryServiceStub(channel)
        while time.monotonic() < deadline:
            try:
                response = stub.WhoAmI(
                    ydb_discovery_pb2.WhoAmIRequest(),
                    metadata=[('x-ydb-database', '/Root'), ('x-ydb-auth-ticket', 'Bearer ' + token)],
                    timeout=3,
                )
                if response.operation.status == ydb_status_codes_pb2.StatusIds.SUCCESS:
                    identity = ydb_discovery_pb2.WhoAmIResult()
                    assert response.operation.result.Unpack(identity)
                    assert identity.user == expected_sid, identity.user
                    assert identity.is_administration_allowed == is_admin, identity
                    return
                last_error = str(response.operation.issues)
            except grpc.RpcError as error:
                last_error = error.details()
            time.sleep(0.1)
    raise RuntimeError('OIDC authentication did not become ready: ' + str(last_error))


def start(args):
    arguments = cmds.produce_arguments(args)
    recipe = cmds.Recipe(arguments)
    source = Path(yatest.common.source_path('ydb/tests/functional/security/oidc/recipe'))
    for name, value in json.loads((source / 'test-env.json').read_text()).items():
        export(name, value)
    directory = Path(yatest.common.output_path('oidc'))
    directory.mkdir(parents=True, exist_ok=True)
    tls.prepare(directory)
    export('OIDC_CA_FILE', directory / 'ca.pem')
    export('SSL_CERT_FILE', directory / 'ca.pem')
    export('OIDC_HELPER_BINARY', yatest.common.binary_path('ydb/tests/functional/security/oidc/recipe/oidc_recipe'))
    with PortManager() as ports:
        environment = keycloak.start(directory, ports.get_port())
        for name, value in environment.items():
            export(name, value)
        cluster = None
        try:
            configuration = KikimrConfigGenerator(
                binary_paths=[yatest.common.binary_path('ydb/apps/ydbd/ydbd')],
                output_path=recipe.generate_data_path(),
                domain_name='Root',
                nodes=1,
            )
            settings = yaml.safe_load(Template((source / 'ydb-config.yaml').read_text()).substitute(os.environ))
            configuration.yaml_config.setdefault('auth_config', {}).update(settings['auth_config'])
            configuration.yaml_config['domains_config']['security_config'].update(
                settings['domains_config']['security_config']
            )
            cluster = KiKiMR(configuration)
            cluster.root_token = 'Bearer ' + os.environ['OIDC_CLUSTER_ADMIN_TOKEN']
            cluster.start()
            recipe.write_metafile(
                {
                    'nodes': {
                        str(node_id): {
                            'pid': node.pid,
                            'command': node.command,
                            'cwd': node.cwd,
                            'stderr_file': node.stderr_file_name,
                            'stdout_file': node.stdout_file_name,
                            'pdisks': [],
                        }
                        for node_id, node in cluster.nodes.items()
                    }
                }
            )
            endpoint = f'localhost:{cluster.nodes[1].port}'
            wait_for_oidc(endpoint, os.environ['OIDC_ACCESS_TOKEN'], os.environ['OIDC_CLIENT_SID'])
            wait_for_oidc(
                endpoint, os.environ['OIDC_CLUSTER_ADMIN_TOKEN'], os.environ['YDB_CLUSTER_ADMIN'], is_admin=True
            )
            recipe.write_endpoint(endpoint)
            recipe.write_database('/Root')
            recipe.write_connection_string(f'grpc://{endpoint}?database=/Root')
        except BaseException:
            try:
                if cluster is not None:
                    cluster.stop(kill=True)
            finally:
                keycloak.stop()
            raise


def stop(args):
    try:
        cmds.stop_recipe(args)
    finally:
        keycloak.stop()


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--approve':
        approve_device(sys.argv[2])
    elif sys.argv[1:] == ['--stats']:
        print(json.dumps(grant_counts(), sort_keys=True))
    else:
        declare_recipe(start, stop)
