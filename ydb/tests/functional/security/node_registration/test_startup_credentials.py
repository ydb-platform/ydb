from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import os
from queue import Empty, Queue
import subprocess
import time

import grpc
import pytest
import yatest.common

from library.python.port_manager import PortManager
from ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc


class RegistrationRecorder(ydb_discovery_v1_pb2_grpc.DiscoveryServiceServicer):
    def __init__(self):
        self.requests = Queue()

    def NodeRegistration(self, request, context):
        self.requests.put((dict(context.invocation_metadata()), context.auth_context()))
        # Keep ydbd at registration: this test only needs its initial outgoing credentials.
        context.abort(grpc.StatusCode.UNAVAILABLE, 'Registration recorded by the test')

    def ListEndpoints(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, 'Use the explicitly configured endpoint')


@contextmanager
def running_node(command, log_path):
    with log_path.open('w') as log:
        process = subprocess.Popen(command, cwd=log_path.parent, stdout=log, stderr=subprocess.STDOUT)
        try:
            yield process
        finally:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)


def client_certificate_args(certificates, tmp_path):
    args = []
    for option, data in (
        ('grpc-ca', certificates['ca']),
        ('grpc-cert', certificates['allowed'][0]),
        ('grpc-key', certificates['allowed'][1]),
    ):
        path = tmp_path / f'{option}.pem'
        path.write_bytes(data)
        args += [f'--{option}', str(path)]
    return args


@pytest.mark.parametrize('protocol', ['grpc', 'grpcs'])
@pytest.mark.parametrize('auth_config,expected_token', [
    pytest.param(None, None, id='empty-static-config'),
    pytest.param('', None, id='empty-auth-config'),
    pytest.param('NodeRegistrationToken: ""', None, id='explicit-empty-token'),
    pytest.param('NodeRegistrationToken: "root@builtin"', 'root@builtin', id='explicit-root-token'),
    pytest.param('NodeRegistrationToken: "node@builtin"', 'node@builtin', id='explicit-custom-token'),
])
def test_startup_registration_credentials(certificates, tmp_path, protocol, auth_config, expected_token):
    log_path = tmp_path / 'ydbd.log'
    # An empty text protobuf is an empty TAppConfig, loaded by LoadBootstrapConfig.
    static_config = tmp_path / 'bootstrap.pb'
    static_config.write_text('')
    with PortManager() as ports, ThreadPoolExecutor(max_workers=1) as executor:
        server = grpc.server(executor)
        ydb_discovery_v1_pb2_grpc.add_DiscoveryServiceServicer_to_server(recorder, server)
        endpoint = f'localhost:{ports.get_port()}'
        command = [
            yatest.common.binary_path(os.environ['YDB_DRIVER_BINARY']), 'server',
            '--tenant', '/Root',
            '--node-broker', f'{protocol}://{endpoint}',
            '--ic-port', str(ports.get_port()),
            '--grpc-port', str(ports.get_port()),
            '--mon-port', str(ports.get_port()),
        ]
        if auth_config is not None:
            auth_path = tmp_path / 'auth.txt'
            auth_path.write_text(auth_config)
            command += ['--auth-file', str(auth_path)]
        if protocol == 'grpcs':
            server_cert, server_key = certificates['server']
            credentials = grpc.ssl_server_credentials(
                [(server_key, server_cert)], root_certificates=certificates['ca'], require_client_auth=True,
            )
            assert server.add_secure_port(endpoint, credentials)
            command += client_certificate_args(certificates, tmp_path)
        else:
            assert server.add_insecure_port(endpoint)
        command.append(str(static_config))
        server.start()
        try:
            with running_node(command, log_path):
                try:
                    metadata, peer_auth = recorder.requests.get(timeout=30)
                except Empty:
                    pytest.fail(f'ydbd did not send NodeRegistration: {log_path.read_text()}')
                assert metadata.get('x-ydb-auth-ticket') == expected_token, metadata
                if protocol == 'grpcs':
                    assert peer_auth['x509_common_name'] == [b'allowed'], peer_auth
        finally:
            server.stop(0).wait()


@pytest.mark.parametrize('cluster', [['dynamic-nodes@cert']], indirect=True, ids=['certificate-auth-only'])
@pytest.mark.parametrize('static_config_text', ['', 'AuthConfig {}'], ids=['empty-config', 'empty-auth-section'])
def test_startup_registration_with_empty_config(cluster, certificates, tmp_path, static_config_text):
    # root@builtin is deliberately NOT allowed to register nodes on this cluster:
    # its implicit transmission must fail even when a valid certificate is supplied.
    static_config = tmp_path / 'bootstrap.pb'
    static_config.write_text(static_config_text)
    log_path = tmp_path / 'ydbd.log'
    with PortManager() as ports:
        command = [
            yatest.common.binary_path(os.environ['YDB_DRIVER_BINARY']), 'server',
            '--node-broker', f'grpcs://localhost:{cluster.nodes[1].grpc_ssl_port}',
            '--node-domain', 'Root',
            '--tenant', '/Root',
            '--ic-port', str(ports.get_port()),
            '--grpc-port', str(ports.get_port()),
            '--mon-port', str(ports.get_port()),
            *client_certificate_args(certificates, tmp_path),
            str(static_config),
        ]
        with running_node(command, log_path) as process:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                output = log_path.read_text()
                assert 'Registration error:' not in output, output
                assert 'Configuration error:' not in output, output
                registered = 'Success. Registered as ' in output
                config_result = output.partition('Trying to get configs from ')[2]
                if registered and config_result and 'Success.' in config_result:
                    return
                assert process.poll() is None, output
                time.sleep(0.1)
            pytest.fail(f'ydbd did not register and fetch its config: {log_path.read_text()}')
