from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import os
import re
from queue import Empty, Queue
import subprocess
import time

import grpc
import pytest
import yatest.common
import yaml

from library.python.port_manager import PortManager
from helpers import YdbGrpcLog, canonical_log, registration_result
from test_node_authentication import open_channel
from ydb.core.protos import grpc_pb2_grpc
from ydb.public.api.grpc import ydb_discovery_v1_pb2_grpc
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator


class RegistrationRecorder(ydb_discovery_v1_pb2_grpc.DiscoveryServiceServicer,
                           grpc_pb2_grpc.TGRpcServerServicer):
    def __init__(self, channel, static_node_id):
        self.discovery = ydb_discovery_v1_pb2_grpc.DiscoveryServiceStub(channel)
        self.legacy = grpc_pb2_grpc.TGRpcServerStub(channel)
        self.static_node_id = static_node_id
        self.requests = Queue()

    def NodeRegistration(self, request, context):
        metadata = dict(context.invocation_metadata())
        log = {
            'token': metadata.get('x-ydb-auth-ticket'),
            'client_common_name': [value.decode() for value in context.auth_context().get('x509_common_name', [])],
        }
        try:
            response = self.discovery.NodeRegistration(request, metadata=context.invocation_metadata(), timeout=30)
        except grpc.RpcError as error:
            log.update(grpc_status=error.code().name, details=error.details())
            self.requests.put(log)
            context.abort(error.code(), error.details())
        log.update(grpc_status='OK')
        log.update(registration_result(response, 'discovery', self.static_node_id, request.port))
        self.requests.put(log)
        return response

    def ListEndpoints(self, request, context):
        return self.forward(self.discovery.ListEndpoints, request, context)

    def ConsoleRequest(self, request, context):
        return self.forward(self.legacy.ConsoleRequest, request, context)

    @staticmethod
    def forward(call, request, context):
        try:
            return call(request, metadata=context.invocation_metadata(), timeout=30)
        except grpc.RpcError as error:
            context.abort(error.code(), error.details())


def capture_startup(process, log_path, *, get_config=False):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        output = log_path.read_text()
        for match in re.finditer(r'^.*\n', output, re.MULTILINE):
            line = match.group()
            if 'Registration error:' in line or 'Configuration error:' in line:
                return output[:match.end()], None
            if get_config:
                if 'Success. Got dynamic config from ' in line:
                    return output[:match.end()], None
            elif 'Success. Registered as ' in line:
                return output[:match.end()], None
        if process.poll() is not None:
            return log_path.read_text(), process.returncode
        time.sleep(0.1)
    pytest.fail(f'ydbd did not finish initialization: {log_path.read_text()}')


def normalize_startup(output, tmp_path, endpoint, static_node_id):
    output = output.replace(str(tmp_path), '<test-dir>').replace(endpoint, '<node-broker>')
    output = re.sub(
        r'(Success\. Registered as )(\d+)',
        lambda match: match[1] + ('<dynamic-node>' if int(match[2]) > static_node_id else match[2]),
        output,
    )
    return output.splitlines()


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


@pytest.mark.parametrize('cluster', [['root@builtin', 'node@builtin', 'dynamic-nodes@cert']],
                         indirect=True, ids=['startup-auth'])
@pytest.mark.parametrize('protocol', ['grpc', 'grpcs'])
@pytest.mark.parametrize('config_source,config_value', [
    pytest.param('none', None, id='without-config'),
    pytest.param('protobuf', '', id='empty-static-config'),
    pytest.param('protobuf', 'LogConfig { DefaultLevel: 5 }', id='protobuf-without-auth'),
    pytest.param('protobuf', [
        'LogConfig { DefaultLevel: 5 }',
        'DomainsConfig { Domain { DomainId: 1 Name: "Root" } }',
    ], id='multiple-protobuf-files'),
    pytest.param('protobuf', 'AuthConfig {}', id='empty-auth-section'),
    # Bootstrap auth values are applied after registration; preserve the legacy default here.
    pytest.param('protobuf', 'AuthConfig { NodeRegistrationToken: "" }', id='bootstrap-empty-token'),
    pytest.param('protobuf', 'AuthConfig { NodeRegistrationToken: "node@builtin" }', id='bootstrap-custom-token'),
    pytest.param('auth-file', '', id='empty-auth-file-without-static-config'),
    pytest.param('auth-token-file', '', id='empty-auth-token-file-without-static-config'),
    pytest.param('log-file', '', id='only-log-config'),
    pytest.param('auth-file', 'NodeRegistrationToken: ""', id='empty-token-without-static-config'),
    pytest.param('auth-file', 'NodeRegistrationToken: "root@builtin"', id='root-token-without-static-config'),
    pytest.param('auth-file', 'NodeRegistrationToken: "node@builtin"', id='custom-token-without-static-config'),
    pytest.param('auth-token-file', 'NodeRegistrationToken: "node@builtin"', id='auth-token-file-without-static-config'),
    pytest.param('bootstrap-auth-file', '', id='empty-auth-file'),
    pytest.param('bootstrap-auth-token-file', '', id='empty-auth-token-file'),
    pytest.param('bootstrap-auth-file', 'NodeRegistrationToken: ""', id='explicit-empty-token'),
    pytest.param('bootstrap-auth-file', 'NodeRegistrationToken: "root@builtin"', id='explicit-root-token'),
    pytest.param('bootstrap-auth-file', 'NodeRegistrationToken: "node@builtin"', id='explicit-custom-token'),
    pytest.param('bootstrap-auth-token-file', 'NodeRegistrationToken: "node@builtin"', id='auth-token-file'),
    pytest.param('yaml', None, id='yaml-without-auth'),
    pytest.param('yaml', {}, id='yaml-empty-auth'),
    pytest.param('yaml', {'node_registration_token': ''}, id='yaml-empty-token'),
    pytest.param('yaml', {'node_registration_token': 'node@builtin'}, id='yaml-custom-token'),
    pytest.param('config-dir', None, id='config-directory-without-auth'),
    pytest.param('config-dir', {}, id='config-directory-empty-auth'),
    pytest.param('config-dir', {'node_registration_token': 'node@builtin'}, id='config-directory-token'),
    pytest.param('empty-config-dir', None, id='empty-config-directory'),
    pytest.param('missing-config-dir', None, id='missing-config-directory'),
    pytest.param('unreferenced-yaml', {'node_registration_token': 'node@builtin'}, id='config-without-cli-option'),
    pytest.param('missing-yaml', None, id='missing-yaml-file'),
    pytest.param('missing-protobuf', None, id='missing-protobuf-file'),
])
def test_startup_registration_credentials(cluster, certificates, tmp_path, protocol, config_source, config_value):
    log_path = tmp_path / 'ydbd.log'
    with (PortManager() as ports, ThreadPoolExecutor(max_workers=2) as executor,
          open_channel(cluster, certificates, protocol, 'allowed') as upstream):
        recorder = RegistrationRecorder(upstream, cluster.nodes[1].node_id)
        server = grpc.server(executor)
        ydb_discovery_v1_pb2_grpc.add_DiscoveryServiceServicer_to_server(recorder, server)
        grpc_pb2_grpc.add_TGRpcServerServicer_to_server(recorder, server)
        endpoint = f'localhost:{ports.get_port()}'
        command = [
            yatest.common.binary_path(os.environ['YDB_DRIVER_BINARY']), 'server',
            '--tenant', '/Root',
            '--node-domain', 'Root',
            '--node-host', 'localhost',
            '--node-resolve-host', 'localhost',
            '--node-address', '127.0.0.1',
            '--node-broker', f'{protocol}://{endpoint}',
            '--ic-port', str(ports.get_port()),
            '--grpc-port', str(ports.get_port()),
            '--mon-port', str(ports.get_port()),
        ]
        if config_source in ('yaml', 'config-dir', 'unreferenced-yaml'):
            config = KikimrConfigGenerator(nodes=1).yaml_config
            config.pop('auth_config', None)
            if config_value is not None:
                config['auth_config'] = config_value
            config_path = tmp_path / 'config.yaml'
            config_path.write_text(yaml.safe_dump(config))
            if config_source == 'yaml':
                command += ['--yaml-config', str(config_path)]
            elif config_source == 'config-dir':
                command += ['--config-dir', str(tmp_path)]
        elif config_source == 'empty-config-dir':
            command += ['--config-dir', str(tmp_path)]
        elif config_source == 'missing-config-dir':
            command += ['--config-dir', str(tmp_path / 'missing')]
        elif config_source == 'missing-yaml':
            command += ['--yaml-config', str(tmp_path / 'missing.yaml')]
        elif config_source not in ('none', 'protobuf', 'missing-protobuf'):
            config_path = tmp_path / 'config.txt'
            config_path.write_text(config_value)
            option = config_source.removeprefix('bootstrap-')
            command += [f'--{option}', str(config_path)]
        if protocol == 'grpcs':
            server_cert, server_key = certificates['server']
            credentials = grpc.ssl_server_credentials(
                [(server_key, server_cert)], root_certificates=certificates['ca'], require_client_auth=True,
            )
            server.add_secure_port(endpoint, credentials)
            command += client_certificate_args(certificates, tmp_path)
        else:
            server.add_insecure_port(endpoint)
        if config_source == 'protobuf' or config_source.startswith('bootstrap-'):
            configs = config_value if config_source == 'protobuf' else ''
            if isinstance(configs, str):
                configs = [configs]
            for index, content in enumerate(configs):
                static_config = tmp_path / f'bootstrap-{index}.pb'
                static_config.write_text(content)
                # Positional paths reach the configurator through freeArgs.
                command.append(str(static_config))
        elif config_source == 'missing-protobuf':
            command.append(str(tmp_path / 'missing.pb'))
        server.start()
        try:
            server_log = YdbGrpcLog(cluster)
            with running_node(command, log_path) as process:
                output, exit_code = capture_startup(process, log_path)
                log = {'startup': normalize_startup(output, tmp_path, endpoint, cluster.nodes[1].node_id)}
                if exit_code is not None:
                    log['exit_code'] = exit_code
                try:
                    log['registration'] = recorder.requests.get_nowait()
                except Empty:
                    log['registration'] = None
                log['ydb_log'] = server_log.response('NodeRegistration') if log['registration'] is not None else []
                return canonical_log(tmp_path, log)
        finally:
            server.stop(0).wait()


@pytest.mark.parametrize('cluster', [['dynamic-nodes@cert']], indirect=True, ids=['certificate-auth-only'])
@pytest.mark.parametrize('empty_config_dir', [False, True], ids=['no-config', 'empty-config-directory'])
def test_startup_registration_without_config(cluster, certificates, tmp_path, empty_config_dir):
    # root@builtin is deliberately NOT allowed to register nodes on this cluster:
    # its implicit transmission must fail even when a valid certificate is supplied.
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
        ]
        if empty_config_dir:
            command += ['--config-dir', str(tmp_path)]
        server_log = YdbGrpcLog(cluster)
        with running_node(command, log_path) as process:
            output, exit_code = capture_startup(process, log_path, get_config=True)
            endpoint = f'localhost:{cluster.nodes[1].grpc_ssl_port}'
            return canonical_log(tmp_path, {
                'exit_code': exit_code,
                'startup': normalize_startup(output, tmp_path, endpoint, cluster.nodes[1].node_id),
                'ydb_log': server_log.response('NodeRegistration'),
            })
