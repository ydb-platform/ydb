import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest
import yatest.common

from ydb.tests.library.harness.tls_tools import generate_selfsigned_cert
from ydb.tests.tools.fqrun.examples.service.config import connection_ddl, profile_query
from ydb.tests.tools.fqrun.examples.service.mock import MockServices


@pytest.fixture
def service(request, tmp_path):
    tls = getattr(request, 'param', False)
    options = {}
    if tls:
        cert, key = generate_selfsigned_cert('localhost')
        certificate, private_key = tmp_path / 'cert.pem', tmp_path / 'key.pem'
        certificate.write_bytes(cert)
        private_key.write_bytes(key)
        options = dict(certificate=certificate, private_key=private_key)
    instance = MockServices(**options)
    try:
        yield instance
    finally:
        instance.stop()


class Runner:
    def __init__(self, directory, service, protocol='HTTP', batch_rows=1, timeout=30000, tvm=False):
        self.directory = directory
        self.config = directory / 'app.conf'
        self.results = directory / 'results.jsonl'
        self.stdout, self.stderr, self.log = [directory / (name + '.log') for name in ('stdout', 'stderr', 'query')]
        self.plan = directory / 'plan.json'
        self.module = directory / 'profile.wasm'
        root = Path(yatest.common.source_path('ydb/udfs/wasm/profile'))
        shutil.copyfile(root / 'ut/data/profile.wasm', self.module)
        shutil.copyfile(root / 'manifest.json', str(self.module) + '.manifest.json')
        self.config.write_text('LogConfig { DefaultLevel: 5 }\nFederatedQueryConfig { WasmServices { '
                               'MaxBatchRows: %d CallTimeoutMs: %d } }\n' % (batch_rows, timeout))
        host = 'localhost' if service.tls else '127.0.0.1'
        address = ('%s://%s:%d' % ('https' if service.tls else 'http', host, service.http.server_port)) \
            if protocol == 'HTTP' else '%s:%d' % (host, service.grpc_port)
        self.scheme = directory / 'scheme.sql'
        secrets = ("CREATE SECRET example_token WITH (value='fqrun-example-token');\n"
                   "CREATE SECRET example_tvm_secret WITH (value='fqrun-example-tvm-secret');\n")
        ddl = connection_ddl('/Root/profiles', address, protocol, service.ca_certificate, auth='TVM' if tvm else 'TOKEN')
        # Schema secrets are database paths; avoid legacy owner-scoped secrets.
        ddl = ddl.replace('TOKEN_SECRET_NAME = "example_token"', 'TOKEN_SECRET_PATH = "/Root/example_token"')
        ddl = ddl.replace('TVM_SECRET_NAME = "example_tvm_secret"', 'TVM_SECRET_PATH = "/Root/example_tvm_secret"')
        self.scheme.write_text(secrets + ddl)

    def start(self, ids=(42, 43), extra=(), sql=None, config_last=False):
        command = [yatest.common.binary_path('ydb/tests/tools/kqprun/kqprun'), '-c', str(self.config),
                   '--wasm-service-module', str(self.module), '-s', str(self.scheme), '-C', 'query',
                   '--sql', sql or profile_query('/Root/profiles', ids),
                   '--log-default=warn', '--result-file', str(self.results), '--log-file', str(self.log), '--script-plan-file', str(self.plan), '-P', 'json']
        if config_last:
            command[1:3] = []
            command += ['-c', str(self.config)]
        command += list(extra)
        with self.stdout.open('wb') as out, self.stderr.open('wb') as err:
            return subprocess.Popen(command, stdout=out, stderr=err, cwd=self.directory,
                                    env=dict(os.environ, YQL_TOKEN='kqprun-test-token'))

    def wait(self, process, timeout=180):
        try:
            return process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            pytest.fail('kqprun did not finish:\n' + self.output())

    def output(self):
        return '\n'.join(p.read_text(errors='replace') for p in (self.stdout, self.stderr, self.log, self.plan) if p.exists())

    def rows(self):
        return [json.loads(line) for line in self.results.read_text().splitlines() if line.strip()]


@pytest.mark.parametrize('service', [False, True], indirect=True)
@pytest.mark.parametrize('protocol', ['HTTP', 'GRPC'])
@pytest.mark.parametrize('batch_rows', [1, 8])
def test_profile_200_rows(service, tmp_path, protocol, batch_rows):
    runner = Runner(tmp_path, service, protocol, batch_rows)
    assert runner.wait(runner.start(ids=range(1000, 1200))) == 0, runner.output()
    rows = runner.rows()
    assert [int(row['id']) for row in rows] == list(range(1000, 1200))
    assert all(row['name'] == 'Ada' and int(row['score']) == 97 for row in rows)
    assert service.requests
    assert all(kind == protocol.lower() and auth == 'Bearer fqrun-example-token' for kind, _, auth in service.requests)
    ids = [value for _, request, _ in service.requests for value in (request if isinstance(request, tuple) else (request,))]
    assert sorted(ids) == list(range(1000, 1200))
    assert 'fqrun-example-token' not in runner.output()
    assert 'WasmService' in runner.plan.read_text()


@pytest.mark.parametrize('protocol', ['HTTP', 'GRPC'])
def test_mock_tvm_and_real_provider_stub(service, tmp_path, protocol):
    service.require_tvm_ticket = True
    runner = Runner(tmp_path, service, protocol, tvm=True)
    assert runner.wait(runner.start(extra=['--mock-tvm', '--mock-tvm-delay-ms=100', '--threads=1'])) == 0, runner.output()
    assert len(runner.rows()) == 2
    assert service.tvm_tickets
    service.requests.clear()
    assert runner.wait(runner.start()) != 0
    assert 'TVM ticket provider is not implemented' in runner.output()
    assert not service.requests
    assert 'fqrun-example-tvm-secret' not in runner.output()


@pytest.mark.parametrize('protocol', ['HTTP', 'GRPC'])
def test_deadline_and_shutdown(service, tmp_path, protocol):
    runner = Runner(tmp_path, service, protocol, timeout=250)
    process = runner.start(ids=[999])
    assert service.started.wait(60), runner.output()
    # The embedded database must shut down without waiting for this response.
    assert runner.wait(process, timeout=15) != 0
    output = runner.output().lower()
    # libcurl's millisecond timer can finish just before the runtime deadline.
    # Profile then reports EServiceError::Transport / EClientError::Deadline.
    assert any(message in output for message in (
        'deadline', 'timed out', 'wasm service failed: error=1, detail=3')), runner.output()


def test_local_module_option_independent_of_config_order(service, tmp_path):
    runner = Runner(tmp_path, service)
    assert runner.wait(runner.start(config_last=True)) == 0, runner.output()
    assert len(runner.rows()) == 2


def test_udf_store_upload_after_start(service, tmp_path):
    import time
    import grpc
    import ydb
    import re
    from ydb.public.api.grpc.ydb_udf_v1_pb2_grpc import UdfServiceStub
    from ydb.public.api.protos import ydb_udf_pb2 as udf
    from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

    database = '/Root/services'
    runner = Runner(tmp_path, service)
    command = [yatest.common.binary_path('ydb/tests/tools/kqprun/kqprun'), '-c', str(runner.config),
               '--wasm-service-store', '--dedicated', 'services', '-D', database, '-G', '0', '--log-default=warn', '--log-file', str(runner.log)]
    with runner.stdout.open('wb') as out, runner.stderr.open('wb') as err:
        process = subprocess.Popen(command, stdout=out, stderr=err, cwd=tmp_path,
                                   env=dict(os.environ, YQL_TOKEN='kqprun-test-token'))
    try:
        ready = time.monotonic() + 90
        while 'Initialization finished' not in runner.stdout.read_text():
            assert process.poll() is None and time.monotonic() < ready, runner.output()
            time.sleep(0.1)
        match = re.search(r'Tenant \[services\] gRPC port: (\d+)', runner.stdout.read_text())
        assert match, runner.output()
        endpoint = 'localhost:' + match.group(1)
        with grpc.insecure_channel(endpoint) as channel:
            grpc.channel_ready_future(channel).result(timeout=90)
            stub = UdfServiceStub(channel)
            body = runner.module.read_bytes()
            manifest = Path(str(runner.module) + '.manifest.json').read_text()
            metadata = (('x-ydb-database', database),)
            chunks = [udf.UploadModuleChunk(metadata=udf.UploadModuleMetadata(
                params=udf.UploadModuleParams(manifest_json=manifest), total_size=len(body))),
                udf.UploadModuleChunk(data=body)]
            responses = list(stub.UploadModule(iter(chunks), metadata=metadata, timeout=60))
            assert len(responses) == 1 and responses[0].operation.status == StatusIds.SUCCESS, responses
            deadline = time.monotonic() + 120
            while True:
                response = stub.DescribeModule(udf.DescribeModuleRequest(name='WASM_PROFILE'), metadata=metadata, timeout=15)
                result = udf.DescribeModuleResult()
                assert response.operation.result.Unpack(result), response
                assert all(platform.status != udf.FAILED for platform in result.platforms), result
                if result.platforms and all(platform.status == udf.READY for platform in result.platforms):
                    break
                assert time.monotonic() < deadline, result
                time.sleep(0.5)
        with ydb.Driver(endpoint=endpoint, database=database) as driver:
            driver.wait(timeout=30)
            with ydb.QuerySessionPool(driver) as pool:
                pool.execute_with_retries(runner.scheme.read_text().replace('/Root/', database + '/'))
                deadline = time.monotonic() + 60
                while True:
                    try:
                        result = pool.execute_with_retries(profile_query(database + '/profiles'))
                        break
                    except ydb.Error as error:
                        assert time.monotonic() < deadline and ('Unknown' in str(error) or 'not loaded' in str(error)), error
                        time.sleep(0.5)
                assert [row.id for row in result[0].rows] == [42, 43]
                assert all(row.name == 'Ada' and row.score == 97 for row in result[0].rows)
                assert service.requests
    finally:
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def test_store_requires_tenant(service, tmp_path):
    runner = Runner(tmp_path, service)
    command = [yatest.common.binary_path('ydb/tests/tools/kqprun/kqprun'), '-c', str(runner.config),
               '--wasm-service-store', '--sql', 'SELECT 1']
    result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    assert result.returncode != 0
    assert 'requires exactly one dedicated/shared tenant' in result.stderr


@pytest.mark.parametrize('configured', [False, True])
def test_store_rejects_multiple_tenants(service, tmp_path, configured):
    runner = Runner(tmp_path, service)
    command = [yatest.common.binary_path('ydb/tests/tools/kqprun/kqprun'), '-c', str(runner.config),
               '--dedicated', 'a', '--dedicated', 'b', '-D', '/Root/a', '-C', 'query', '--sql', 'SELECT 1']
    if configured:
        with runner.config.open('a') as config:
            config.write('UdfStoreConfig { Enabled: true EnableWasmUdf: true }\n')
    else:
        command.append('--wasm-service-store')
    result = subprocess.run(command, capture_output=True, text=True, timeout=30)
    assert result.returncode != 0
    assert 'requires exactly one dedicated/shared tenant' in result.stderr
    assert 'Initialization finished' not in result.stdout


def test_store_restores_persistent_tenant(service, tmp_path):
    runner = Runner(tmp_path, service)
    storage = tmp_path / 'storage'
    command = [yatest.common.binary_path('ydb/tests/tools/kqprun/kqprun'), '-c', str(runner.config),
               '--storage-path', str(storage), '--storage-size', '32', '--wasm-service-store',
               '-D', '/Root/services', '-C', 'query', '--sql', 'SELECT 1', '--log-default=warn']
    # The first invocation creates the tenant. The second must restore it from
    # kqprun_storage_meta.conf without repeating creation options.
    for extra in (['--dedicated', 'services'], []):
        result = subprocess.run(command + extra, capture_output=True, text=True, timeout=150,
                                env=dict(os.environ, YQL_TOKEN='kqprun-test-token'))
        assert result.returncode == 0, result.stdout + result.stderr
    assert (storage / 'kqprun_storage_meta.conf').exists()
