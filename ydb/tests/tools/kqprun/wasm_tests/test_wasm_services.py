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


@pytest.mark.parametrize('wasm_auth', [False, True])
@pytest.mark.parametrize('sql_batch_size', [None, 8])
def test_udf_store_upload_after_start(service, tmp_path, wasm_auth, sql_batch_size):
    import time
    import grpc
    import ydb
    import re
    from ydb.public.api.grpc.ydb_udf_v1_pb2_grpc import UdfServiceStub
    from ydb.public.api.protos import ydb_udf_pb2 as udf
    from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

    database = '/Root/services'
    runner = Runner(tmp_path, service)
    if wasm_auth:
        service.auth_secret = b'public-test-secret'
        provider = 'AuthProviders { Provider: "TEST_AUTH" Module: "TestAuth" Endpoint: %s }' % json.dumps(
            'http://127.0.0.1:%d' % service.http.server_port)
        runner.config.write_text(runner.config.read_text().replace('WasmServices {', 'WasmServices { ' + provider))
        ddl = connection_ddl('/Root/profiles', 'http://127.0.0.1:%d' % service.http.server_port, auth='WASM')
        ddl = ddl.replace('AUTH_METHOD = "WASM"', 'AUTH_METHOD = "WASM", AUTH_PROVIDER = "TEST_AUTH", AUTH_PARAMETERS = ' +
                          json.dumps(json.dumps({'secret': '/Root/auth_secret'})))
        runner.scheme.write_text("CREATE SECRET `/Root/auth_secret` WITH (value='public-test-secret');\n" + ddl)
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
            artifacts = [(runner.module.read_bytes(), Path(str(runner.module) + '.manifest.json').read_text())]
            if wasm_auth:
                auth_root = Path(yatest.common.source_path('ydb/udfs/wasm/auth_test'))
                artifacts.append(((auth_root / 'ut/data/auth_test.wasm').read_bytes(), (auth_root / 'manifest.json').read_text()))
            metadata = (('x-ydb-database', database),)
            for body, manifest in artifacts:
                chunks = [udf.UploadModuleChunk(metadata=udf.UploadModuleMetadata(
                    params=udf.UploadModuleParams(manifest_json=manifest), total_size=len(body))),
                    udf.UploadModuleChunk(data=body)]
                responses = list(stub.UploadModule(iter(chunks), metadata=metadata, timeout=60))
                assert len(responses) == 1 and responses[0].operation.status == StatusIds.SUCCESS, responses
                deadline = time.monotonic() + 120
                while True:
                    response = stub.DescribeModule(udf.DescribeModuleRequest(name=json.loads(manifest)['module_name']), metadata=metadata, timeout=15)
                    result = udf.DescribeModuleResult()
                    assert response.operation.result.Unpack(result), response
                    assert all(platform.status != udf.FAILED for platform in result.platforms), result
                    if result.platforms and all(platform.status == udf.READY for platform in result.platforms):
                        break
                    assert time.monotonic() < deadline, result
                    time.sleep(0.5)
        credentials = ydb.AccessTokenCredentials("root@builtin") if wasm_auth else None
        with ydb.Driver(endpoint=endpoint, database=database, credentials=credentials) as driver:
            driver.wait(timeout=30)
            with ydb.QuerySessionPool(driver) as pool:
                pool.execute_with_retries(runner.scheme.read_text().replace('/Root/', database + '/'))
                deadline = time.monotonic() + 60
                while True:
                    try:
                        result = pool.execute_with_retries(profile_query(
                            database + '/profiles', range(1000, 1200), batch_size=sql_batch_size))
                        break
                    except ydb.Error as error:
                        assert time.monotonic() < deadline and ('Unknown' in str(error) or 'not loaded' in str(error)), error
                        time.sleep(0.5)
                assert [row.id for row in result[0].rows] == list(range(1000, 1200))
                assert all(row.name == 'Ada' and row.score == 97 for row in result[0].rows)
                assert service.requests
                calls = [request for _, request, _ in service.requests]
                if sql_batch_size is None:
                    assert all(not isinstance(request, tuple) for request in calls)
                else:
                    assert all(isinstance(request, tuple) and 1 <= len(request) <= sql_batch_size for request in calls)
                    assert any(len(request) > 1 for request in calls)
                received = [value for request in calls for value in (request if isinstance(request, tuple) else (request,))]
                assert sorted(received) == list(range(1000, 1200))
                if wasm_auth:
                    assert service.auth_requests and all(value == service.auth_secret for value in service.auth_requests)
                    assert service.auth_headers and all(ticket == 'synthetic-auth-ticket' for _, ticket in service.auth_headers)
                    assert 'public-test-secret' not in runner.output()
                    assert 'synthetic-auth-ticket' not in runner.output()
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


@pytest.mark.parametrize('protocol', ['HTTP', 'GRPC'])
def test_wasm_auth_schema_secret(service, tmp_path, protocol):
    service.auth_secret = b'public-test-secret'
    runner = Runner(tmp_path, service, protocol)
    root = Path(yatest.common.source_path('ydb/udfs/wasm/auth_test'))
    module = tmp_path / 'auth.wasm'
    shutil.copyfile(root / 'ut/data/auth_test.wasm', module)
    shutil.copyfile(root / 'manifest.json', str(module) + '.manifest.json')
    provider = 'AuthProviders { Provider: "TEST_AUTH" Module: "TestAuth" Endpoint: %s Alias: "auth.test" }' % json.dumps(
        'http://127.0.0.1:%d' % service.http.server_port)
    runner.config.write_text(runner.config.read_text().replace('WasmServices {', 'WasmServices { ' + provider))
    endpoint = 'http://127.0.0.1:%d' % service.http.server_port if protocol == 'HTTP' else '127.0.0.1:%d' % service.grpc_port
    ddl = connection_ddl('/Root/profiles', endpoint, protocol, auth='WASM')
    ddl = ddl.replace('AUTH_METHOD = "WASM"', 'AUTH_METHOD = "WASM", AUTH_PROVIDER = "TEST_AUTH", AUTH_PARAMETERS = ' +
                      json.dumps(json.dumps({'secret': '/Root/auth_secret'})))
    runner.scheme.write_text("CREATE SECRET `/Root/auth_secret` WITH (value='public-test-secret');\n" + ddl)
    assert runner.wait(runner.start(ids=range(1000, 1200), extra=['--wasm-service-module', str(module), '--threads=1'])) == 0, runner.output()
    assert len(runner.rows()) == 200
    assert service.auth_requests and all(value == service.auth_secret for value in service.auth_requests)
    assert len(service.auth_requests) < len(service.requests)
    assert len(service.auth_headers) == len(service.requests)
    assert all(ticket == 'synthetic-auth-ticket' for _, ticket in service.auth_headers)
    assert 'public-test-secret' not in runner.output()
    assert 'synthetic-auth-ticket' not in runner.output()


@pytest.mark.parametrize('protocol', ['http', 'grpc'])
@pytest.mark.parametrize('default_rows,sql_rows', [(1, 8), (1, 64), (64, 1), (8, None), (0, None)])
def test_sql_batch_size(service, tmp_path, protocol, default_rows, sql_rows):
    runner = Runner(tmp_path, service, protocol.upper(), default_rows)
    alias = '/Root/profiles'
    ids = list(range(1000, 1200))
    sql = profile_query(alias, ids, batch_size=sql_rows)
    assert runner.wait(runner.start(sql=sql)) == 0, runner.output()
    assert [int(row['id']) for row in runner.rows()] == ids
    effective = (default_rows or 1) if sql_rows is None else sql_rows
    batches = [request for _, request, _ in service.requests]
    assert batches
    if effective == 1:
        assert all(not isinstance(request, tuple) for request in batches), batches
    else:
        assert all(isinstance(request, tuple) and 1 <= len(request) <= effective for request in batches), batches
        assert any(len(request) > 1 for request in batches), batches
    received = [value for request in batches for value in (request if isinstance(request, tuple) else (request,))]
    assert sorted(received) == ids


@pytest.mark.parametrize('sql_rows', ['0', '-1', '65', '2147483648', '"eight"'])
def test_sql_batch_size_invalid(service, tmp_path, sql_rows):
    protocol, default_rows = 'http', 1
    runner = Runner(tmp_path, service, protocol.upper(), default_rows)
    alias = '/Root/profiles'
    sql = profile_query(alias, batch_size=sql_rows)
    assert runner.wait(runner.start(sql=sql)) != 0, runner.output()
    assert 'BATCH_SIZE' in runner.output() or 'Int32' in runner.output(), runner.output()
    assert not service.requests


@pytest.mark.parametrize('sql_rows', [1, 8])
def test_sql_batch_size_scalar_manifest(service, tmp_path, sql_rows):
    runner = Runner(tmp_path, service, batch_rows=64)
    alias = '/Root/profiles'
    manifest = Path(str(runner.module) + '.manifest.json')
    document = json.loads(manifest.read_text())
    method = document['service_methods'][0]
    method.update(batch=False, max_batch_rows=1)
    manifest.write_text(json.dumps(document))
    sql = profile_query(alias, batch_size=sql_rows)
    code = runner.wait(runner.start(sql=sql))
    if sql_rows == 1:
        assert code == 0, runner.output()
        assert len(runner.rows()) == 2
        assert all(not isinstance(request, tuple) for _, request, _ in service.requests)
    else:
        assert code != 0, runner.output()
        assert 'does not support batching' in runner.output()
        assert not service.requests


def test_sql_batch_size_per_invocation(service, tmp_path):
    runner = Runner(tmp_path, service, batch_rows=64)
    alias = '/Root/profiles'
    scalar = profile_query(alias, range(1000, 1016), batch_size=1)
    batch = profile_query(alias, range(2000, 2016), batch_size='$batch_size')
    # Keep both calls in one execution, sharing module/method/connection.
    scalar = scalar[:scalar.index('SELECT id, name, score FROM $profiles')]
    scalar = scalar.replace('$input', '$scalar_input').replace('$profiles', '$scalar_profiles')
    batch = batch[:batch.index('SELECT id, name, score FROM $profiles')]
    sql = '$batch_size=8;' + scalar + batch + 'SELECT * FROM $scalar_profiles UNION ALL SELECT * FROM $profiles;'
    assert runner.wait(runner.start(sql=sql)) == 0, runner.output()
    received = []
    batched = False
    for _, request, _ in service.requests:
        if isinstance(request, tuple):
            assert 1 <= len(request) <= 8 and all(2000 <= value < 2016 for value in request)
            batched |= len(request) > 1
            received.extend(request)
        else:
            assert 1000 <= request < 1016
            received.append(request)
    assert batched
    expected = list(range(1000, 1016)) + list(range(2000, 2016))
    assert sorted(received) == expected
    assert sorted(int(row['id']) for row in runner.rows()) == expected


@pytest.mark.parametrize('use_lambda', [False, True])
@pytest.mark.parametrize('batch_size', [1, 8, 64])
def test_service_expression_200_rows(service, tmp_path, use_lambda, batch_size):
    runner = Runner(tmp_path, service)
    options = "AsStruct('%s' AS connection, %d AS batch_size)" % ('/Root/profiles', batch_size)
    source = ','.join('AsStruct(%dul AS ordinal, %dul AS id, "row-%d" AS payload)' %
                      (i, 1000 + i % 3, i) for i in range(200))
    expression = '$lookup(id)' if use_lambda else 'WASM_PROFILE::Profile(id, $options)'
    sql = (
        "$options = " + options + ";"
        "$lookup = ($id) -> { RETURN WASM_PROFILE::Profile($id, $options); };"
        "$input = SELECT * FROM AS_TABLE(AsList(" + source + "));"
        "$result = SELECT ordinal, payload, " + expression + " AS profile FROM $input;"
        "SELECT ordinal, payload, profile.id AS id, profile.name AS name, profile.score AS score "
        "FROM $result ORDER BY ordinal;"
    )
    assert runner.wait(runner.start(sql=sql)) == 0, runner.output()
    rows = runner.rows()
    assert len(rows) == 200
    assert [int(row['ordinal']) for row in rows] == list(range(200))
    assert [int(row['id']) for row in rows] == [1000 + i % 3 for i in range(200)]
    assert [row['payload'] for row in rows] == ['row-%d' % i for i in range(200)]
    assert all(row['name'] == 'Ada' and int(row['score']) == 97 for row in rows)
    batches = [request if isinstance(request, tuple) else (request,) for _, request, _ in service.requests]
    assert sum(map(len, batches)) == 200
    assert all(1 <= len(batch) <= batch_size for batch in batches)
    if batch_size > 1:
        assert any(len(batch) > 1 for batch in batches)
    assert 'WasmService' in runner.plan.read_text()


@pytest.mark.parametrize('dependent', [False, True])
def test_service_expression_multiple_calls(service, tmp_path, dependent):
    runner = Runner(tmp_path, service)
    second = 'WASM_PROFILE::Profile(id, $options)' if not dependent else 'WASM_PROFILE::Profile(first.id, $options)'
    sql = (
        "$options=AsStruct('/Root/profiles' AS connection, 8 AS batch_size);"
        "$input=SELECT * FROM AS_TABLE(AsList(AsStruct(42ul AS id), AsStruct(43ul AS id)));"
        "$first=SELECT id, WASM_PROFILE::Profile(id, $options) AS first FROM $input;"
        "$both=SELECT first, " + second + " AS second FROM $first;"
        "SELECT first.id AS id, second.id AS second_id FROM $both ORDER BY id;"
    )
    assert runner.wait(runner.start(sql=sql)) == 0, runner.output()
    assert [(int(row['id']), int(row['second_id'])) for row in runner.rows()] == [(42, 42), (43, 43)]
    received = [value for _, request, _ in service.requests
                for value in (request if isinstance(request, tuple) else (request,))]
    assert sorted(received) == [42, 42, 43, 43]


@pytest.mark.parametrize('expression', [
    "WASM_PROFILE::Profile(id, AsStruct('/Root/profiles' AS connection, 0 AS batch_size))",
    "WASM_PROFILE::Profile(CAST(id AS String), $options)",
    "WASM_PROFILE::Profile(id, AsStruct(CAST(id AS String) AS connection))",
])
def test_service_expression_invalid_before_network(service, tmp_path, expression):
    runner = Runner(tmp_path, service)
    sql = ("$options=AsStruct('/Root/profiles' AS connection);"
           "$input=SELECT 42ul AS id; SELECT " + expression + " AS profile FROM $input;")
    assert runner.wait(runner.start(sql=sql)) != 0
    assert not service.requests
    assert 'Service' in runner.output()
