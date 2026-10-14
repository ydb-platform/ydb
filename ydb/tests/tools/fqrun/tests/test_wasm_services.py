import json
import os
from pathlib import Path
import signal
import subprocess

import pytest
import yatest.common

from ydb.tests.library.harness.tls_tools import generate_selfsigned_cert
from ydb.tests.tools.fqrun.examples.service.config import connection_ddl, profile_query, write_setup
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
    def __init__(self, directory, service, batch_rows=1, timeout_ms=30000):
        self.directory = directory
        write_setup(yatest.common.source_path(), directory, service, batch_rows, timeout_ms)
        self.config = directory / 'app.conf'
        self.results = directory / 'results.jsonl'
        self.stdout = directory / 'stdout.log'
        self.stderr = directory / 'stderr.log'
        self.log = directory / 'query.log'
        self.ast = directory / 'ast.yql'
        self.plan = directory / 'plan.json'

    def start(self, alias='profiles_http', sql=None, extra=(), execution='analytics', backend='in-place', backend_last=False):
        sql = sql if sql is not None else profile_query(alias)
        # Put --compute before --cfg: the override must not depend on argument order.
        command = [yatest.common.binary_path('ydb/tests/tools/fqrun/fqrun')]
        if backend is not None and not backend_last:
            command += ['--compute=' + backend]
        command += ['--cfg=' + str(self.config), '-C', execution, '--sql', sql,
                    '--result-file=' + str(self.results), '--log-file=' + str(self.log),
                    '--ast-file=' + str(self.ast), '--plan-file=' + str(self.plan)]
        if alias:
            command += ['--connection=' + str(self.directory / (alias + '.pb.txt'))]
        if backend is not None and backend_last:
            command += ['--compute=' + backend]
        command += list(extra)
        with self.stdout.open('wb') as out, self.stderr.open('wb') as err:
            # Never inherit real credentials from the developer's environment.
            environment = dict(os.environ, YQL_TOKEN='fqrun-test-yql-token')
            return subprocess.Popen(command, stdout=out, stderr=err, cwd=self.directory, env=environment)

    def wait(self, process):
        try:
            process.wait(timeout=120)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            pytest.fail('fqrun did not finish:\n' + self.output())
        return process.returncode

    def output(self):
        return '\n'.join(path.read_text(errors='replace') for path in
                         (self.stdout, self.stderr, self.log, self.ast, self.plan) if path.exists())

    def rows(self):
        return [json.loads(line) for line in self.results.read_text().splitlines() if line.strip()]


@pytest.mark.parametrize('service', [False, True], indirect=True)
@pytest.mark.parametrize('protocol', ['http', 'grpc'])
@pytest.mark.parametrize('batch_rows', [1, 2])
def test_profile(service, tmp_path, protocol, batch_rows):
    runner = Runner(tmp_path, service, batch_rows)
    assert runner.wait(runner.start('profiles_' + protocol)) == 0, runner.output()
    rows = runner.rows()
    assert [int(row['id']) for row in rows] == [42, 43]
    assert all(row['name'] == 'Ada' and int(row['score']) == 97 for row in rows)
    assert service.requests
    assert all(kind == protocol and auth == 'Bearer fqrun-example-token'
               for kind, _, auth in service.requests)
    ids = [value for _, request, _ in service.requests
           for value in (request if isinstance(request, tuple) else (request,))]
    assert sorted(ids) == [42, 43]
    assert 'fqrun-example-token' not in runner.output()


@pytest.mark.parametrize('protocol', ['http', 'grpc'])
@pytest.mark.parametrize('batch_rows', [1, 2])
def test_profile_200_rows(service, tmp_path, protocol, batch_rows):
    runner = Runner(tmp_path, service, batch_rows)
    runner.config.write_text(runner.config.read_text().replace(
        'WasmServices {', 'WasmServices { MaxBufferedRows: 2', 1))
    ids = list(range(1000, 1200))
    alias = 'profiles_' + protocol
    sql = "PRAGMA dq.MaxTasksPerStage='1';" + profile_query(alias, ids)
    assert runner.wait(runner.start(alias, sql)) == 0, runner.output()
    rows = runner.rows()
    assert [int(row['id']) for row in rows] == ids
    assert all(row['name'] == 'Ada' and int(row['score']) == 97 for row in rows)
    requests = [value for _, request, _ in service.requests
                for value in (request if isinstance(request, tuple) else (request,))]
    assert sorted(requests) == ids


@pytest.mark.parametrize('batch_rows', [1, 2])
@pytest.mark.parametrize('payload_size', [8, 1024])
def test_echo_200_rows(service, tmp_path, batch_rows, payload_size):
    runner = Runner(tmp_path, service, batch_rows)
    runner.config.write_text(runner.config.read_text().replace(
        'WasmServices {', 'WasmServices { MaxBufferedRows: 2', 1))
    messages = ['%03d:' % i + 'x' * (payload_size - 4) for i in range(200)]
    source = 'SELECT message FROM AS_TABLE(AsList(%s))' % ','.join(
        'AsStruct(%s || $padding AS message)' % json.dumps('%03d:' % i) for i in range(200))
    sql = (tmp_path / 'echo.sql').read_text().replace('SELECT "hello" AS message', source)
    sql = "PRAGMA dq.MaxTasksPerStage='1';$padding=%s;" % json.dumps('x' * (payload_size - 4)) + sql
    assert runner.wait(runner.start('echo_http', sql)) == 0, runner.output()
    rows = runner.rows()
    assert sorted(row['value'] for row in rows) == messages
    assert all(int(row['length']) == payload_size and row['empty'] is False for row in rows)


@pytest.mark.parametrize('protocol', ['http', 'grpc'])
def test_shared_transport_memory_limit(service, tmp_path, protocol):
    runner = Runner(tmp_path, service)
    runner.config.write_text(runner.config.read_text().replace(
        'WasmServices {', 'WasmServices { MaxTransportOperations: 1 MaxTransportBytes: 1', 1))
    assert runner.wait(runner.start('profiles_' + protocol)) != 0, runner.output()
    assert 'WASM service failed: error=1, detail=5' in runner.output()  # Transport / ResourceLimit
    assert not service.requests


def test_second_module(service, tmp_path):
    runner = Runner(tmp_path, service)
    assert runner.wait(runner.start('echo_http', (tmp_path / 'echo.sql').read_text())) == 0, runner.output()
    row, = runner.rows()
    assert row['value'] == 'hello' and int(row['length']) == 5
    assert int(row['delta']) == -5 and row['empty'] is False
    assert len(service.requests) == 1 and service.requests[0][0] == 'echo'


@pytest.mark.parametrize('backend', [None, 'in-place', 'ydb'])
def test_without_wasm(service, tmp_path, backend):
    runner = Runner(tmp_path, service)
    runner.config.write_text(Path(yatest.common.source_path(
        'ydb/tests/tools/fqrun/configuration/app_config.conf')).read_text())
    process = runner.start(alias=None, sql='SELECT 42 AS answer', backend=backend, backend_last=True)
    assert runner.wait(process) == 0, runner.output()
    assert int(runner.rows()[0]['answer']) == 42
    assert not service.requests


@pytest.mark.parametrize('extra,execution,backend,error', [
    ((), 'analytics', 'ydb', 'WASM services require IN_PLACE'),
    ((), 'analytics', None, 'WASM services require IN_PLACE'),
    ((), 'stream', 'in-place', 'streaming is unsupported'),
    (('--single-compute-db',), 'analytics', 'in-place', 'cannot be combined'),
    (('--shared-compute-db=/Root/db@localhost:1',), 'analytics', 'in-place', 'cannot be combined'),
])
def test_invalid_mode(service, tmp_path, extra, execution, backend, error):
    runner = Runner(tmp_path, service)
    assert runner.wait(runner.start(extra=extra, execution=execution, backend=backend)) != 0
    assert error in runner.output()
    assert not service.requests


@pytest.mark.parametrize('replacement,error', [
    (('profiles_http', 'missing_connection'), 'Unknown or inaccessible external service connection'),
    (("'Profile'", "'Missing'"), 'Unknown WASM service method'),
    (("'WASM_PROFILE'", "'MissingModule'"), None),
    (('Struct<id:Uint64>', 'Struct<id:Utf8>'), None),
])
def test_resolution_error(service, tmp_path, replacement, error):
    runner = Runner(tmp_path, service)
    sql = profile_query('profiles_http').replace(*replacement)
    assert runner.wait(runner.start(sql=sql)) != 0, runner.output()
    if error:
        assert error in runner.output()
    assert not service.requests


@pytest.mark.parametrize('corruption', ['missing_wasm', 'bad_manifest'])
def test_module_load_error(service, tmp_path, corruption):
    runner = Runner(tmp_path, service)
    config = runner.config.read_text()
    if corruption == 'missing_wasm':
        config = config.replace('profile.wasm', 'missing.wasm')
    else:
        manifest = tmp_path / 'bad_manifest.json'
        manifest.write_text('{}')
        config = config.replace(json.dumps(yatest.common.source_path('ydb/udfs/wasm/profile/manifest.json')),
                                json.dumps(str(manifest)))
    runner.config.write_text(config)
    assert runner.wait(runner.start()) != 0
    assert not service.requests


@pytest.mark.parametrize('protocol', ['http', 'grpc'])
def test_deadline(service, tmp_path, protocol):
    runner = Runner(tmp_path, service, timeout_ms=500)
    alias = 'profiles_' + protocol
    assert runner.wait(runner.start(alias, profile_query(alias, (999,)))) != 0, runner.output()
    assert service.started.is_set()


@pytest.mark.parametrize('execution', ['analytics', 'async-analytics'])
@pytest.mark.parametrize('sig', [signal.SIGINT, signal.SIGTERM])
def test_cancellation(service, tmp_path, execution, sig):
    runner = Runner(tmp_path, service)
    process = runner.start('profiles_grpc', profile_query('profiles_grpc', (999,)), execution=execution)
    try:
        assert service.started.wait(60), runner.output()
        process.send_signal(sig)
        assert service.cancelled.wait(25), runner.output()
        assert runner.wait(process) != 0, runner.output()
        assert 'ABORTED_BY_USER' in runner.output()
        assert 'Timed out waiting for query cancellation' not in runner.output()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def test_bad_response(service, tmp_path):
    runner = Runner(tmp_path, service)
    assert runner.wait(runner.start(sql=profile_query('profiles_http', (400,)))) != 0, runner.output()
    assert service.requests


def test_verbose_connection_does_not_print_secrets(service, tmp_path):
    runner = Runner(tmp_path, service)
    connection = tmp_path / 'profiles_http.pb.txt'
    connection.write_text(connection.read_text().replace(
        'protocol: HTTP',
        'headers { key: "X-Test-Secret" value: "fqrun-secret-header" } protocol: HTTP',
    ))
    assert runner.wait(runner.start(extra=('--verbosity=3',))) == 0, runner.output()
    output = runner.output()
    assert 'Creating connection' in output
    assert 'fqrun-example-token' not in output
    assert 'fqrun-secret-header' not in output


@pytest.mark.parametrize('service', [False, True], indirect=True)
@pytest.mark.parametrize('protocol', ['http', 'grpc'])
def test_ddl_services(service, tmp_path, protocol):
    runner = Runner(tmp_path, service)
    process = runner.start(alias=None, sql=profile_query('profiles_' + protocol), extra=(
        '--init-query=' + str(tmp_path / 'connections.sql'),
        '--secrets-file=' + str(tmp_path / 'secrets.json'), '--verbosity=3', '--scope-id=ddl-scope',
    ))
    assert runner.wait(process) == 0, runner.output()
    assert [int(row['id']) for row in runner.rows()] == [42, 43]
    assert all(auth == 'Bearer fqrun-example-token' for _, _, auth in service.requests)
    assert 'fqrun-example-token' not in runner.output()


def ddl_options(tmp_path, sql):
    path = tmp_path / 'init.sql'
    path.write_text(sql)
    return ('--init-query=' + str(path),)


def local_ddl(service, auth='NONE'):
    return connection_ddl('profiles_http', 'http://127.0.0.1:%d' % service.http.server_port, auth=auth)


@pytest.mark.parametrize('auth,expected', [('NONE', None), ('CURRENT_IAM', 'Bearer fqrun-test-yql-token')])
def test_ddl_auth(service, tmp_path, auth, expected):
    runner = Runner(tmp_path, service)
    process = runner.start(alias=None, sql=profile_query('profiles_http'),
                           extra=ddl_options(tmp_path, local_ddl(service, auth)))
    assert runner.wait(process) == 0, runner.output()
    assert service.requests and all(token == expected for _, _, token in service.requests)


@pytest.mark.parametrize('protobuf_first', [False, True])
def test_ddl_if_not_exists(service, tmp_path, protobuf_first):
    runner = Runner(tmp_path, service)
    ddl = local_ddl(service)
    repeated = ddl.replace('SOURCE `', 'SOURCE IF NOT EXISTS `').replace("127.0.0.1", "127.0.0.2")
    sql = repeated if protobuf_first else '-- SQL comment with ;\n' + ddl + repeated
    process = runner.start(alias='profiles_http' if protobuf_first else None, sql=profile_query('profiles_http'),
                           extra=ddl_options(tmp_path, sql) + ('--loop-count=2',))
    assert runner.wait(process) == 0, runner.output()
    assert len(service.requests) == 4  # Initialization runs once, outside the loop.


@pytest.mark.parametrize('mutation,error', [
    (lambda ddl: ddl + ddl, 'Failed to create connection'),
    (lambda ddl: ddl + 'SELECT 1;', 'supports only CREATE'),
    (lambda ddl: ddl.replace('CREATE EXTERNAL', 'CREATE OR REPLACE EXTERNAL'), 'OR REPLACE'),
    (lambda ddl: ddl.replace('`profiles_http`', 'other.`profiles_http`'), 'cluster-qualified'),
    (lambda ddl: ddl.replace('PROTOCOL = "HTTP"', 'PROTOCOL = "FTP"'), 'PROTOCOL must'),
    (lambda ddl: ddl.replace('USE_TLS = FALSE', 'USE_TLS = FALSE, METHOD = "PATCH"'), 'Unsupported ExternalService property'),
    (lambda ddl: ddl.replace('USE_TLS = FALSE', 'USE_TLS = TRUE'), 'scheme must match'),
    (lambda ddl: ddl.replace('USE_TLS = FALSE', 'USE_TLS = "maybe"'), 'USE_TLS must'),
    (lambda ddl: ddl.replace('USE_TLS = FALSE', 'USE_TLS = FALSE, UNKNOWN = "x"'), 'Unsupported ExternalService property'),
    (lambda ddl: ddl.replace('USE_TLS = FALSE', 'USE_TLS = FALSE, protocol = "HTTP"'), 'duplicate keys'),
    (lambda ddl: ddl.replace('USE_TLS = FALSE', 'USE_TLS = FALSE, TOKEN_SECRET_NAME = "unexpected"'), 'not supported'),
    (lambda ddl: ddl.replace('AUTH_METHOD = "NONE"', 'AUTH_METHOD = "TOKEN", TOKEN_SECRET_NAME = "missing"'),
     'not present in --secrets-file'),
    (lambda ddl: ddl.replace('SOURCE_TYPE = "ExternalService"', 'SOURCE_TYPE = "ObjectStorage"'), 'SOURCE_TYPE must'),
    (lambda ddl: ddl.replace('USE_TLS = FALSE', 'USE_TLS = FALSE, CA_CERTIFICATE = "pem"'), 'requires TLS'),
    (lambda ddl: '', 'must contain CREATE'),
    (lambda ddl: ddl.replace('WITH (', 'WITH (' + "'broken'"), 'Invalid initialization SQL'),
])
def test_ddl_errors(service, tmp_path, mutation, error):
    runner = Runner(tmp_path, service)
    process = runner.start(alias=None, sql=profile_query('profiles_http'),
                           extra=ddl_options(tmp_path, mutation(local_ddl(service))))
    assert runner.wait(process) != 0, runner.output()
    assert error in runner.output()
    assert not service.requests


def test_ddl_protobuf_name_conflict(service, tmp_path):
    runner = Runner(tmp_path, service)
    assert runner.wait(runner.start(extra=ddl_options(tmp_path, local_ddl(service)))) != 0
    assert 'Failed to create connection' in runner.output()
    assert not service.requests


def test_ddl_init_only(service, tmp_path):
    runner = Runner(tmp_path, service)
    command = [yatest.common.binary_path('ydb/tests/tools/fqrun/fqrun'), '--compute=in-place',
               '--cfg=' + str(runner.config), '--scope-id=init-only']
    command += list(ddl_options(tmp_path, local_ddl(service)))
    result = subprocess.run(command, capture_output=True, timeout=120,
                            env=dict(os.environ, YQL_TOKEN='fqrun-test-yql-token'))
    assert result.returncode == 0, result.stderr.decode(errors='replace')
    assert not service.requests


@pytest.mark.parametrize('signal_number', [signal.SIGTERM, signal.SIGINT])
@pytest.mark.parametrize('delay_kind', ['loop', 'query'])
def test_interrupt_between_queries(service, tmp_path, signal_number, delay_kind):
    import time
    runner = Runner(tmp_path, service)
    extra = ('--loop-count=2', '--loop-delay=60000') if delay_kind == 'loop' else (
        '--sql=SELECT 43', '--query-delay=60000')
    process = runner.start(alias=None, sql='SELECT 42', extra=extra)
    try:
        deadline = time.monotonic() + 30
        while 'Query finished. Duration:' not in runner.output():
            assert process.poll() is None, runner.output()
            assert time.monotonic() < deadline, 'First query did not finish: ' + runner.output()
            time.sleep(0.05)
        time.sleep(0.3)  # Let result handling finish and enter the inter-query delay.
        assert process.poll() is None
        process.send_signal(signal_number)
        process.wait(timeout=5)
        assert process.returncode != 0
        assert 'Query execution interrupted' in runner.output()
        assert runner.output().count('Query finished. Duration:') == 1
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()


@pytest.mark.parametrize('protocol', ['http', 'grpc'])
def test_runner_keeps_local_modules_with_server_source_default(service, tmp_path, protocol):
    runner = Runner(tmp_path, service)
    # Even an explicit server source setting cannot make the local runner
    # depend on UDF Store. No Store is enabled or populated in this process.
    runner.config.write_text(runner.config.read_text().replace(
        'WasmServices {', 'WasmServices { ModuleSource: UDF_STORE', 1))
    assert runner.wait(runner.start('profiles_' + protocol)) == 0, runner.output()
    assert [int(row['id']) for row in runner.rows()] == [42, 43]
    assert service.requests and all(kind == protocol for kind, _, _ in service.requests)


def monitoring_snapshot(runner, process):
    import re
    import time
    from urllib.request import urlopen
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        assert process.poll() is None, runner.output()
        assert 'Failed to execute query' not in runner.output(), runner.output()
        match = re.search(r'Monitoring port: .*?:(\d+)', runner.output())
        # Init DDL also logs Query finished; wait for the actual query result.
        if match and runner.results.exists() and runner.results.stat().st_size:
            with urlopen('http://localhost:' + match[1] + '/counters/counters=yq/json', timeout=5) as response:
                sensors = json.load(response)['sensors']
            pending = [s for s in sensors if s['labels'].get('subsystem') == 'wasm_services' and
                       s['labels'].get('sensor') in ('RequestsInFlight', 'ReservedBytes', 'CleanupPending') and s['value']]
            if not pending:
                return sensors
        time.sleep(0.05)
    pytest.fail('Query/monitoring did not become ready: ' + runner.output())


def assert_call_metrics(sensors, alias, protocol):
    selected = [sensor for sensor in sensors if all(sensor['labels'].get(k) == v for k, v in
                dict(subsystem='wasm_services', component='requests', call_alias=alias, protocol=protocol).items())]

    def counter(name, **labels):
        values = [s['value'] for s in selected if s['labels'].get('sensor') == name and
                  all(s['labels'].get(k) == v for k, v in labels.items())]
        assert len(values) == 1, selected
        return values[0]
    started = counter('RequestsStarted')
    assert started > 0
    assert counter('RequestsCompleted', outcome='success') == started
    assert counter('RequestsInFlight') == 0
    assert counter('ReservedBytes') == 0
    assert counter('CleanupPending') == 0
    assert any(s['labels'].get('sensor') == 'RequestDurationMs' for s in selected)
    assert not any('Tvm' in s['labels'].get('sensor', '') for s in selected)
    return started


@pytest.mark.parametrize('protocol', ['http', 'grpc'])
@pytest.mark.parametrize('batch_rows', [1, 2])
def test_call_alias_monitoring(service, tmp_path, protocol, batch_rows):
    runner = Runner(tmp_path, service, batch_rows)
    process = runner.start('profiles_' + protocol, extra=('--monitoring', '--hold'))
    try:
        sensors = monitoring_snapshot(runner, process)
        assert_call_metrics(sensors, 'profile.batch' if batch_rows == 2 else 'profile.lookup', protocol)
        labels = json.dumps([s['labels'] for s in sensors])
        assert 'fqrun-example-token' not in labels and '/profile' not in labels
    finally:
        process.terminate()
        runner.wait(process)
