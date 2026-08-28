import hashlib
import json
import os
import re
import shutil
import signal
import time
from pathlib import Path

import pytest
import yatest.common


TLS_FILES = ('ca.pem', 'cert.pem', 'key.pem')
LOCAL_YDB_TIMEOUT = 180
READY_TIMEOUT = 90


def _binary_path(environment_variable):
    return yatest.common.binary_path(os.environ[environment_variable])


def _command_environment(**overrides):
    environment = os.environ.copy()
    for name in (
        'FQ_CONNECTOR_ENDPOINT',
        'GRPC_PORT',
        'GRPC_TLS_PORT',
        'MON_PORT',
        'POSTGRES_PASSWORD',
        'POSTGRES_USER',
        'YDB_ADDITIONAL_LOG_CONFIGS',
        'YDB_DEFAULT_LOG_LEVEL',
        'YDB_ENABLE_COLUMN_TABLES',
        'YDB_ENABLE_PQCD',
        'YDB_ENFORCE_USER_TOKEN_REQUIREMENT',
        'YDB_ERASURE',
        'YDB_FEATURE_FLAGS',
        'YDB_GRPC_ENABLE_TLS',
        'YDB_GRPC_SERVICES',
        'YDB_GRPC_TLS_DATA_PATH',
        'YDB_PQ_CLIENT_SERVICE_TYPES',
        'YDB_REPORT_MONITORING_INFO',
        'YDB_TEST_FIXED_PORT',
    ):
        environment.pop(name, None)
    environment.update(overrides)
    return environment


def _run(command, environment, check=True, timeout=LOCAL_YDB_TIMEOUT):
    return yatest.common.execute(
        [str(argument) for argument in command],
        env=environment,
        check_exit_code=check,
        text=True,
        timeout=timeout,
    )


def _pid_is_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    process_stat = Path('/proc/{}/stat'.format(pid))
    if process_stat.is_file() and process_stat.read_text().split()[2] == 'Z':
        return False
    return True


def _wait_for_pid_exit(pid):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if not _pid_is_alive(pid):
            return
        time.sleep(0.1)
    raise AssertionError('YDB process {} is still alive after local_ydb stop'.format(pid))


def _is_expected_ydb_process(pid):
    command_line = Path('/proc/{}/cmdline'.format(pid))
    try:
        content = command_line.read_bytes()
    except OSError:
        return False
    return str(_binary_path('YDB_DRIVER_BINARY')).encode('utf-8') in content


def _bundle_hash(directory):
    digest = hashlib.sha256()
    for filename in TLS_FILES:
        digest.update(filename.encode('utf-8'))
        digest.update((directory / filename).read_bytes())
    return digest.hexdigest()


def _json_rows(result):
    return [line for line in result.stdout.splitlines() if line.strip()]


def _set_default_log_level(config_path, level):
    content = config_path.read_text()
    section_match = re.search(r'(?m)^log_config:\s*$', content)
    assert section_match is not None, 'log_config is absent from generated config'

    next_section = re.search(r'(?m)^[^\s#][^\n]*:\s*$', content[section_match.end():])
    section_end = section_match.end() + next_section.start() if next_section else len(content)
    section = content[section_match.end():section_end]
    level_match = re.search(
        r'(?m)^([ \t]+default_level:[ \t]*)([0-9]+)([ \t]*(?:#.*)?)$',
        section,
    )
    assert level_match is not None, 'log_config.default_level is absent from generated config'
    assert int(level_match.group(2)) != level

    section = (
        section[:level_match.start()]
        + level_match.group(1)
        + str(level)
        + level_match.group(3)
        + section[level_match.end():]
    )
    config_path.write_text(content[:section_match.end()] + section + content[section_end:])


class LocalYdb:
    def __init__(self, working_directory, environment=None):
        self.working_directory = Path(working_directory)
        self.environment = environment if environment is not None else _command_environment()

    def _command(self, action, *extra_arguments, check=True):
        return _run(
            [
                _binary_path('LOCAL_YDB_BINARY'),
                action,
                '--ydb-working-dir',
                self.working_directory,
                '--ydb-binary-path',
                _binary_path('YDB_DRIVER_BINARY'),
                '--suppress-version-check',
                *extra_arguments,
            ],
            self.environment,
            check=check,
        )

    def deploy(self, check=True):
        return self._command('deploy', check=check)

    def stop(self):
        return self._command('stop')

    def start(self):
        return self._command('start')

    def update(self):
        return self._command('update')

    def cleanup(self):
        return self._command('cleanup')

    @property
    def recipe_path(self):
        return self.working_directory / 'ydb_recipe.json'

    @property
    def config_path(self):
        return self.working_directory / 'cluster' / 'kikimr_configs' / 'config.yaml'

    def recipe(self):
        return json.loads(self.recipe_path.read_text())

    def pid(self):
        nodes = self.recipe()['nodes']
        return int(nodes[sorted(nodes)[0]]['pid'])

    def _connection(self):
        endpoint = (self.working_directory / 'ydb_endpoint.txt').read_text().strip()
        database = (self.working_directory / 'ydb_database.txt').read_text().strip()
        return endpoint, '/' + database.lstrip('/')

    def query(self, statement, tls_ca=None, output_format=None, check=True):
        endpoint, database = self._connection()
        command = [
            _binary_path('YDB_CLI_BINARY'),
            '--endpoint',
            '{}://{}'.format('grpcs' if tls_ca else 'grpc', endpoint),
            '--database',
            database,
            '--no-discovery',
        ]
        if tls_ca:
            command.extend(['--ca-file', tls_ca])
        command.extend(['sql', '-s', statement])
        if output_format:
            command.extend(['--format', output_format])
        return _run(command, self.environment, check=check, timeout=30)

    def wait_for_query(self, statement, tls_ca=None, output_format=None):
        deadline = time.monotonic() + READY_TIMEOUT
        last_result = None
        while time.monotonic() < deadline:
            last_result = self.query(
                statement,
                tls_ca=tls_ca,
                output_format=output_format,
                check=False,
            )
            if last_result.returncode == 0:
                return last_result
            time.sleep(0.5)
        raise AssertionError(
            'YDB did not accept a query in {} seconds:\nstdout:\n{}\nstderr:\n{}'.format(
                READY_TIMEOUT,
                last_result.stdout if last_result else '',
                last_result.stderr if last_result else '',
            )
        )

    def log_offsets(self):
        result = {}
        for path in self._log_paths():
            if path.is_file():
                result[path] = path.stat().st_size
        return result

    def logs_since(self, offsets):
        chunks = []
        for path in self._log_paths():
            if not path.is_file():
                continue
            offset = offsets.get(path, 0)
            with path.open('rb') as stream:
                stream.seek(offset)
                chunks.append(stream.read().decode('utf-8', errors='replace'))
        return '\n'.join(chunks)

    def _log_paths(self):
        paths = set(self.working_directory.rglob('logfile_*'))
        if self.recipe_path.exists():
            for node in self.recipe()['nodes'].values():
                for key in ('stdout_file', 'stderr_file'):
                    if node.get(key):
                        paths.add(Path(node[key]))
        return sorted(paths)

    def close(self):
        pid = self.pid() if self.recipe_path.exists() else None
        try:
            if pid is not None:
                self._command('cleanup', check=False)
        finally:
            if pid is not None and _pid_is_alive(pid) and _is_expected_ydb_process(pid):
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            shutil.rmtree(self.working_directory, ignore_errors=True)


@pytest.fixture
def local_ydb(tmp_path):
    instance = LocalYdb(tmp_path / 'ydb')
    try:
        yield instance
    finally:
        instance.close()


@pytest.fixture(scope='module')
def generated_tls_bundle(tmp_path_factory):
    root = tmp_path_factory.mktemp('generated-tls')
    certificates = root / 'certificates'
    certificates.mkdir()
    instance = LocalYdb(
        root / 'ydb',
        _command_environment(
            YDB_GRPC_ENABLE_TLS='1',
            YDB_GRPC_TLS_DATA_PATH=str(certificates),
        ),
    )
    try:
        instance.deploy()
        result = instance.wait_for_query(
            'SELECT 1;',
            tls_ca=certificates / 'ca.pem',
            output_format='json-unicode',
        )
        assert _json_rows(result) == ['{"column0":1}']
        for filename in TLS_FILES:
            assert (certificates / filename).is_file()
        instance.cleanup()
        yield certificates
    finally:
        instance.close()


def test_deploy_stop_start_update_and_cleanup_preserve_data(local_ydb):
    local_ydb.deploy()
    local_ydb.wait_for_query(
        'CREATE TABLE acceptance ('
        'id Uint64, value Utf8, PRIMARY KEY (id));'
    )
    local_ydb.query(
        'UPSERT INTO acceptance (id, value) VALUES (1, "lifecycle-ok");'
    )
    result = local_ydb.query(
        'SELECT value FROM acceptance WHERE id = 1;',
        output_format='json-unicode',
    )
    assert _json_rows(result) == ['{"value":"lifecycle-ok"}']

    first_pid = local_ydb.pid()
    local_ydb.stop()
    _wait_for_pid_exit(first_pid)

    local_ydb.start()
    second_pid = local_ydb.pid()
    assert second_pid != first_pid
    result = local_ydb.wait_for_query(
        'SELECT value FROM acceptance WHERE id = 1;',
        output_format='json-unicode',
    )
    assert _json_rows(result) == ['{"value":"lifecycle-ok"}']

    local_ydb.update()
    third_pid = local_ydb.pid()
    assert third_pid != second_pid
    result = local_ydb.wait_for_query(
        'SELECT value FROM acceptance WHERE id = 1;',
        output_format='json-unicode',
    )
    assert _json_rows(result) == ['{"value":"lifecycle-ok"}']

    local_ydb.cleanup()
    assert not local_ydb.working_directory.exists()


def test_modified_config_is_applied_after_restart(local_ydb):
    local_ydb.deploy()
    local_ydb.stop()

    _set_default_log_level(local_ydb.config_path, 6)
    log_offsets = local_ydb.log_offsets()
    local_ydb.start()
    local_ydb.wait_for_query(
        'CREATE TABLE config_acceptance ('
        'id Uint64, PRIMARY KEY (id));'
    )
    local_ydb.query(
        'UPSERT INTO config_acceptance (id) VALUES (1), (2), (3);'
    )
    result = local_ydb.query(
        'SELECT id FROM config_acceptance ORDER BY id;',
        output_format='json-unicode',
    )
    assert _json_rows(result) == ['{"id":1}', '{"id":2}', '{"id":3}']
    local_ydb.stop()
    assert ' INFO:' in local_ydb.logs_since(log_offsets)


def test_generated_tls_bundle_is_reused_from_read_only_directory(
    tmp_path,
    generated_tls_bundle,
):
    certificates = tmp_path / 'certificates'
    shutil.copytree(generated_tls_bundle, certificates)
    expected_hash = _bundle_hash(certificates)
    for path in certificates.iterdir():
        path.chmod(0o444)
    certificates.chmod(0o555)

    instance = LocalYdb(
        tmp_path / 'ydb',
        _command_environment(
            YDB_GRPC_ENABLE_TLS='1',
            YDB_GRPC_TLS_DATA_PATH=str(certificates),
        ),
    )
    try:
        instance.deploy()
        result = instance.wait_for_query(
            'SELECT 1;',
            tls_ca=certificates / 'ca.pem',
            output_format='json-unicode',
        )
        assert _json_rows(result) == ['{"column0":1}']
        assert _bundle_hash(certificates) == expected_hash
    finally:
        instance.close()
        if certificates.exists():
            certificates.chmod(0o755)
            for path in certificates.iterdir():
                path.chmod(0o644)


def test_partial_tls_bundle_is_rejected(tmp_path, generated_tls_bundle):
    certificates = tmp_path / 'certificates'
    certificates.mkdir()
    shutil.copyfile(generated_tls_bundle / 'ca.pem', certificates / 'ca.pem')
    instance = LocalYdb(
        tmp_path / 'ydb',
        _command_environment(
            YDB_GRPC_ENABLE_TLS='1',
            YDB_GRPC_TLS_DATA_PATH=str(certificates),
        ),
    )
    try:
        result = instance.deploy(check=False)
        assert result.returncode != 0
        assert 'cert.pem' in result.stderr
        assert 'key.pem' in result.stderr
        assert not instance.recipe_path.exists()
    finally:
        instance.close()


def test_numeric_zero_disables_tls_and_does_not_generate_certificates(tmp_path):
    certificates = tmp_path / 'certificates'
    certificates.mkdir()
    instance = LocalYdb(
        tmp_path / 'ydb',
        _command_environment(
            YDB_GRPC_ENABLE_TLS='0',
            YDB_GRPC_TLS_DATA_PATH=str(certificates),
        ),
    )
    try:
        instance.deploy()
        result = instance.wait_for_query('SELECT 1;', output_format='json-unicode')
        assert _json_rows(result) == ['{"column0":1}']
        assert list(certificates.iterdir()) == []
    finally:
        instance.close()
