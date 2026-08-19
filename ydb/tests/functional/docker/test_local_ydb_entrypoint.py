import gzip
import hashlib
import os
import shlex
import signal
import socket
import ssl
import subprocess
import time

import pytest
import yatest.common

from library.python.port_manager import PortManager
from ydb.tests.library.harness import tls_tools


TLS_FILE_NAMES = ('ca.pem', 'cert.pem', 'key.pem')
FIXED_MTIME_NS = 946684800 * 10**9


def _write_tls_data(directory):
    directory.mkdir(parents=True, exist_ok=True)
    cert, key = tls_tools.generate_selfsigned_cert('localhost')
    data = {
        'ca.pem': cert,
        'cert.pem': cert,
        'key.pem': key,
    }
    for filename, content in data.items():
        path = directory / filename
        path.write_bytes(content)
        os.utime(str(path), ns=(FIXED_MTIME_NS, FIXED_MTIME_NS))
    return data


def _tls_snapshot(directory):
    result = {}
    for filename in TLS_FILE_NAMES:
        path = directory / filename
        stat_result = path.stat()
        result[filename] = (
            hashlib.sha256(path.read_bytes()).hexdigest(),
            stat_result.st_ino,
            stat_result.st_mtime_ns,
        )
    return result


class LocalYdbEntrypoint(object):
    def __init__(self, root, port_manager):
        self.root = root
        self.working_dir = root / 'ydb_data'
        self.tls_dir = root / 'ydb_certs'
        self.preinit_dir = root / 'preinit.d'
        self.init_dir = root / 'init.d'
        self.log_path = root / 'initialize_local_ydb.log'
        for directory in (self.working_dir, self.tls_dir, self.preinit_dir, self.init_dir):
            directory.mkdir(parents=True, exist_ok=True)

        self.grpc_port = port_manager.get_port()
        self.grpc_tls_port = port_manager.get_port()
        self.mon_port = port_manager.get_port()
        self.ic_port = port_manager.get_port()
        self.public_http_port = port_manager.get_port()
        self.http_proxy_port = port_manager.get_port()

        self.ydb_cli = yatest.common.binary_path('ydb/apps/ydb/ydb')
        self.ydbd = yatest.common.binary_path(os.environ['YDB_DRIVER_BINARY'])
        self.local_ydb = yatest.common.binary_path('ydb/public/tools/local_ydb/local_ydb')
        self.entrypoint = yatest.common.source_path('.github/docker/files/initialize_local_ydb')
        self.health_check = yatest.common.source_path('.github/docker/files/health_check')

        self.env = os.environ.copy()
        for name in (
            'YDB_CONNECTION_STRING',
            'YDB_DATABASE',
            'YDB_ENDPOINT',
            'YDB_RECIPE_METAFILE',
            'YDB_SSL_ROOT_CERTIFICATES_FILE',
        ):
            self.env.pop(name, None)
        self.env.update(
            {
                'GRPC_PORT': str(self.grpc_port),
                'GRPC_TLS_PORT': str(self.grpc_tls_port),
                'HTTP_PROXY_PORT': str(self.http_proxy_port),
                'IC_PORT': str(self.ic_port),
                'MON_PORT': str(self.mon_port),
                'PUBLIC_HTTP_PORT': str(self.public_http_port),
                'YDB_CLI_BINARY': self.ydb_cli,
                'YDBD_BINARY': self.ydbd,
                'LOCAL_YDB_BINARY': self.local_ydb,
                'YDB_HEALTH_CHECK_BINARY': self.health_check,
                'YDB_GRPC_ENABLE_TLS': '1',
                'YDB_GRPC_TLS_DATA_PATH': str(self.tls_dir),
                'YDB_INITSCRIPTS_DIR': str(self.init_dir),
                'YDB_KAFKA_PROXY_PORT': '0',
                'YDB_PDISK_SIZE': '64MB',
                'YDB_PREINITSCRIPTS_DIR': str(self.preinit_dir),
                'YDB_TINY_MODE': 'true',
                'YDB_USE_IN_MEMORY_PDISKS': 'true',
                'YDB_WORKING_DIR': str(self.working_dir),
            }
        )

        self.process = None
        self._log_file = None

    def start(self):
        assert self.process is None
        self._log_file = open(str(self.log_path), 'ab', buffering=0)
        self.process = subprocess.Popen(
            ['bash', self.entrypoint],
            env=self.env,
            stdout=self._log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        return self

    def _logs(self):
        if self._log_file is not None:
            self._log_file.flush()
        if not self.log_path.exists():
            return ''
        return self.log_path.read_text(errors='replace')

    def wait_ready(self, timeout=45):
        command = [
            self.ydb_cli,
            '--endpoint',
            'grpc://localhost:{}'.format(self.grpc_port),
            '--database',
            '/local',
            '--no-discovery',
            'sql',
            '-s',
            'select 1',
        ]
        deadline = time.time() + timeout
        last_output = ''
        while time.time() < deadline:
            if self.process.poll() is not None:
                pytest.fail('entrypoint exited with code {}\n{}'.format(self.process.returncode, self._logs()))
            try:
                result = subprocess.run(
                    command,
                    env=self.env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=5,
                )
                last_output = result.stdout
                if result.returncode == 0:
                    return
            except subprocess.TimeoutExpired:
                last_output = 'YDB CLI readiness check timed out'
            time.sleep(0.5)
        pytest.fail('YDB did not become ready: {}\n{}'.format(last_output, self._logs()))

    def wait_tls(self, ca_path, cert_path, timeout=20):
        context = ssl.create_default_context(cafile=str(ca_path))
        expected_certificate = ssl.PEM_cert_to_DER_cert(cert_path.read_text())
        deadline = time.time() + timeout
        last_error = None
        while time.time() < deadline:
            try:
                with socket.create_connection(('localhost', self.grpc_tls_port), timeout=2) as raw_socket:
                    with context.wrap_socket(raw_socket, server_hostname='localhost') as tls_socket:
                        assert tls_socket.getpeercert(binary_form=True) == expected_certificate
                        return
            except (OSError, ssl.SSLError) as error:
                last_error = error
                time.sleep(0.25)
        pytest.fail('TLS endpoint did not present the supplied certificate: {}\n{}'.format(last_error, self._logs()))

    def wait_exit(self, timeout=45):
        try:
            return self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            pytest.fail('entrypoint did not exit\n{}'.format(self._logs()))

    def query(self, statement):
        return subprocess.run(
            [
                self.ydb_cli,
                '--endpoint',
                'grpc://localhost:{}'.format(self.grpc_port),
                '--database',
                '/local',
                '--no-discovery',
                'sql',
                '-s',
                statement,
            ],
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=10,
            check=True,
        ).stdout

    def stop(self):
        process = self.process
        self.process = None
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=5)

        subprocess.run(
            [
                self.local_ydb,
                'stop',
                '--ydb-working-dir',
                str(self.working_dir),
                '--ydb-binary-path',
                self.ydbd,
            ],
            env=self.env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
            check=False,
        )
        if self._log_file is not None:
            self._log_file.close()
            self._log_file = None


@pytest.fixture
def entrypoint(tmp_path):
    with PortManager() as port_manager:
        instance = LocalYdbEntrypoint(tmp_path, port_manager)
        try:
            yield instance
        finally:
            instance.stop()


def test_entrypoint_generates_tls_certificates_for_empty_directory(entrypoint):
    entrypoint.start()
    entrypoint.wait_ready()

    for filename in TLS_FILE_NAMES:
        assert (entrypoint.tls_dir / filename).stat().st_size > 0
    entrypoint.wait_tls(entrypoint.tls_dir / 'ca.pem', entrypoint.tls_dir / 'cert.pem')


def test_entrypoint_preserves_and_serves_pre_generated_tls_certificates(entrypoint):
    expected_data = _write_tls_data(entrypoint.tls_dir)
    before = _tls_snapshot(entrypoint.tls_dir)

    entrypoint.start()
    entrypoint.wait_ready()
    entrypoint.wait_tls(entrypoint.tls_dir / 'ca.pem', entrypoint.tls_dir / 'cert.pem')

    assert _tls_snapshot(entrypoint.tls_dir) == before
    for filename, content in expected_data.items():
        assert (entrypoint.tls_dir / filename).read_bytes() == content


def test_sourced_preinit_script_can_select_custom_tls_directory(entrypoint, tmp_path):
    source_tls_dir = tmp_path / 'source-certs'
    custom_tls_dir = tmp_path / 'mnt' / 'custom-certs'
    expected_data = _write_tls_data(source_tls_dir)
    script = entrypoint.preinit_dir / '01-provide-tls.sh'
    commands = [
        'export YDB_GRPC_TLS_DATA_PATH={}'.format(shlex.quote(str(custom_tls_dir))),
        'mkdir -p "$YDB_GRPC_TLS_DATA_PATH"',
    ]
    for filename in TLS_FILE_NAMES:
        commands.append(
            'cp {} "$YDB_GRPC_TLS_DATA_PATH/{}"'.format(
                shlex.quote(str(source_tls_dir / filename)),
                filename,
            )
        )
        commands.append('touch -t 200001010000 "$YDB_GRPC_TLS_DATA_PATH/{}"'.format(filename))
    script.write_text('\n'.join(commands) + '\n')

    entrypoint.start()
    entrypoint.wait_ready()
    entrypoint.wait_tls(custom_tls_dir / 'ca.pem', custom_tls_dir / 'cert.pem')

    assert not any((entrypoint.tls_dir / filename).exists() for filename in TLS_FILE_NAMES)
    for filename, content in expected_data.items():
        path = custom_tls_dir / filename
        assert path.read_bytes() == content
        assert path.stat().st_mtime < 978307200  # Before 2001: deploy did not rewrite the file.


def test_entrypoint_rejects_partial_tls_certificate_set(entrypoint):
    ca, _ = tls_tools.generate_selfsigned_cert('localhost')
    (entrypoint.tls_dir / 'ca.pem').write_bytes(ca)

    entrypoint.start()
    assert entrypoint.wait_exit() != 0
    assert 'cert.pem' in entrypoint._logs()
    assert 'key.pem' in entrypoint._logs()


def test_documented_init_scripts_run_once(entrypoint, tmp_path):
    marker = tmp_path / 'init-script-runs'
    entrypoint.env['INIT_MARKER'] = str(marker)
    (entrypoint.init_dir / '01-create-table.sql').write_text(
        'CREATE TABLE documented_init (id Uint64, value Utf8, PRIMARY KEY (id));\n'
    )
    with gzip.open(str(entrypoint.init_dir / '02-insert-row.sql.gz'), 'wt') as sql_file:
        sql_file.write('UPSERT INTO documented_init (id, value) VALUES (1u, "ready");\n')
    (entrypoint.init_dir / '03-record-run.sh').write_text('printf "run\\n" >> "$INIT_MARKER"\n')

    entrypoint.start()
    entrypoint.wait_ready()
    deadline = time.time() + 30
    while time.time() < deadline and not marker.exists():
        if entrypoint.process.poll() is not None:
            pytest.fail('entrypoint exited before init scripts completed\n{}'.format(entrypoint._logs()))
        time.sleep(0.25)
    assert marker.read_text().splitlines() == ['run']
    assert 'ready' in entrypoint.query('SELECT value FROM documented_init WHERE id = 1u;')

    entrypoint.stop()
    entrypoint.start()
    entrypoint.wait_ready()
    time.sleep(1)
    assert marker.read_text().splitlines() == ['run']


def test_failing_preinit_script_stops_entrypoint_before_deploy(entrypoint):
    (entrypoint.preinit_dir / '01-fail.sh').write_text('return 23\n')

    entrypoint.start()
    assert entrypoint.wait_exit(timeout=15) != 0
    assert not (entrypoint.working_dir / 'ydb_recipe.json').exists()
    assert 'Pre-init scripts failed' in entrypoint._logs()


def test_failing_init_script_stops_entrypoint(entrypoint):
    (entrypoint.init_dir / '01-fail.sh').write_text('return 23\n')

    entrypoint.start()
    assert entrypoint.wait_exit() != 0
    assert not (entrypoint.working_dir / '.user_scripts_initialized').exists()
    assert 'Init scripts failed' in entrypoint._logs()
