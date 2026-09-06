#!/usr/bin/env python3
"""Run the packaged Bash probes with controlled CLI failures in disposable Linux.

Only /ydb is replaced: timeout, flock, procfs, the scripts and Docker restart are real.
Run: python3 .github/docker/tests/test_healthcheck.py
"""
import os
from pathlib import Path
import subprocess
import time
import unittest
import uuid


ROOT = Path(__file__).resolve().parent.parent
MOCK_CLI = r'''#!/usr/bin/env bash
set -eu
printf '%s\n' "$*" >>/tmp/fixture/arguments
shift 5
op="$1"
case "$op" in
  sql)
    case "$3" in
      'select 1') op=select ;;
      'create table'*) op=create ;;
      'drop table'*) op=drop ;;
      *) exit 64 ;;
    esac ;;
  scheme) test "$2" = ls ;;
  discovery) test "$2" = whoami; op=liveness ;;
  *) exit 64 ;;
esac
echo "$op" >>/tmp/fixture/calls
if [ -f /tmp/fixture/hang ]; then
  echo "$$" >/tmp/fixture/child_pid
  touch /tmp/fixture/entered
  trap '' TERM
  sleep 30
fi
if [ -f /tmp/fixture/fail_once ]; then
  rm /tmp/fixture/fail_once
  exit 1
fi
if [ -f /tmp/fixture/fail ]; then
  read -r fail </tmp/fixture/fail
  if [ "$fail" = "$op" ] || [ "$fail" = all ]; then exit 1; fi
fi
'''


class HealthcheckTest(unittest.TestCase):
    read_only = False

    @classmethod
    def cleanup_container(cls):
        subprocess.run(['docker', 'rm', '-f', cls.name], check=False, capture_output=True)
        if cls.read_only:
            subprocess.run(['docker', 'image', 'rm', cls.name], check=False, capture_output=True)
            subprocess.run(['docker', 'volume', 'rm', cls.name, cls.name + '-frozen'],
                           check=False, capture_output=True)

    @classmethod
    def setUpClass(cls):
        cls.name = 'healthcheck-test-' + uuid.uuid4().hex[:12]
        subprocess.run(['docker', 'run', '-d', '--platform', 'linux/amd64', '--name', cls.name, '--network', 'none',
                        os.environ.get('HEALTHCHECK_TEST_IMAGE', 'ubuntu:22.04'),
                        'sleep', 'infinity'], check=True, stdout=subprocess.DEVNULL)
        cls.addClassCleanup(cls.cleanup_container)
        for name in ('health_check', 'health_readiness', 'health_liveness', 'health_common'):
            path = ROOT / 'files' / name
            if path.exists():
                subprocess.run(['docker', 'cp', str(path), cls.name + ':/' + name], check=True)
        cls.shell('chmod +x /health_check /health_readiness /health_liveness')
        cls.shell('cat >/ydb; chmod +x /ydb', input=MOCK_CLI)
        if cls.read_only:
            # Bake the probes into the image: docker cp cannot install them into
            # an already running read-only container. Keep only test fixtures on
            # a writable volume so injected failures survive docker restart.
            subprocess.run(['docker', 'commit', cls.name, cls.name], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(['docker', 'rm', '-f', cls.name], check=True, stdout=subprocess.DEVNULL)
            subprocess.run(['docker', 'run', '-d', '--platform', 'linux/amd64', '--name', cls.name,
                            '--network', 'none', '--read-only', '--volume', cls.name + ':/tmp/fixture',
                            '--volume', cls.name + '-frozen:/frozen:ro',
                            cls.name, 'sleep', 'infinity'], check=True, stdout=subprocess.DEVNULL)

    @classmethod
    def shell(cls, command, **kwargs):
        return subprocess.run(['docker', 'exec', '-i', cls.name, 'bash', '-c', command],
                              text=True, capture_output=True, check=True, **kwargs)

    def setUp(self):
        self.shell('rm -rf /dev/shm/ydb_health; mkdir -p /tmp/fixture; rm -rf /tmp/fixture/*')

    def probe(self, name='health_readiness', **env):
        command = ['docker', 'exec']
        for key, value in {'YDB_READINESS_SLEEP': '0', **env}.items():
            command += ['-e', key + '=' + str(value)]
        return subprocess.run(command + [self.name, '/' + name], text=True,
                              capture_output=True, timeout=15)

    def assert_ok(self, result):
        self.assertEqual(result.returncode, 0, result.stderr)

    def calls(self):
        return self.shell('cat /tmp/fixture/calls 2>/dev/null || true').stdout.splitlines()

    def test_readiness_executes_cli_and_all_operations(self):
        self.assert_ok(self.probe())
        self.assertEqual(self.calls(), ['select', 'scheme', 'create', 'drop'])

    def test_each_readiness_error_is_propagated(self):
        for operation in ('select', 'scheme', 'create', 'drop'):
            for ddl in ('true', 'false') if operation in ('select', 'scheme') else ('true',):
                with self.subTest(operation=operation, ddl=ddl):
                    self.shell('echo ' + operation + ' >/tmp/fixture/fail')
                    self.assertNotEqual(self.probe(YDB_READINESS_ENABLE_DDL=ddl).returncode, 0)

    def test_readiness_without_ddl_still_requires_sql_and_scheme(self):
        self.assert_ok(self.probe(YDB_READINESS_ENABLE_DDL='false'))
        self.assertEqual(self.calls(), ['select', 'scheme'])

    def test_retry_repeats_the_failed_attempt(self):
        self.shell('touch /tmp/fixture/fail_once')
        self.assert_ok(self.probe())
        self.assertEqual(self.calls(), ['select', 'select', 'scheme', 'create', 'drop'])

    def test_custom_endpoint_and_database_are_preserved(self):
        self.assert_ok(self.probe(YDB_ENDPOINT='grpcs://test.example:1234', YDB_DATABASE='/other/db'))
        args = self.shell('cat /tmp/fixture/arguments').stdout
        self.assertIn('--endpoint grpcs://test.example:1234 --database /other/db --no-discovery', args)
        self.assertIn('`/other/db/.sys_health/test`', args)

    def test_liveness_uses_one_small_rpc_and_propagates_failure(self):
        self.assert_ok(self.probe('health_liveness'))
        self.assertEqual(self.calls(), ['liveness'])
        self.shell('echo liveness >/tmp/fixture/fail')
        self.assertNotEqual(self.probe('health_liveness').returncode, 0)

    def test_cached_health_avoids_sql_and_liveness_failure_invalidates_it(self):
        self.assert_ok(self.probe('health_check'))
        self.shell(': >/tmp/fixture/calls')
        self.assert_ok(self.probe('health_check'))
        self.assertEqual(self.calls(), ['liveness'])
        self.shell('echo liveness >/tmp/fixture/fail')
        self.assertNotEqual(self.probe('health_check').returncode, 0)
        self.shell('echo select >/tmp/fixture/fail')
        self.assertNotEqual(self.probe('health_check').returncode, 0)

    def test_expired_cache_does_not_hide_readiness_failures(self):
        self.assert_ok(self.probe('health_check', YDB_READINESS_INTERVAL_SECONDS=1))
        time.sleep(1.1)
        self.shell('echo select >/tmp/fixture/fail')
        for _ in range(3):
            self.assertNotEqual(self.probe('health_check', YDB_READINESS_INTERVAL_SECONDS=1).returncode, 0)

    def test_changed_probe_settings_do_not_reuse_readiness(self):
        for settings in ({'YDB_DATABASE': '/new'}, {'YDB_ENDPOINT': 'grpc://other:1234'},
                         {'YDB_READINESS_ENABLE_DDL': 'false'}):
            with self.subTest(settings=settings):
                self.assert_ok(self.probe('health_check'))
                self.shell('echo select >/tmp/fixture/fail')
                self.assertNotEqual(self.probe('health_check', **settings).returncode, 0)
                self.shell('rm /tmp/fixture/fail')

    def test_corrupt_or_future_timestamp_requires_readiness(self):
        for timestamp in ('garbage', '9999999999'):
            with self.subTest(timestamp=timestamp):
                self.assert_ok(self.probe())
                self.shell("sed -i '1c\\" + timestamp + "' /dev/shm/ydb_health/last_readiness_ok; "
                           'echo select >/tmp/fixture/fail')
                self.assertNotEqual(self.probe('health_check').returncode, 0)
                self.shell('rm /tmp/fixture/fail')

    def test_failure_to_store_success_does_not_report_healthy(self):
        self.shell('mkdir -p /dev/shm/ydb_health/last_readiness_ok.new')
        self.assertNotEqual(self.probe().returncode, 0)
        self.shell('rmdir /dev/shm/ydb_health/last_readiness_ok.new; echo select >/tmp/fixture/fail')
        self.assertNotEqual(self.probe('health_check').returncode, 0)

    def test_zero_deadline_is_rejected(self):
        self.assertNotEqual(self.probe(YDB_READINESS_TIMEOUT='0s').returncode, 0)
        self.assertNotEqual(self.probe('health_liveness', YDB_LIVENESS_TIMEOUT='0s').returncode, 0)
        self.assertEqual(self.calls(), [])

    def test_retry_sleep_is_inside_the_total_deadline(self):
        self.shell('echo select >/tmp/fixture/fail')
        start = time.monotonic()
        self.assertNotEqual(self.probe(YDB_READINESS_TIMEOUT='1s', YDB_READINESS_SLEEP=30).returncode, 0)
        self.assertLess(time.monotonic() - start, 5)

    def test_container_restart_requires_new_readiness(self):
        self.assert_ok(self.probe('health_check'))
        self.shell('echo select >/tmp/fixture/fail')
        subprocess.run(['docker', 'restart', '--timeout', '1', self.name], check=True,
                       stdout=subprocess.DEVNULL)
        self.assertNotEqual(self.probe('health_check').returncode, 0)

    def test_deadline_kills_children_and_releases_readiness_lock(self):
        self.shell('touch /tmp/fixture/hang')
        start = time.monotonic()
        self.assertNotEqual(self.probe(YDB_READINESS_TIMEOUT='1s').returncode, 0)
        self.assertLess(time.monotonic() - start, 5)
        self.shell('test -f /tmp/fixture/child_pid')
        state = self.shell('if [ -f /tmp/fixture/child_pid ]; then read p </tmp/fixture/child_pid; '
                           'if [ -r /proc/$p/stat ]; then cat /proc/$p/stat; fi; fi').stdout
        if state:
            self.assertEqual(state.split(') ')[1].split()[0], 'Z', state)
        self.shell('rm /tmp/fixture/hang')
        self.assert_ok(self.probe())

    def test_busy_readiness_never_returns_cached_success(self):
        self.assert_ok(self.probe('health_check'))
        self.shell('touch /tmp/fixture/hang')
        with subprocess.Popen(['docker', 'exec', '-e', 'YDB_READINESS_TIMEOUT=3s',
                               self.name, '/health_readiness'], stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True) as pending:
            for _ in range(30):
                if self.shell('test -f /tmp/fixture/entered && echo entered || true').stdout:
                    break
                time.sleep(0.05)
            else:
                self.fail('readiness did not start the controlled RPC')
            self.assertNotEqual(self.probe('health_check').returncode, 0)
            self.assertNotEqual(pending.wait(timeout=6), 0)
        self.shell('rm /tmp/fixture/hang')
        self.assert_ok(self.probe())


class ReadOnlyHealthcheckTest(HealthcheckTest):
    read_only = True

    def test_custom_state_directory_keeps_cache_and_failure_invalidation(self):
        settings = {'YDB_HEALTH_STATE_DIR': '/tmp/fixture/custom-health'}
        self.assert_ok(self.probe('health_check', **settings))
        self.shell('test -s /tmp/fixture/custom-health/last_readiness_ok; : >/tmp/fixture/calls')
        self.assert_ok(self.probe('health_check', **settings))
        self.assertEqual(self.calls(), ['liveness'])
        self.shell('echo liveness >/tmp/fixture/fail')
        self.assertNotEqual(self.probe('health_check', **settings).returncode, 0)
        self.shell('echo select >/tmp/fixture/fail')
        self.assertNotEqual(self.probe('health_check', **settings).returncode, 0)

    def test_read_only_state_directory_fails_instead_of_skipping_readiness(self):
        for path in ('/tmp/ydb_health', '/etc'):
            with self.subTest(path=path):
                self.assertNotEqual(self.probe('health_check', YDB_HEALTH_STATE_DIR=path).returncode, 0)
        self.assertEqual(self.calls(), [])

    def test_persistent_custom_cache_requires_readiness_after_restart(self):
        settings = {'YDB_HEALTH_STATE_DIR': '/tmp/fixture/custom-health'}
        self.assert_ok(self.probe('health_check', **settings))
        self.shell('echo select >/tmp/fixture/fail')
        subprocess.run(['docker', 'restart', '--timeout', '1', self.name], check=True,
                       stdout=subprocess.DEVNULL)
        self.assertNotEqual(self.probe('health_check', **settings).returncode, 0)

    def test_read_only_cache_cannot_be_trusted_even_when_fresh(self):
        # Seed a valid record via another container, then expose it read-only.
        # A liveness failure could not invalidate this record, so it must never
        # allow the target container to bypass readiness.
        cached = self.shell('source /health_common; health_uptime; health_context').stdout
        subprocess.run(['docker', 'run', '--rm', '-i', '--platform', 'linux/amd64', '--network', 'none',
                        '--volume', self.name + '-frozen:/state', self.name,
                        'bash', '-c', 'cat >/state/last_readiness_ok'],
                       input=cached, text=True, check=True)
        self.assertNotEqual(self.probe('health_check', YDB_HEALTH_STATE_DIR='/frozen').returncode, 0)
        self.assertEqual(self.calls(), [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
