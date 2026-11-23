import json
import time
import unittest

import requests

from ydb.tests.library.common.helpers import plain_or_under_sanitizer
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR


class TestMonitoringAudit(unittest.TestCase):
    def _start_cluster(self, **kwargs):
        config = KikimrConfigGenerator(
            nodes=1,
            erasure=Erasure.NONE,
            enforce_user_token_requirement=True,
            default_clusteradmin='root@builtin',
            default_users={'root': ''},
            enable_audit_log=True,
            audit_log_config={
                'file_backend': {'format': 'JSON_LOG_COMPATIBLE'},
                'log_class_config': [{
                    'log_class': 'ClusterAdmin',
                    'enable_logging': True,
                    'log_phase': ['Received', 'Completed'],
                }],
            },
            **kwargs,
        )
        config.yaml_config['domains_config']['security_config']['monitoring_allowed_sids'] = ['root', 'root@builtin']
        self.cluster = KiKiMR(config)
        self.addCleanup(self.cluster.stop)
        self.cluster.start()
        node = self.cluster.nodes[1]
        self.base_url = f'http://{node.host}:{node.mon_port}'

    def _wait_for_records(self, path):
        deadline = time.monotonic() + plain_or_under_sanitizer(30, 120)
        records = []
        while time.monotonic() < deadline:
            records = []
            with open(self.cluster.config.audit_file_path) as audit_file:
                for line in audit_file:
                    if not line.endswith('\n'):
                        continue
                    # Preserve pairs so duplicate identity keys cannot pass unnoticed.
                    pairs = json.loads(line, object_pairs_hook=list)
                    record = dict(pairs)
                    if record.get('component') == 'monitoring' and record.get('url') == path:
                        records.append(pairs)
            if len(records) >= 2:
                return records
            time.sleep(0.1)
        self.fail(f'Expected both monitoring audit phases for {path}, got {records}')

    def _assert_request(self, path, status, subject, sanitized_token, headers=None, cookies=None):
        response = requests.get(
            self.base_url + path,
            headers=headers,
            cookies=cookies,
            allow_redirects=False,
            timeout=30,
        )
        self.assertEqual(response.status_code, status, response.text)
        records = self._wait_for_records(path)
        self.assertEqual(len(records), 2, records)
        expected_statuses = ['IN-PROCESS', 'SUCCESS' if status == 200 else 'ERROR']
        for pairs, expected_status in zip(records, expected_statuses):
            record = dict(pairs)
            self.assertEqual([value for key, value in pairs if key == 'subject'], [subject], record)
            self.assertEqual([value for key, value in pairs if key == 'sanitized_token'], [sanitized_token], record)
            self.assertEqual(record['status'], expected_status, record)
            self.assertEqual(record['operation'], 'HTTP REQUEST', record)
            for token in list((headers or {}).values()) + list((cookies or {}).values()):
                self.assertNotIn(token, json.dumps(record))
        self.assertEqual(dict(records[0])['reason'], 'Execute')
        self.assertTrue(dict(records[1])['reason'].startswith(str(status)), records)

    def test_authenticated_request(self):
        self._start_cluster()
        response = requests.post(
            self.base_url + '/login',
            json={'user': 'root', 'password': '', 'database': '/Root'},
            timeout=30,
        )
        self.assertEqual(response.status_code, 200, response.text)
        token = response.cookies['ydb_session_id']
        self._assert_request(
            '/trace', 200, 'root', token.rsplit('.', 1)[0] + '.**',
            cookies={'ydb_session_id': token},
        )

    def test_missing_credentials(self):
        self._start_cluster()
        self._assert_request('/trace', 401, '{none}', '{none}')

    def test_invalid_credentials(self):
        self._start_cluster()
        self._assert_request(
            '/trace', 403, '{none}', '{none}',
            headers={'Authorization': 'Bearer invalid-monitoring-token'},
        )

    def test_disabled_authorization(self):
        self._start_cluster()
        # /login bypasses authorization; GET is rejected by the handler itself.
        self._assert_request(
            '/login', 400, '{none}', '{none}',
            headers={'Authorization': 'Bearer ignored-monitoring-token'},
        )

    def test_default_sid_without_client_token(self):
        self._start_cluster(default_user_sid='root@builtin')
        self._assert_request('/trace', 200, 'root@builtin', '{none}')
