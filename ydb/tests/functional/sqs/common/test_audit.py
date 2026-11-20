#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ipaddress
import json
import os
import uuid

import pytest
import requests
from hamcrest import has_length

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.wait_for import wait_for_and_assert
from ydb.tests.library.sqs.requests_client import auth_headers, REQUEST_TIMEOUT
from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, get_test_with_sqs_tenant_installation


class TestSqsActionAudit(get_test_with_sqs_tenant_installation(KikimrSqsTestBase)):
    erasure = Erasure.NONE
    token = 't1.audit-test.secret@builtin'
    sanitized_token = 't1.a****ltin (505EA662)'

    @classmethod
    def _setup_config_generator(cls):
        config = super()._setup_config_generator()
        config.yaml_config['sqs_config']['cloud_events_config'] = {'enable_cloud_events': False}
        # Allow anonymous requests, but require supplied tokens to pass authentication.
        config.yaml_config['sqs_config']['force_access_control'] = False
        config.yaml_config['domains_config']['security_config']['enforce_user_token_check_requirement'] = True
        cls.audit_file = os.path.join(config.working_dir, 'sqs-action-audit.json')
        with open(cls.audit_file, 'w'):
            pass
        config.yaml_config['audit_config'] = {
            'file_backend': {'file_path': cls.audit_file, 'format': 'JSON_LOG_COMPATIBLE'},
        }
        return config

    def _wait_for_audit_record(self, request_id):
        def read_records():
            records = []
            with open(self.audit_file) as audit_file:
                for line in audit_file:
                    if not line.endswith('\n'):
                        continue
                    record = json.loads(line)
                    if record.get('component') == 'ymq' and record.get('request_id', '').endswith('-' + request_id):
                        records.append(record)
            return records

        return wait_for_and_assert(read_records, has_length(1), step_seconds=0.1)[0]

    @pytest.mark.parametrize('security_token', [token, None], ids=['authenticated', 'anonymous'])
    @pytest.mark.parametrize('remote_address', [None, '192.0.2.17', '2001:db8::17'], ids=['socket', 'ipv4', 'ipv6'])
    def test_create_queue_audit_fields(self, security_token, remote_address):
        if security_token is not None:
            self._sqs_api.modify_permissions('Grant', security_token, self._username, ['CreateQueue'])

        for queue_name, http_status, audit_status in [
            (self.queue_name, 200, 'SUCCESS'),
            (self.queue_name + '!', 400, 'ERROR'),
        ]:
            request_id = uuid.uuid4().hex
            headers = auth_headers(self._username, security_token=security_token)
            headers['X-Request-Id'] = request_id
            if remote_address is not None:
                headers['X-Forwarded-For'] = f'{remote_address}, 198.51.100.7'
            response = requests.post(
                f'http://localhost:{self.sqs_port}',
                data={'Action': 'CreateQueue', 'QueueName': queue_name},
                headers=headers, timeout=REQUEST_TIMEOUT,
            )
            assert response.status_code == http_status, response.text

            record = self._wait_for_audit_record(request_id)
            assert record['operation'] == 'create_queue', record
            assert record['status'] == audit_status, record
            assert record['sanitized_token'] == (self.sanitized_token if security_token else '{none}'), record
            if remote_address is None:
                address = ipaddress.ip_address(record['remote_address'])
                if isinstance(address, ipaddress.IPv6Address):
                    address = address.ipv4_mapped or address
                assert address.is_loopback, record
            else:
                assert record['remote_address'] == remote_address, record
