#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ipaddress
import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest
import requests
from hamcrest import has_length

from ydb.public.api.client.yc_private.resourcemanager.transitional import folder_service_pb2, folder_service_pb2_grpc
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.wait_for import wait_for_and_assert
from ydb.tests.library.sqs.cloud_test_base import YandexCloudSqsTestBase
from ydb.tests.library.sqs.requests_client import auth_headers, REQUEST_TIMEOUT
from ydb.tests.library.sqs.test_base import get_test_with_sqs_tenant_installation


class FolderServiceMock(folder_service_pb2_grpc.FolderServiceServicer):
    def List(self, request, context):
        response = folder_service_pb2.ListFoldersResponse()
        response.result.add(id=request.id, cloud_id=f'CLOUD_FOR_{request.id}')
        return response


class TestYmqActionAudit(get_test_with_sqs_tenant_installation(YandexCloudSqsTestBase)):
    erasure = Erasure.NONE
    token = 't1.audit-test.secret@builtin'
    sanitized_token = 't1.a****ltin (505EA662)'

    @classmethod
    def setup_class(cls):
        cls.folder_service_pool = ThreadPoolExecutor(max_workers=1)
        cls.folder_service = grpc.server(cls.folder_service_pool)
        folder_service_pb2_grpc.add_FolderServiceServicer_to_server(FolderServiceMock(), cls.folder_service)
        cls.folder_service_port = cls.folder_service.add_insecure_port('[::]:0')
        cls.folder_service.start()
        try:
            super().setup_class()
        except Exception:
            cls.teardown_class()
            raise

    @classmethod
    def teardown_class(cls):
        try:
            super().teardown_class()
        finally:
            cls.folder_service.stop(0).wait()
            cls.folder_service_pool.shutdown()

    @classmethod
    def _setup_config_generator(cls):
        config = super()._setup_config_generator()
        config.yaml_config['sqs_config']['cloud_events_config'] = {'enable_cloud_events': False}
        # Use the real ticket parser; only folder-to-cloud lookup is mocked.
        config.yaml_config['sqs_config']['yandex_cloud_folder_service_address'] = f'localhost:{cls.folder_service_port}'
        config.yaml_config.setdefault('auth_config', {})['path_to_root_ca'] = ''
        config.yaml_config['domains_config']['security_config']['enforce_user_token_check_requirement'] = True
        config.yaml_config['http_proxy_config'] = {
            'enabled': True,
            'ymq_enabled': True,
            'yandex_cloud_mode': True,
        }
        cls.audit_file = os.path.join(config.working_dir, 'ymq-action-audit.json')
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

    @pytest.mark.parametrize('protocol', ['xml', 'json'])
    @pytest.mark.parametrize('remote_address', [None, '192.0.2.17', '2001:db8::17'], ids=['socket', 'ipv4', 'ipv6'])
    def test_create_queue_audit_fields(self, protocol, remote_address):
        node = self.cluster_nodes[0]
        for queue_name, http_status, audit_status in [
            (self.queue_name, 200, 'SUCCESS'),
            (self.queue_name + '!', 400, 'ERROR'),
        ]:
            request_id = uuid.uuid4().hex
            headers = auth_headers(self.cloud_id, iam_token=self.token)
            headers['X-Request-Id'] = request_id
            if remote_address is not None:
                headers['X-Forwarded-For'] = f'{remote_address}, 198.51.100.7'
            if protocol == 'xml':
                response = requests.post(
                    f'http://localhost:{node.sqs_port}',
                    data={'Action': 'CreateQueue', 'QueueName': queue_name, 'folderId': self.folder_id},
                    headers=headers, timeout=REQUEST_TIMEOUT,
                )
            else:
                headers['X-Amz-Target'] = 'AmazonSQS.CreateQueue'
                response = requests.post(
                    f'http://localhost:{node.http_proxy_port}{self.database}',
                    params={'folderId': self.folder_id}, json={'QueueName': queue_name},
                    headers=headers, timeout=REQUEST_TIMEOUT,
                )
            assert response.status_code == http_status, response.text

            record = self._wait_for_audit_record(request_id)
            assert record['operation'] == 'create_queue', record
            assert record['status'] == audit_status, record
            assert record['sanitized_token'] == self.sanitized_token, record
            if remote_address is None:
                address = ipaddress.ip_address(record['remote_address'])
                if isinstance(address, ipaddress.IPv6Address):
                    address = address.ipv4_mapped or address
                assert address.is_loopback, record
            else:
                assert record['remote_address'] == remote_address, record
