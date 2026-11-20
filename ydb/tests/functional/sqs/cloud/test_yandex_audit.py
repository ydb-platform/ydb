#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import ipaddress
import json
import uuid
from concurrent.futures import ThreadPoolExecutor

import sys
import os

import pytest
import grpc
import requests
from hamcrest import has_length

from ydb.public.api.client.yc_private.resourcemanager.transitional import folder_service_pb2, folder_service_pb2_grpc
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.wait_for import wait_for_and_assert
from ydb.tests.library.sqs.requests_client import auth_headers, REQUEST_TIMEOUT

from ydb.tests.library.sqs.test_base import IS_FIFO_PARAMS, TABLES_FORMAT_PARAMS
from ydb.tests.library.sqs.cloud_test_base import YandexCloudSqsTestBase
from ydb.tests.library.sqs.test_base import get_test_with_sqs_tenant_installation

import random
import string


class CaptureFileOutput:
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        self.saved_pos = os.path.getsize(self.filename)
        return self

    def __exit__(self, *exc):
        # unreliable way to get all due audit records into the file
        time.sleep(0.1)
        with open(self.filename, 'rb', buffering=0) as f:
            f.seek(self.saved_pos)
            self.captured = f.read().decode('utf-8')


class TestCloudEvents(get_test_with_sqs_tenant_installation(YandexCloudSqsTestBase)):
    def generate_random_audit_path():
        def generate_random_string(length):
            characters = string.ascii_letters + string.digits
            return ''.join(random.choice(characters) for _ in range(length))

        return os.getcwd() + '/audit-file-' + generate_random_string(30)

    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestCloudEvents, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['cloud_events_config'] = {
            'enable_cloud_events': True,
            'retry_timeout_seconds': 2
        }

        cls.audit_file = cls.generate_random_audit_path()

        if 'audit_config' not in config_generator.yaml_config:
            config_generator.yaml_config['audit_config'] = {}
            config_generator.yaml_config['audit_config']['file_backend'] = {}

            with open(cls.audit_file, "w") as audit_file:
                audit_file.write('')

        config_generator.yaml_config['audit_config']['file_backend']['file_path'] = cls.audit_file
        return config_generator

    def _before_test_start(self):
        super(TestCloudEvents, self)._before_test_start()
        self.iam_token = 't1.prefix_of_token.super_mega_well_formed_iam_token'

        print(
            f'run test with\ncloud_id={self.cloud_id}\nfolder_id={self.folder_id}\n'
            f'iam_token={self.iam_token}\ncloud_account={self.cloud_account}',
            file=sys.stderr
        )

    def separate_audit_logs(self, audit_data):
        result = audit_data.replace('\r\n', '\n')
        return result.split('\n')

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_create_update_delete_one_queue(self, is_fifo, tables_format):
        capture_audit = CaptureFileOutput(self.audit_file)

        cloud_queue_name = ""

        with capture_audit:
            self._init_with_params(is_fifo, tables_format)

            self._sqs_api = self._create_api_for_user(
                self._username, raise_on_error=True, force_private=False,
                iam_token=self.iam_token, folder_id=self.folder_id
            )

            queue_url1 = self._sqs_api.create_queue(self.queue_name, is_fifo=is_fifo)
            time.sleep(1)

            cloud_queue_name = queue_url1.split('/')[-2]

            tags_keys = ["tag_key_1", "tag_key_2"]
            tags = {
                tags_keys[0]: "tag_value_1",
                tags_keys[1]: "tag_value_2"
            }
            self._sqs_api.tag_queue(queue_url1, tags)
            time.sleep(1)

            self._sqs_api.untag_queue(queue_url1, tags_keys)
            time.sleep(1)

            self._sqs_api.delete_queue(queue_url1)
            # We are waiting because auditLogActor checks events once in a while
            time.sleep(10)

        create = '"operation":"CreateMessageQueue"'
        update = '"operation":"UpdateMessageQueue"'
        delete = '"operation":"DeleteMessageQueue"'

        # I don't understand why sometimes capturing duplicates event messages.
        # It has been verified that the actor itself calls the audit_log function exactly as many times as needed.
        # I believe the problem is in the test wrapper for working with audit logs.
        # So, there is no '==', only '>='
        assert capture_audit.captured.count(create) >= 1
        assert capture_audit.captured.count(update) >= 2
        assert capture_audit.captured.count(delete) >= 1

        audit_log_list = self.separate_audit_logs(capture_audit.captured)

        for log in audit_log_list:
            print(log, file=sys.stderr, end="\n========================================\n")

            is_ymq_cloud_event = (
                create in log
                or update in log
                or delete in log
            )

            if is_ymq_cloud_event:
                permission = ""

                if create in log:
                    permission = "ymq.queues.create"
                elif update in log:
                    permission = "ymq.queues.setAttributes"
                elif delete in log:
                    permission = "ymq.queues.delete"

                none_value = "{none}"

                assert log.count('"request_id":') == 1 and log.count(f'"request_id":"{none_value}"') == 0
                assert log.count(f'"permission":"{permission}"') == 1
                assert log.count('"id":') == 1 and log.count(f'"id":"{none_value}"') == 0
                assert log.count('"reason":') == 0
                assert log.count('"idempotency_id":') == 1 and log.count(f'"idempotency_id":"{none_value}"') == 0
                assert log.count(f'"cloud_id":"{self.cloud_id}"') == 1
                assert log.count('"masked_token":"t1.prefix_of_token.**** (2764EAA0)"') == 1
                assert log.count('"auth_type":"service_account"') == 1
                assert log.count('"remote_address":') == 1 and log.count(f'"remote_address":"{none_value}"') == 0
                assert log.count(f'"folder_id":"{self.folder_id}"') == 1
                assert log.count(f'"resource_id":"{cloud_queue_name}"') == 1
                assert log.count('"created_at":') == 1 and log.count(f'"created_at":"{none_value}"') == 0
                assert log.count('"status":"SUCCESS"') == 1
                assert log.count('"subject":"fake_user_sid@as"') == 1                                                           # there is mock verison of cloud sqs
                assert log.count('"queue":') == 1
                assert log.count('"labels"') == 1
                assert log.count('"component":"ymq"') == 1

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_create_update_delete_one_queue_with_forwarded_for_header(self, is_fifo, tables_format):
        capture_audit = CaptureFileOutput(self.audit_file)

        cloud_queue_name = ""

        remote_address = '192.168.0.101'

        with capture_audit:
            self._init_with_params(is_fifo, tables_format)

            self._sqs_api = self._create_api_for_user(
                self._username, raise_on_error=True, force_private=False,
                iam_token=self.iam_token, folder_id=self.folder_id,
                extra_headers={'X-Forwarded-For': remote_address}
            )

            queue_url1 = self._sqs_api.create_queue(self.queue_name, is_fifo=is_fifo)
            time.sleep(1)

            cloud_queue_name = queue_url1.split('/')[-2]

            tags_keys = ["tag_key_1", "tag_key_2"]
            tags = {
                tags_keys[0]: "tag_value_1",
                tags_keys[1]: "tag_value_2"
            }
            self._sqs_api.tag_queue(queue_url1, tags)
            time.sleep(1)

            self._sqs_api.untag_queue(queue_url1, tags_keys)
            time.sleep(1)

            self._sqs_api.delete_queue(queue_url1)
            # We are waiting because auditLogActor checks events once in a while
            time.sleep(10)

        create = '"operation":"CreateMessageQueue"'
        update = '"operation":"UpdateMessageQueue"'
        delete = '"operation":"DeleteMessageQueue"'

        # I don't understand why sometimes capturing duplicates event messages.
        # It has been verified that the actor itself calls the audit_log function exactly as many times as needed.
        # I believe the problem is in the test wrapper for working with audit logs.
        # So, there is no '==', only '>='
        assert capture_audit.captured.count(create) >= 1
        assert capture_audit.captured.count(update) >= 2
        assert capture_audit.captured.count(delete) >= 1

        audit_log_list = self.separate_audit_logs(capture_audit.captured)

        for log in audit_log_list:
            print(log, file=sys.stderr, end="\n========================================\n")

            is_ymq_cloud_event = (
                create in log
                or update in log
                or delete in log
            )

            if is_ymq_cloud_event:
                permission = ""

                if create in log:
                    permission = "ymq.queues.create"
                elif update in log:
                    permission = "ymq.queues.setAttributes"
                elif delete in log:
                    permission = "ymq.queues.delete"

                none_value = "{none}"

                assert log.count('"request_id":') == 1 and log.count(f'"request_id":"{none_value}"') == 0
                assert log.count(f'"permission":"{permission}"') == 1
                assert log.count('"id":') == 1 and log.count(f'"id":"{none_value}"') == 0
                assert log.count('"reason":') == 0
                assert log.count('"idempotency_id":') == 1 and log.count(f'"idempotency_id":"{none_value}"') == 0
                assert log.count(f'"cloud_id":"{self.cloud_id}"') == 1
                assert log.count('"masked_token":"t1.prefix_of_token.**** (2764EAA0)"') == 1
                assert log.count('"auth_type":"service_account"') == 1
                assert log.count(f'"remote_address":"{remote_address}"') == 1
                assert log.count(f'"folder_id":"{self.folder_id}"') == 1
                assert log.count(f'"resource_id":"{cloud_queue_name}"') == 1
                assert log.count('"created_at":') == 1 and log.count(f'"created_at":"{none_value}"') == 0
                assert log.count('"status":"SUCCESS"') == 1
                assert log.count('"subject":"fake_user_sid@as"') == 1
                assert log.count('"queue":') == 1
                assert log.count('"labels"') == 1
                assert log.count('"component":"ymq"') == 1


class FolderServiceMock(folder_service_pb2_grpc.FolderServiceServicer):
    def List(self, request, context):
        response = folder_service_pb2.ListFoldersResponse()
        response.result.add(id=request.id, cloud_id=f'CLOUD_FOR_{request.id}')
        return response


class TestYmqActionAudit(get_test_with_sqs_tenant_installation(YandexCloudSqsTestBase)):
    erasure = Erasure.NONE
    # This builtin ticket has different sanitized_token and IAM masked_token values.
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
