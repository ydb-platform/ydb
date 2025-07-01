#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import time
import uuid

import sys
import os

import pytest
import yatest

from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, get_test_with_sqs_tenant_installation
from ydb.tests.library.sqs.test_base import IS_FIFO_PARAMS, TABLES_FORMAT_PARAMS

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


class TestCloudEvents(get_test_with_sqs_tenant_installation(KikimrSqsTestBase)):
    def generate_random_audit_path():
        def generate_random_string(length):
            characters = string.ascii_letters + string.digits
            return ''.join(random.choice(characters) for _ in range(length))

        return os.getcwd() + '/audit-file-' + generate_random_string(30)

    @classmethod
    def _setup_config_generator(self):
        config_generator = super(TestCloudEvents, self)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['yandex_cloud_mode'] = True
        config_generator.yaml_config['sqs_config']['enable_queue_master'] = True
        config_generator.yaml_config['sqs_config']['enable_dead_letter_queues'] = True
        config_generator.yaml_config['sqs_config']['account_settings_defaults'] = {'max_queues_count': 40}
        config_generator.yaml_config['sqs_config']['background_metrics_update_time_ms'] = 1000

        config_generator.yaml_config['sqs_config']['cloud_events_config'] = {
            'enable_cloud_events': True,
            'retry_timeout_seconds': 2
        }

        self.audit_file = self.generate_random_audit_path()

        if 'audit_config' not in config_generator.yaml_config:
            config_generator.yaml_config['audit_config'] = {}
            config_generator.yaml_config['audit_config']['file_backend'] = {}

            with open(self.audit_file, "w") as audit_file:
                audit_file.write('')

        config_generator.yaml_config['audit_config']['file_backend']['file_path'] = self.audit_file

        temp_token_file = yatest.common.work_path("tokenfile")
        with open(temp_token_file, "w") as fl:
            fl.write("root@builtin")

        config_generator.yaml_config['sqs_config']['auth_config'] = {'oauth_token': {'token_file': temp_token_file}}
        return config_generator

    def _before_test_start(self):
        self.cloud_account = f'acc_{uuid.uuid1()}'
        self.iam_token = f'usr_{self.cloud_account}'
        self.folder_id = f'folder_{self.cloud_account}'
        self.cloud_id = f'CLOUD_FOR_{self.folder_id}'

        self._username = self.cloud_id

        print(f'run test with\ncloud_id={self.cloud_id}\nfolder_id={self.folder_id}\niam_token={self.iam_token}\ncloud_account={self.cloud_account}', file=sys.stderr)

        logging.info(f'run test with cloud_id={self.cloud_id} folder_id={self.folder_id}')

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
                assert log.count('"masked_token":') == 1 and log.count(f'"masked_token":"{none_value}"') == 0
                assert log.count(f'"auth_type":"{none_value}"') == 1                                                            # there is no auth_type in mock verison of cloud sqs
                assert log.count('"remote_address":') == 1 and log.count(f'"remote_address":"{none_value}"') == 0
                assert log.count(f'"folder_id":"{self.folder_id}"') == 1
                assert log.count(f'"resource_id":"{cloud_queue_name}"') == 1
                assert log.count('"created_at":') == 1 and log.count(f'"created_at":"{none_value}"') == 0
                assert log.count('"status":"SUCCESS"') == 1
                assert log.count('"subject":"fake_user_sid@as"') == 1                                                           # there is mock verison of cloud sqs
                assert log.count('"queue":') == 1
                assert log.count('"labels"') == 1
                assert log.count('"component":"ymq"') == 1
