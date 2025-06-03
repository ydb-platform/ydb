#!/usr/bin/env python
# -*- coding: utf-8 -*-

import boto3
import botocore

import random
import logging
import requests
import time
import uuid

import sys

import pytest
import yatest

import ydb
from hamcrest import assert_that, equal_to, not_none, has_item, has_items, is_not, contains_string
from hamcrest import raises, greater_than, not_, less_than
from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, get_test_with_sqs_tenant_installation
from ydb.tests.library.sqs.test_base import IS_FIFO_PARAMS, TABLES_FORMAT_PARAMS

# Id,
#     Name,
#     Type,
#     CreatedAt,
#     CloudId,
#     FolderId,
#     UserSID,
#     UserSanitizedToken,
#     AuthType,
#     PeerName,
#     RequestId,
#     IdempotencyId,
#     Labels

# VALUES
#     (
#     1001,
#     "test_name_1",
#     "create",
#     1696506100,
#     "cloud123",
#     "folderABC",
#     "userSID_1",
#     "sanitizedToken_1",
#     "OAuth",
#     "peer_name_1",
#     "req_id_1",
#     "idemp_1",
#     "label_1"
#     ),

class TestCloudEvents(get_test_with_sqs_tenant_installation(KikimrSqsTestBase)):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestCloudEvents, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['yandex_cloud_mode'] = True
        config_generator.yaml_config['sqs_config']['enable_queue_master'] = True
        config_generator.yaml_config['sqs_config']['enable_dead_letter_queues'] = True
        config_generator.yaml_config['sqs_config']['account_settings_defaults'] = {'max_queues_count': 40}
        config_generator.yaml_config['sqs_config']['background_metrics_update_time_ms'] = 1000

        config_generator.yaml_config['sqs_config']['cloud_events_config'] = {
            'enable_cloud_events': True,
            'retry_timeout_seconds': 2
        }
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

    # @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    # @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_create_update_delete_one_queue(self):
        self._init_with_params()
        # self._init_with_params(tables_format=tables_format)

        self._sqs_api = self._create_api_for_user(
            self._username, raise_on_error=True, force_private=False,
            iam_token=self.iam_token, folder_id=self.folder_id
        )

        queue_url1 = self._sqs_api.create_queue(self.queue_name, is_fifo=False)
        time.sleep(1)

        tags_keys = [ "tag_key_1", "tag_key_2" ]
        tags = {
            tags_keys[0]: "tag_value_1",
            tags_keys[1]: "tag_value_2"
        }
        self._sqs_api.tag_queue(queue_url1, tags)
        time.sleep(1)

        self._sqs_api.untag_queue(queue_url1, tags_keys)
        time.sleep(1)

        self._sqs_api.delete_queue(queue_url1)
        time.sleep(10)
