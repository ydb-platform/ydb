#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import random
import uuid

import yatest

from ydb.tests.library.sqs.test_base import KikimrSqsTestBase


class YandexCloudSqsTestBase(KikimrSqsTestBase):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(YandexCloudSqsTestBase, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['yandex_cloud_mode'] = True
        config_generator.yaml_config['sqs_config']['enable_queue_master'] = True
        config_generator.yaml_config['sqs_config']['enable_dead_letter_queues'] = True
        config_generator.yaml_config['sqs_config']['account_settings_defaults'] = {'max_queues_count': 40}
        config_generator.yaml_config['sqs_config']['background_metrics_update_time_ms'] = 1000

        cls.event_output_file = yatest.common.output_path("events-%s.txt" % random.randint(1, 10000000))
        config_generator.yaml_config['sqs_config']['yc_search_events_config'] = {
            'enable_yc_search': True,
            'output_file_name': cls.event_output_file,
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

        logging.info(f'run test with cloud_id={self.cloud_id} folder_id={self.folder_id}')

    def _setup_user(self, _username, retries_count=3):
        pass  # account should be created automatically

    @classmethod
    def create_metauser(cls, http_port, config_generator):
        pass

    def teardown_method(self, method=None):
        self.check_all_users_queues_tables_consistency()
        super(YandexCloudSqsTestBase, self).teardown_method(method)
