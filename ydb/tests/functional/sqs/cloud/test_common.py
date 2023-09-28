#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random
import logging
import time
import uuid

import pytest
import ydb.tests.library.common.yatest_common as yatest_common

from hamcrest import assert_that, equal_to, not_none, raises, greater_than
from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, IS_FIFO_PARAMS, TABLES_FORMAT_PARAMS
from ydb.tests.library.sqs.test_base import get_test_with_sqs_installation_by_path, get_test_with_sqs_tenant_installation


class CommonTests(KikimrSqsTestBase):
    def is_cloud_mode(self):
        return hasattr(self, 'cloud_account')

    def get_queue_info_from_db(self, queue_name):
        queues_table_path = f'{self.sqs_root}/.Queues'
        query = f'SELECT * FROM `{queues_table_path}` WHERE '
        if self.is_cloud_mode():
            query += f'Account="{self.cloud_id}" AND '
            query += f'CustomQueueName="{queue_name}"'
        else:
            query += f'Account="{self._username}" AND  QueueName="{queue_name}"'
        data_result_sets = self._execute_yql_query(query)
        assert_that(len(data_result_sets), equal_to(1))
        assert_that(len(data_result_sets[0].rows), equal_to(1))
        return data_result_sets[0].rows[0]

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_private_create_queue(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)
        name_suffix = '.fifo' if is_fifo else ''
        common_expected_attributes = {
            'DelaySeconds': '0',
            'MaximumMessageSize': '262144',
            'MessageRetentionPeriod': '345600',
            'ReceiveMessageWaitTimeSeconds': '0',
            'VisibilityTimeout': '30'
        }
        if is_fifo:
            common_expected_attributes['ContentBasedDeduplication'] = 'false'
            common_expected_attributes['FifoQueue'] = 'true'

        self.__next_queue_index = 0

        def create_with_params(private_api=False, created_timestamp_sec=None, custom_name=None, error=None):
            queue_name = f'queue_{self.__next_queue_index}_{self.queue_name}'
            self.__next_queue_index += 1

            def call_create():
                return self._sqs_api.create_queue(
                    queue_name, is_fifo=is_fifo,
                    private_api=private_api, created_timestamp_sec=created_timestamp_sec, custom_name=custom_name
                )
            if error:
                assert_that(call_create, raises(RuntimeError, pattern=error))
            else:
                queue_url = call_create()
                row = self.get_queue_info_from_db(custom_name if custom_name and self.is_cloud_mode() else queue_name)
                if created_timestamp_sec:
                    assert_that(row['CreatedTimestamp'], equal_to(created_timestamp_sec * 1000))
                expected_custom_name = custom_name if custom_name else (queue_name if self.is_cloud_mode() else '')
                assert_that(row['CustomQueueName'], equal_to(expected_custom_name))

                expected_attributes = common_expected_attributes.copy()
                if created_timestamp_sec is not None:
                    expected_attributes['CreatedTimestamp'] = str(created_timestamp_sec)
                last_error = None
                for i in range(15):
                    try:
                        attributes = self._sqs_api.get_queue_attributes(queue_url)
                        break
                    except RuntimeError as e:
                        last_error = e
                        if ("The specified queue doesn't exist" not in str(e)
                                and "<Code>ThrottlingException</Code>" not in str(e)):
                            raise e
                    time.sleep(1)
                else:
                    raise last_error
                for attr in expected_attributes:
                    assert_that(attributes[attr], equal_to(expected_attributes[attr]))

        create_with_params()
        create_with_params(error='The action or operation requested is invalid', created_timestamp_sec=123)
        create_with_params(error='The action or operation requested is invalid', custom_name='custom_name_1' + name_suffix)
        create_with_params(error='The action or operation requested is invalid', custom_name='')
        create_with_params(error='The action or operation requested is invalid', created_timestamp_sec=1234, custom_name='custom_name_2')

        create_with_params(private_api=True)
        create_with_params(private_api=True, created_timestamp_sec=12345)

        error = None if self.is_cloud_mode() else 'Custom queue name must be empty or unset'
        create_with_params(private_api=True, error=error, custom_name='custom_name_3' + name_suffix)
        create_with_params(private_api=True, error=error, created_timestamp_sec=123456, custom_name='custom_name_4' + name_suffix)

        create_with_params(private_api=True, error='Invalid created timestamp.', created_timestamp_sec=int(time.time()+60))
        create_with_params(
            private_api=True,
            error='Invalid custom queue name.' if self.is_cloud_mode() else 'Custom queue name must be empty or unset',
            custom_name='some incorrect name5' + name_suffix
        )

        if not is_fifo:
            create_with_params(
                private_api=True,
                error='Invalid custom queue name.' if self.is_cloud_mode() else None,
                custom_name=''
            )

        if is_fifo:
            create_with_params(error='The action or operation requested is invalid', custom_name='custom_name_6')
            create_with_params(error='The action or operation requested is invalid', created_timestamp_sec=1234567, custom_name='custom_name_7')
            create_with_params(private_api=True, error='Name of FIFO queue should end with', custom_name='custom_name_8')
            create_with_params(private_api=True, error='Name of FIFO queue should end with', created_timestamp_sec=12345678, custom_name='custom_name_9')

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_private_queue_recreation(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)
        queue_url = self._sqs_api.create_queue(self.queue_name, is_fifo=is_fifo)
        attributes = self._sqs_api.get_queue_attributes(queue_url)
        created_timestamp_sec = attributes["CreatedTimestamp"]

        row = self.get_queue_info_from_db(self.queue_name)
        queue_name = row['QueueName']
        custom_name = row["CustomQueueName"]
        version = row["Version"]

        delete_result = self._sqs_api.delete_queue(queue_url)
        assert_that(delete_result, not_none())

        new_queue_url = self._sqs_api.create_queue(
            queue_name, is_fifo=is_fifo,
            private_api=True, created_timestamp_sec=created_timestamp_sec, custom_name=custom_name
        )
        assert queue_url == new_queue_url
        row = self.get_queue_info_from_db(self.queue_name)
        assert_that(row["QueueName"], equal_to(queue_name))
        assert_that(row["CustomQueueName"], equal_to(custom_name))
        assert_that(row["Version"], greater_than(version))

        time.sleep(15)
        new_attributes = self._sqs_api.get_queue_attributes(queue_url)
        assert_that(attributes, equal_to(new_attributes))


class TestCommonSqsYandexCloudMode(get_test_with_sqs_tenant_installation(CommonTests)):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestCommonSqsYandexCloudMode, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['yandex_cloud_mode'] = True
        config_generator.yaml_config['sqs_config']['enable_queue_master'] = True
        config_generator.yaml_config['sqs_config']['enable_dead_letter_queues'] = True
        config_generator.yaml_config['sqs_config']['account_settings_defaults'] = {'max_queues_count': 40}
        config_generator.yaml_config['sqs_config']['background_metrics_update_time_ms'] = 1000

        cls.event_output_file = yatest_common.output_path("events-%s.txt" % random.randint(1, 10000000))
        config_generator.yaml_config['sqs_config']['yc_search_events_config'] = {
            'enable_yc_search': True,
            'output_file_name': cls.event_output_file,
        }
        temp_token_file = yatest_common.work_path("tokenfile")
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

        self._sqs_api = self._create_api_for_user('ignored', raise_on_error=True, force_private=False, iam_token=self.iam_token, folder_id=self.folder_id)

        logging.info(f'run test with cloud_id={self.cloud_id} folder_id={self.folder_id}')

    def _setup_user(self, _username, retries_count=3):
        pass  # account should be created automatically

    @classmethod
    def create_metauser(cls, cluster, config_generator):
        pass

    def teardown_method(self, method=None):
        self.check_all_users_queues_tables_consistency()
        super(TestCommonSqsYandexCloudMode, self).teardown_method(method)


class CommonYandexTest(CommonTests):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(CommonYandexTest, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['account_settings_defaults'] = {'max_queues_count': 10}
        return config_generator


class TestCommonYandexWithTenant(get_test_with_sqs_tenant_installation(CommonYandexTest)):
    pass


class TestCommonYandexWithPath(get_test_with_sqs_installation_by_path(CommonYandexTest)):
    pass
