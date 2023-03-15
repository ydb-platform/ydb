#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time

import pytest
from hamcrest import assert_that, raises, greater_than, contains_string, equal_to, instance_of

from ydb.tests.library.sqs.requests_client import SqsSendMessageParams

from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, IS_FIFO_PARAMS, TABLES_FORMAT_PARAMS

from ydb.tests.oss.ydb_sdk_import import ydb


class TestSqsQuotingWithKesus(KikimrSqsTestBase):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestSqsQuotingWithKesus, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['quoting_config'] = {
            'enable_quoting': True,
            'kesus_quoter_config': {'default_limits': {'std_send_message_rate': 1000}}
        }
        return config_generator

    def test_creates_quoter(self):
        quoter_description = self._driver.scheme_client.describe_path('{}/{}/.Quoter'.format(self.sqs_root, self._username))
        assert_that(quoter_description.is_coordination_node())

        # Check that user is properly deleted
        self._sqs_api.delete_user(self._username)

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_properly_creates_and_deletes_queue(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)
        created_queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)
        self._sqs_api.delete_queue(created_queue_url)

        quoter_description = self._driver.scheme_client.describe_path('{}/{}/.Quoter'.format(self.sqs_root, self._username))
        assert_that(quoter_description.is_coordination_node())

        def describe_queue_path():
            self._driver.scheme_client.describe_path('{}/{}/{}'.format(self.sqs_root, self._username, self.queue_name))

        assert_that(
            describe_queue_path,
            raises(
                ydb.issues.SchemeError
            )
        )


class TestSqsQuotingWithLocalRateLimiter(KikimrSqsTestBase):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestSqsQuotingWithLocalRateLimiter, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['quoting_config'] = {
            'enable_quoting': True,
            'quota_deadline_ms': 150,
            'local_rate_limiter_config': {
                'rates': {
                    'std_send_message_rate': 10,
                    'std_receive_message_rate': 10,
                    'std_delete_message_rate': 10,
                    'std_change_message_visibility_rate': 10,
                    'fifo_send_message_rate': 10,
                    'fifo_receive_message_rate': 10,
                    'fifo_delete_message_rate': 10,
                    'fifo_change_message_visibility_rate': 10,
                    'create_objects_rate': 2,
                    'delete_objects_rate': 2,
                    'other_requests_rate': 5,
                }
            }
        }
        return config_generator

    def test_does_not_create_kesus(self):
        def call_describe():
            self._driver.scheme_client.describe_path('{}/{}/.Quoter'.format(self.sqs_root, self._username))

        assert_that(
            call_describe,
            raises(
                ydb.issues.SchemeError
            )
        )

        # Check that user is properly deleted
        self._sqs_api.delete_user(self._username)

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_does_actions_with_queue(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)
        self._create_queue_send_x_messages_read_y_messages(self.queue_name,
                                                           send_count=1,
                                                           read_count=1,
                                                           msg_body_template=self._msg_body_template,
                                                           visibility_timeout=1000,
                                                           is_fifo=is_fifo)
        self._sqs_api.get_queue_attributes(self.queue_url)
        self._sqs_api.delete_queue(self.queue_url)

    def call_n_times_except_throttling(self, count, func):
        throttling_times = 0
        i = 0
        while i < count:
            try:
                func(i)
                i += 1
            except RuntimeError as err:
                assert_that(str(err), contains_string("ThrottlingException"))
                throttling_times += 1
        return throttling_times

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_send_message_rate(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        self._create_queue_and_assert(self.queue_name)

        counters = self._get_sqs_counters()
        throttling_counter_labels = {
            'subsystem': 'core',
            'user': self._username,
            'queue': self.queue_name,
            'sensor': 'RequestsThrottled',
        }
        prev_throttling_counter_value = self._get_counter_value(counters, throttling_counter_labels, 0)

        def call_send(i):
            if i % 2:
                self._sqs_api.send_message(self.queue_url, 'data')
            else:
                self._sqs_api.send_message_batch(self.queue_url, [SqsSendMessageParams('data'), SqsSendMessageParams('data')])

        start_time = time.time()
        throttling_times = self.call_n_times_except_throttling(50, call_send)
        duration = time.time() - start_time
        logging.debug('Duration: {}'.format(duration))
        assert_that(duration, greater_than(4))  # reserve 1 second for test stability

        if throttling_times:
            counters = self._get_sqs_counters()
            throttling_counter_value = self._get_counter_value(counters, throttling_counter_labels, 0)
            assert_that(throttling_counter_value - prev_throttling_counter_value, equal_to(throttling_times))

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_create_queue_rate(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_urls = []

        def call_create_queue(i):
            url = self._sqs_api.create_queue('{}_{}.fifo'.format(self.queue_name, i), is_fifo=True)
            queue_urls.append(url)

        start_time = time.time()
        self.call_n_times_except_throttling(6, call_create_queue)
        duration = time.time() - start_time
        logging.debug('Duration: {}'.format(duration))
        assert_that(duration, greater_than(2))  # reserve 1 second for test stability

        # test that one delete queue batch can delete all the queues despite that delete objects rate is only 2:
        assert_that(len(queue_urls), equal_to(6))
        delete_queue_batch_result = self._sqs_api.private_delete_queue_batch(queue_urls)
        logging.debug('Delete queue batch result: {}'.format(delete_queue_batch_result))
        assert_that(
            delete_queue_batch_result['DeleteQueueBatchResultEntry'], instance_of(list)
        )
        assert_that(
            len(delete_queue_batch_result['DeleteQueueBatchResultEntry']), equal_to(6)  # no errors, all items are results
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_other_requests_rate(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        self._create_queue_and_assert(self.queue_name)

        def call(i):
            if i % 2:
                self._sqs_api.get_queue_url(self.queue_name)
            else:
                self._sqs_api.get_queue_attributes(self.queue_url)

        start_time = time.time()
        self.call_n_times_except_throttling(30, call)
        duration = time.time() - start_time
        logging.debug('Duration: {}'.format(duration))
        assert_that(duration, greater_than(5))  # reserve 1 second for test stability
