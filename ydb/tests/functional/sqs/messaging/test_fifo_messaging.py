#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time

import pytest
from hamcrest import assert_that, equal_to, not_none, greater_than, less_than_or_equal_to, has_items, raises

from ydb.tests.library.sqs.matchers import ReadResponseMatcher, extract_message_ids

from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, get_test_with_sqs_installation_by_path, get_test_with_sqs_tenant_installation
from ydb.tests.library.sqs.test_base import VISIBILITY_CHANGE_METHOD_PARAMS, TABLES_FORMAT_PARAMS


class SqsFifoMicroBatchTest(KikimrSqsTestBase):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(SqsFifoMicroBatchTest, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['group_selection_batch_size'] = 2
        return config_generator

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_micro_batch_read(self, tables_format):
        self._init_with_params(is_fifo=True, tables_format=tables_format)
        created_queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=True)
        seq_no = 0
        max_number_of_messages = 3
        for i in range(max_number_of_messages):
            self._send_message_and_assert(created_queue_url, 'test_send_message', seq_no=str(seq_no), group_id=str(seq_no))
            seq_no += 1

        for i in range(2):
            msgs = self._sqs_api.receive_message(created_queue_url, max_number_of_messages=max_number_of_messages, visibility_timeout=0, receive_request_attempt_id='test')
            assert_that(len(set([msgs[i]['MessageId'] for i in range(len(msgs))])), equal_to(len(msgs)))


class TestSqsFifoMicroBatchesWithTenant(get_test_with_sqs_tenant_installation(SqsFifoMicroBatchTest)):
    pass


class TestSqsFifoMicroBatchesWithPath(get_test_with_sqs_installation_by_path(SqsFifoMicroBatchTest)):
    pass


class SqsFifoMessagingTest(KikimrSqsTestBase):
    def setup_method(self, method=None):
        super(SqsFifoMessagingTest, self).setup_method(method)
        self.queue_name = self.queue_name + ".fifo"

    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(SqsFifoMessagingTest, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['group_selection_batch_size'] = 100
        return config_generator

    def test_only_single_read_infly_from_fifo(self):
        self._init_with_params(tables_format=0)
        self._create_queue_send_x_messages_read_y_messages(
            self.queue_name, send_count=10, read_count=1, visibility_timeout=1000,
            msg_body_template=self._msg_body_template, is_fifo=True
        )
        self._read_messages_and_assert(
            self.queue_url, messages_count=10, matcher=ReadResponseMatcher().with_n_messages(0)
        )

    def test_fifo_read_delete_single_message(self):
        self._init_with_params(tables_format=0)
        created_queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=True)
        message_ids = self._send_messages(
            created_queue_url, message_count=10, msg_body_template=self._msg_body_template, is_fifo=True, group_id='group'
        )
        read_result = self._read_messages_and_assert(
            self.queue_url, messages_count=1, matcher=ReadResponseMatcher().with_message_ids(message_ids[:1])
        )
        handle = read_result[0]['ReceiptHandle']
        assert_that(
            self._sqs_api.delete_message(self.queue_url, handle), not_none()
        )
        self._read_messages_and_assert(
            self.queue_url, messages_count=1, matcher=ReadResponseMatcher().with_message_ids(message_ids[1:2])
        )

        counters = self._get_sqs_counters()
        delete_counter_labels = {
            'subsystem': 'core',
            'user': self._username,
            'queue': self.queue_name,
            'sensor': 'DeleteMessage_Count',
        }
        assert_that(self._get_counter_value(counters, delete_counter_labels, 0), equal_to(1))

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_write_and_read_to_different_groups(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        seq_no = 1
        self._create_queue_and_assert(self.queue_name, is_fifo=True)
        message_ids = []
        for group_id in range(10):
            msg_id = self._send_message_and_assert(
                self.queue_url, self._msg_body_template.format('1'), seq_no, str(group_id)
            )
            seq_no += 1
            message_ids.append(msg_id)

        result = self._read_messages_and_assert(
            self.queue_url, 10, visibility_timeout=1000, matcher=ReadResponseMatcher().with_n_messages(10)
        )
        received_message_ids = extract_message_ids(result)
        assert_that(
            sorted(received_message_ids), equal_to(sorted(message_ids))
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_can_read_from_different_groups(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        seq_no = 1
        self._create_queue_and_assert(self.queue_name, is_fifo=True)
        message_ids = []
        for group_id in range(20):
            msg_id = self._send_message_and_assert(
                self.queue_url, self._msg_body_template.format(group_id), seq_no, str(group_id)
            )
            seq_no += 1
            message_ids.append(msg_id)

        first_message_ids = extract_message_ids(
            self._read_messages_and_assert(
                self.queue_url, 10, ReadResponseMatcher().with_n_messages(10), visibility_timeout=1000
            )
        )
        second_message_ids = extract_message_ids(
            self._read_messages_and_assert(
                self.queue_url, 10, ReadResponseMatcher().with_n_messages(10), visibility_timeout=1000
            )
        )

        assert_that(
            len(set(first_message_ids + second_message_ids)),
            equal_to(len(first_message_ids) + len(second_message_ids))
        )
        self._read_messages_and_assert(
            self.queue_url, 10, visibility_timeout=1000, matcher=ReadResponseMatcher().with_n_messages(0)
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_send_and_read_multiple_messages(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=True)
        first_message_id = self._send_message_and_assert(
            queue_url, self._msg_body_template.format('0'), seq_no=1, group_id='group'
        )
        time.sleep(5)
        self._send_message_and_assert(
            queue_url, self._msg_body_template.format('1'), seq_no=2, group_id='group'
        )
        self._read_messages_and_assert(
            queue_url, messages_count=1,
            matcher=ReadResponseMatcher().with_message_ids([first_message_id, ])
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_read_dont_stall(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=True)
        pack_size = 5
        first_pack_ids = self._send_messages(
            queue_url, message_count=pack_size, msg_body_template=self._msg_body_template, is_fifo=True,
            group_id='1',
        )
        time.sleep(5)
        second_pack_ids = self._send_messages(
            queue_url, message_count=pack_size, msg_body_template=self._msg_body_template, is_fifo=True,
            group_id='2'
        )
        time.sleep(5)
        self._read_messages_and_assert(
            queue_url, messages_count=10, visibility_timeout=1000,
            matcher=ReadResponseMatcher().with_message_ids(
                [first_pack_ids[0], second_pack_ids[0]]
            )
        )
        third_pack_ids = self._send_messages(
            queue_url, message_count=pack_size, msg_body_template=self._msg_body_template, is_fifo=True,
            group_id='3'
        )
        self._read_messages_and_assert(
            queue_url, messages_count=10, visibility_timeout=1000,
            matcher=ReadResponseMatcher().with_message_ids(third_pack_ids[:1])
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_visibility_timeout_works(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        self._create_queue_send_x_messages_read_y_messages(
            self.queue_name, send_count=5, read_count=1, visibility_timeout=10,
            msg_body_template=self._msg_body_template, is_fifo=True, group_id='1'
        )
        second_pack_ids = self._send_messages(self.queue_url, 5, group_id='2', is_fifo=True)
        self._read_messages_and_assert(
            self.queue_url, messages_count=5, matcher=ReadResponseMatcher().with_these_or_more_message_ids(second_pack_ids[:1]),
            visibility_timeout=10
        )
        time.sleep(12)
        self._read_messages_and_assert(
            self.queue_url, messages_count=5, visibility_timeout=1000, matcher=ReadResponseMatcher().with_these_or_more_message_ids(
                [self.message_ids[0], second_pack_ids[0]]
            )
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_delete_message_works(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        self._create_queue_send_x_messages_read_y_messages(
            self.queue_name, send_count=10, read_count=1, visibility_timeout=1,
            msg_body_template=self._msg_body_template, is_fifo=True
        )

        handle = self.read_result[0]['ReceiptHandle']
        assert_that(
            self._sqs_api.delete_message(self.queue_url, handle), not_none()
        )
        time.sleep(1)
        self._read_messages_and_assert(
            self.queue_url, messages_count=5, visibility_timeout=1000, matcher=ReadResponseMatcher().with_message_ids(self.message_ids[1:2])
        )

        counters = self._get_sqs_counters()
        delete_counter_labels = {
            'subsystem': 'core',
            'user': self._username,
            'queue': self.queue_name,
            'sensor': 'DeleteMessage_Count',
        }
        assert_that(self._get_counter_value(counters, delete_counter_labels, 0), equal_to(1))

        # break a queue and check failure
        self._break_queue(self._username, self.queue_name, True)

        handle_2 = self.read_result[0]['ReceiptHandle']

        def call_delete():
            self._sqs_api.delete_message(self.queue_url, handle_2)

        assert_that(
            call_delete,
            raises(RuntimeError, pattern='InternalFailure')
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_write_read_delete_many_groups(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=True)
        message_ids = {}
        for i in range(10):
            message_ids[i] = self._send_messages(queue_url, 5, is_fifo=True, group_id=str(i))

        matcher = ReadResponseMatcher().with_n_messages(10).with_message_ids(
            [i[0] for i in message_ids.values()]
        ).with_messages_data(
            [self._msg_body_template.format(5*i) for i in range(10)]
        )
        result = self._read_messages_and_assert(queue_url, 10, matcher=matcher, visibility_timeout=5)
        # Delete message from group 0
        for msg in result:
            if msg['MessageId'] in message_ids[0]:
                assert_that(
                    self._sqs_api.delete_message(self.queue_url, msg['ReceiptHandle']), not_none()
                )
                message_ids[0] = message_ids[0][1:]
                break
        time.sleep(5)
        matcher = ReadResponseMatcher().with_n_messages(10).with_message_ids(
            [i[0] for i in message_ids.values()]
        ).with_messages_data(
            [self._msg_body_template.format(1)] + [self._msg_body_template.format(i*5) for i in range(1, 10)]
        )
        self._read_messages_and_assert(queue_url, 10, matcher=matcher, visibility_timeout=1000)

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_queue_attributes(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=True)
        attributes = self._sqs_api.get_queue_attributes(queue_url)
        assert_that(attributes, has_items(
            'FifoQueue',
            'ContentBasedDeduplication',
            'ApproximateNumberOfMessages',
            'ApproximateNumberOfMessagesDelayed',
            'ApproximateNumberOfMessagesNotVisible',
            'CreatedTimestamp',
            'DelaySeconds',
            'MaximumMessageSize',
            'MessageRetentionPeriod',
            'ReceiveMessageWaitTimeSeconds',
            'VisibilityTimeout',
        ))
        assert_that(attributes['VisibilityTimeout'], equal_to('30'))

        self._sqs_api.set_queue_attributes(queue_url, {'VisibilityTimeout': '2'})
        attributes = self._sqs_api.get_queue_attributes(queue_url)
        assert_that(attributes['VisibilityTimeout'], equal_to('2'))

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_validates_group_id(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=True)

        def send_caller(group_id):
            def call_send():
                self._sqs_api.send_message(queue_url, 'body', deduplication_id='42', group_id=group_id)

            return call_send

        def check(group_id):
            assert_that(
                send_caller(group_id),
                raises(RuntimeError, pattern='(MissingParameter|InvalidParameterValue)')
            )

        check('§')
        check('')
        check(None)  # without

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_validates_deduplication_id(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=True)

        def send_caller(deduplication_id):
            def call_send():
                self._sqs_api.send_message(queue_url, 'body', deduplication_id=deduplication_id, group_id='42')

            return call_send

        def check(deduplication_id):
            assert_that(
                send_caller(deduplication_id),
                raises(RuntimeError, pattern='(MissingParameter|InvalidParameterValue)')
            )

        check('§')
        check('')
        check(None)  # without

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_validates_receive_attempt_id(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=True)

        def receive_caller(receive_request_attempt_id):
            def call_receive():
                self._sqs_api.receive_message(queue_url, 'body', receive_request_attempt_id=receive_request_attempt_id)

            return call_receive

        def check(receive_request_attempt_id):
            assert_that(
                receive_caller(receive_request_attempt_id),
                raises(RuntimeError, pattern='InvalidParameterValue')
            )

        check('§')

    @pytest.mark.parametrize('content_based', [True, False], ids=['content_based', 'by_deduplication_id'])
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_deduplication(self, content_based, tables_format):
        self._init_with_params(tables_format=tables_format)
        attributes = {}
        if content_based:
            attributes['ContentBasedDeduplication'] = 'true'
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=True, use_http=True, attributes=attributes)

        if content_based:
            deduplication_id = None
            other_deduplication_id = None
            body_1 = 'body'
            body_2 = 'body'
            body_3 = 'other_body'
        else:
            deduplication_id = 'trololo'
            other_deduplication_id = '42'
            body_1 = 'body_1'
            body_2 = 'body_1'
            body_3 = 'body_1'

        def get_deduplicated_messages():
            counters = self._get_sqs_counters()
            labels = {
                'subsystem': 'core',
                'user': self._username,
                'queue': self.queue_name,
                'sensor': 'SendMessage_DeduplicationCount',
            }
            return self._get_counter_value(counters, labels, 0)

        self._sqs_api.send_message(queue_url, body_1, deduplication_id=deduplication_id, group_id='1')

        deduplicated = get_deduplicated_messages()
        self._sqs_api.send_message(queue_url, body_2, deduplication_id=deduplication_id, group_id='2')
        assert_that(get_deduplicated_messages(), equal_to(deduplicated + 1))

        self._sqs_api.send_message(queue_url, body_3, deduplication_id=other_deduplication_id, group_id='3')
        assert_that(get_deduplicated_messages(), equal_to(deduplicated + 1))

        self._read_messages_and_assert(queue_url, 10, visibility_timeout=1000, matcher=ReadResponseMatcher().with_n_messages(2), wait_timeout=3)

    @pytest.mark.parametrize('after_crutch_batch', [False, True], ids=['standard_mode', 'after_crutch_batch'])
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_receive_attempt_reloads_same_messages(self, after_crutch_batch, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=True)

        groups_selection_batch_size = self.config_generator.yaml_config['sqs_config']['group_selection_batch_size']
        assert_that(groups_selection_batch_size, equal_to(100))

        groups_count = groups_selection_batch_size + 50 if after_crutch_batch else 15
        for group_number in range(groups_count):
            self._send_messages(queue_url, 1, is_fifo=True, group_id='group_{}'.format(group_number))

        if after_crutch_batch:
            first_lock_messages_count = groups_selection_batch_size + 10
            self._read_messages_and_assert(
                queue_url, messages_count=first_lock_messages_count, visibility_timeout=1000,
                matcher=ReadResponseMatcher().with_n_messages(first_lock_messages_count)
            )

        receive_attempt_id = 'my_attempt'
        other_receive_attempt_id = 'my_other_attempt'

        def receive(attempt_id):
            read_result = self._sqs_api.receive_message(queue_url, max_number_of_messages=10, visibility_timeout=1000, receive_request_attempt_id=attempt_id)
            message_set = set([res['MessageId'] for res in read_result])
            return read_result, message_set

        read_result_1, message_set_1 = receive(receive_attempt_id)
        read_result_2, message_set_2 = receive(receive_attempt_id)
        read_result_3, message_set_3 = receive(other_receive_attempt_id)

        assert_that(len(read_result_1), greater_than(0))
        assert_that(len(read_result_1), equal_to(len(read_result_2)))
        assert_that(len(read_result_1) + len(read_result_3), less_than_or_equal_to(groups_count))

        assert_that(len(message_set_1 & message_set_2), equal_to(len(message_set_1)))
        assert_that(len(message_set_1 & message_set_3), equal_to(0))

    @pytest.mark.parametrize(**VISIBILITY_CHANGE_METHOD_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_visibility_change_disables_receive_attempt_id(self, delete_message, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=True)
        groups_count = 5
        for group_number in range(groups_count):
            self._send_messages(queue_url, 1, is_fifo=True, group_id='group_{}'.format(group_number))

        receive_attempt_id = 'my_attempt'
        read_result_1 = self._sqs_api.receive_message(queue_url, max_number_of_messages=10, visibility_timeout=1000, receive_request_attempt_id=receive_attempt_id)
        assert_that(len(read_result_1), greater_than(0))
        receipt_handle = read_result_1[0]['ReceiptHandle']
        logging.debug('{} message with receipt handle {}'.format('Delete' if delete_message else 'Change visibility of', receipt_handle))
        if delete_message:
            self._sqs_api.delete_message(queue_url, receipt_handle)
        else:
            self._sqs_api.change_message_visibility(queue_url, receipt_handle, visibility_timeout=1500)
        read_result_2 = self._sqs_api.receive_message(queue_url, max_number_of_messages=10, visibility_timeout=1000, receive_request_attempt_id=receive_attempt_id)
        if read_result_2 is None:
            read_result_2 = []
        assert_that(len(read_result_1) + len(read_result_2), less_than_or_equal_to(groups_count))
        if read_result_2:
            message_set_1 = set([res['MessageId'] for res in read_result_1])
            message_set_2 = set([res['MessageId'] for res in read_result_2])
            assert_that(len(message_set_1 & message_set_2), equal_to(0))

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_crutch_groups_selection_algorithm_selects_second_group_batch(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=True)
        groups_selection_batch_size = self.config_generator.yaml_config['sqs_config']['group_selection_batch_size']
        groups_count = groups_selection_batch_size + 50
        for group_number in range(groups_count):
            self._send_messages(queue_url, 1, is_fifo=True, group_id='group_{}'.format(group_number))

        self._read_messages_and_assert(
            queue_url, messages_count=groups_count, visibility_timeout=1000,
            matcher=ReadResponseMatcher().with_n_messages(groups_count)
        )


class TestSqsFifoMessagingWithTenant(get_test_with_sqs_tenant_installation(SqsFifoMessagingTest)):
    pass


class TestSqsFifoMessagingWithPath(get_test_with_sqs_installation_by_path(SqsFifoMessagingTest)):
    pass
