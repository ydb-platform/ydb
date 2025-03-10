#!/usr/bin/env python
# -*- coding: utf-8 -*-
import base64
import logging
import time

import pytest
from hamcrest import assert_that, equal_to, not_none, greater_than, has_item, has_items, raises, is_not, not_, empty, instance_of

from ydb.tests.library.sqs.requests_client import SqsMessageAttribute, SqsSendMessageParams, SqsChangeMessageVisibilityParams

from ydb.tests.library.sqs.matchers import ReadResponseMatcher, extract_message_ids
from ydb.tests.library.sqs.test_base import to_bytes
from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, get_test_with_sqs_installation_by_path, get_test_with_sqs_tenant_installation
from ydb.tests.library.sqs.test_base import IS_FIFO_PARAMS, TABLES_FORMAT_PARAMS


class SqsGenericMessagingTest(KikimrSqsTestBase):
    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_send_message(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)
        created_queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)
        self.seq_no += 1
        self._send_message_and_assert(created_queue_url, 'test_send_message', seq_no=self.seq_no if is_fifo else None, group_id='group' if is_fifo else None)

        if tables_format != 0:
            return
        # break a queue and check failure
        self._break_queue(self._username, self.queue_name, is_fifo)

        def call_send():
            group = 'trololo' if is_fifo else None
            dedup = group
            self._sqs_api.send_message(
                created_queue_url, 'body', deduplication_id=dedup, group_id=group
            )

        assert_that(
            call_send,
            raises(
                RuntimeError,
                pattern='InternalFailure'
            )
        )

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_send_message_batch(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)
        queue_attributes = {}
        if is_fifo:
            queue_attributes['ContentBasedDeduplication'] = 'true'
        created_queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo, use_http=True, attributes=queue_attributes)
        group_id = 'group' if is_fifo else None
        group_id_2 = 'group2' if is_fifo else None
        batch_request = [
            SqsSendMessageParams('body 1', attributes=[SqsMessageAttribute('a', 'String', 'b')], group_id=group_id),
            SqsSendMessageParams('body 2', delay_seconds=100500, group_id=group_id),
            SqsSendMessageParams('body 3', attributes=[SqsMessageAttribute('x', 'String', 'y')], group_id=group_id),
            SqsSendMessageParams('body 4', group_id=group_id_2),
        ]
        send_message_batch_result = self._sqs_api.send_message_batch(created_queue_url, batch_request)
        logging.debug('SendMessageBatch result: {}'.format(send_message_batch_result))

        assert_that(len(send_message_batch_result), equal_to(4))

        assert_that(send_message_batch_result[1]['BatchResultErrorEntry']['Code'], equal_to('InvalidParameterValue'))

        assert_that(send_message_batch_result[2]['SendMessageBatchResultEntry']['MD5OfMessageAttributes'],
                    not_(equal_to(send_message_batch_result[0]['SendMessageBatchResultEntry']['MD5OfMessageAttributes'])))

        assert_that(send_message_batch_result[2]['SendMessageBatchResultEntry']['MD5OfMessageBody'],
                    not_(equal_to(send_message_batch_result[0]['SendMessageBatchResultEntry']['MD5OfMessageBody'])))

        assert_that(send_message_batch_result[3]['SendMessageBatchResultEntry']['MD5OfMessageBody'],
                    not_(equal_to(send_message_batch_result[0]['SendMessageBatchResultEntry']['MD5OfMessageBody'])))

        message_ids = (
            send_message_batch_result[0]['SendMessageBatchResultEntry']['MessageId'],
            send_message_batch_result[2]['SendMessageBatchResultEntry']['MessageId'],
            send_message_batch_result[3]['SendMessageBatchResultEntry']['MessageId'],
        )
        for i in range(3):
            msgs = self._sqs_api.receive_message(created_queue_url, max_number_of_messages=1,
                                                 visibility_timeout=1000)
            assert_that(len(msgs), equal_to(1))
            assert_that(message_ids, has_item(msgs[0]['MessageId']))
            assert_that(
                self._sqs_api.delete_message(created_queue_url, msgs[0]['ReceiptHandle']), not_none()
            )

        msgs = self._sqs_api.receive_message(created_queue_url, max_number_of_messages=10,
                                             visibility_timeout=1000)
        assert_that(len([] if msgs is None else msgs), equal_to(0))

        if tables_format != 0:
            return
        # break a queue and check failure
        self._break_queue(self._username, self.queue_name, is_fifo)

        group = 'trololo' if is_fifo else None
        dedup = group
        errors = self._sqs_api.send_message_batch(
            created_queue_url, [SqsSendMessageParams('other body', group_id=group, deduplication_id=dedup)]
        )
        assert_that(len(errors), equal_to(1))
        assert_that(errors[0]['BatchResultErrorEntry']['Code'], equal_to('InternalFailure'))

        # Test error handling
        def call_send_message_batch_with_greater_than_ten_messages():
            requests = []
            for i in range(11):
                requests.append(SqsSendMessageParams('body', attributes=[], group_id=group_id))
            self._sqs_api.send_message_batch(created_queue_url, requests)

        assert_that(
            call_send_message_batch_with_greater_than_ten_messages,
            raises(
                RuntimeError,
                pattern='AWS.SimpleQueueService.TooManyEntriesInBatchRequest'
            )
        )

        def call_send_message_batch_with_no_messages():
            requests = []
            self._sqs_api.send_message_batch(created_queue_url, requests)

        assert_that(
            call_send_message_batch_with_no_messages,
            raises(
                RuntimeError,
                pattern='AWS.SimpleQueueService.EmptyBatchRequest'
            )
        )

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_send_and_read_message(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        created_queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)
        body = '<' + self._msg_body_template.format('trololo') + '<'  # to ensure that we have correct xml
        attributes = {
            SqsMessageAttribute('a', 'String', 'xyz'),
            SqsMessageAttribute('b', 'Number', 42),
            SqsMessageAttribute('c', 'Binary', base64.b64encode(b'binary_data')),
        }
        if is_fifo:
            self.seq_no += 1
            message_id = self._send_message_and_assert(created_queue_url, body, group_id='3', attributes=attributes, seq_no=self.seq_no)
        else:
            message_id = self._send_message_and_assert(created_queue_url, body, attributes=attributes)
        read_message_result = self._read_while_not_empty(created_queue_url, 1)

        assert_that(
            read_message_result, ReadResponseMatcher().with_message_ids([message_id, ])
        )

        message_attributes = read_message_result[0]['MessageAttribute']
        message_attributes_by_name = {}
        for ma in message_attributes:
            assert_that(
                ma, has_item('Name')
            )
            message_attributes_by_name[ma['Name']] = ma
        assert_that(
            message_attributes_by_name,
            has_items('a', 'b', 'c')
        )
        assert_that(
            message_attributes_by_name['a']['Value']['DataType'],
            equal_to('String')
        )
        assert_that(
            message_attributes_by_name['a']['Value']['StringValue'],
            equal_to('xyz')
        )
        assert_that(
            message_attributes_by_name['b']['Value']['DataType'],
            equal_to('Number')
        )
        assert_that(
            message_attributes_by_name['b']['Value']['StringValue'],
            equal_to('42')
        )
        assert_that(
            message_attributes_by_name['c']['Value']['DataType'],
            equal_to('Binary')
        )
        assert_that(
            to_bytes(message_attributes_by_name['c']['Value']['BinaryValue']),
            equal_to(base64.b64encode(to_bytes('binary_data')))
        )

        attributes = read_message_result[0]['Attribute']
        attributes_by_name = {}
        for a in attributes:
            assert_that(
                a, has_item('Name')
            )
            attributes_by_name[a['Name']] = a

        def assert_has_nonempty_attribute(name):
            assert_that(
                attributes_by_name,
                has_item(name)
            )
            assert_that(attributes_by_name[name]['Value'], not_(empty()))

        assert_has_nonempty_attribute('ApproximateFirstReceiveTimestamp')
        assert_has_nonempty_attribute('ApproximateReceiveCount')
        assert_has_nonempty_attribute('SentTimestamp')

        if is_fifo:
            assert_has_nonempty_attribute('MessageDeduplicationId')
            assert_has_nonempty_attribute('MessageGroupId')
            assert_has_nonempty_attribute('SequenceNumber')
        else:
            assert_that(
                attributes_by_name,
                not_(has_item('MessageDeduplicationId'))
            )
            assert_that(
                attributes_by_name,
                not_(has_item('MessageGroupId'))
            )
            assert_that(
                attributes_by_name,
                not_(has_item('SequenceNumber'))
            )

        counters = self._get_sqs_counters()
        send_counter_labels = {
            'subsystem': 'core',
            'user': self._username,
            'queue': self.queue_name,
            'sensor': 'SendMessage_Count',
        }
        receive_counter_labels = {
            'subsystem': 'core',
            'user': self._username,
            'queue': self.queue_name,
            'sensor': 'ReceiveMessage_Count',
        }
        assert_that(self._get_counter_value(counters, send_counter_labels, 0), equal_to(1))
        assert_that(self._get_counter_value(counters, receive_counter_labels, 0), equal_to(1))

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_validates_message_attributes(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        created_queue_url = self._create_queue_and_assert(self.queue_name)

        def call_send(attributes):
            self._sqs_api.send_message(
                created_queue_url, 'msg_body', attributes=attributes
            )

        def assert_invalid_attributes(attributes):
            def call_with_attrs():
                call_send(attributes)
            assert_that(
                call_with_attrs,
                raises(
                    RuntimeError,
                    pattern='InvalidParameterValue|InvalidParameterCombination'
                )
            )

        attributes1 = {
            SqsMessageAttribute('!a', 'String', 'xyz'),
        }
        assert_invalid_attributes(attributes1)

        attributes2 = {
            SqsMessageAttribute('a', 'String', 'xyz'),
            SqsMessageAttribute('a', 'String', 'xyz2'),
        }
        assert_invalid_attributes(attributes2)

        attributes3 = {
            SqsMessageAttribute('ya.reserved-prefix', 'String', 'xyz')
        }
        assert_invalid_attributes(attributes3)

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_send_to_nonexistent_queue(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        created_queue_url = self._create_queue_and_assert(self.queue_name)

        def call_send():
            self._sqs_api.send_message(
                to_bytes(created_queue_url) + to_bytes('1'), to_bytes('42')
            )

        assert_that(
            call_send,
            raises(
                RuntimeError,
                pattern='AWS.SimpleQueueService.NonExistentQueue'
            )
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_receive_with_very_big_visibility_timeout(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name)

        def call_with_very_big_visibility_timeout():
            self._sqs_api.receive_message(queue_url, visibility_timeout=100500)

        assert_that(
            call_with_very_big_visibility_timeout,
            raises(RuntimeError, pattern='InvalidParameter')
        )

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_create_q_twice(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        created_queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)
        self.seq_no += 1
        message_id = self._send_message_and_assert(created_queue_url, self._msg_body_template, seq_no=self.seq_no if is_fifo else None, group_id='group' if is_fifo else None)
        second_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)
        assert_that(second_url, equal_to(created_queue_url))

        self._read_messages_and_assert(
            created_queue_url,
            messages_count=1,
            visibility_timeout=1000, matcher=ReadResponseMatcher().with_message_ids([message_id, ])
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_send_and_read_multiple_messages(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=False)
        first_message_id = self._send_message_and_assert(
            queue_url, self._msg_body_template.format('0')
        )
        time.sleep(5)
        second_message_id = self._send_message_and_assert(
            queue_url, self._msg_body_template.format('1')
        )
        self._read_messages_and_assert(
            queue_url, messages_count=2, visibility_timeout=1000,
            matcher=ReadResponseMatcher().with_message_ids(
                [first_message_id, second_message_id]
            ).with_messages_data([self._msg_body_template.format('0'), self._msg_body_template.format('1')])
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_read_dont_stall(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=False)
        first_pack_size = 5
        second_pack_size = 5
        first_pack_ids = self._send_messages(
            queue_url, message_count=first_pack_size, msg_body_template=self._msg_body_template
        )
        self._send_messages(queue_url, message_count=second_pack_size, msg_body_template=self._msg_body_template)
        self._read_messages_and_assert(
            queue_url, messages_count=10, visibility_timeout=1000, matcher=ReadResponseMatcher(
            ).with_n_or_more_messages(first_pack_size + 1).with_these_or_more_message_ids(first_pack_ids)
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_multi_read_dont_stall(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name)
        pack_size = 7
        total_packs = 3
        all_ids = set()
        read_ids = set()
        for _ in range(total_packs):
            all_ids.update(self._send_messages(
                queue_url, message_count=pack_size, msg_body_template=self._msg_body_template
            ))
            result = self._read_messages_and_assert(
                queue_url, messages_count=10, visibility_timeout=1000,
                matcher=ReadResponseMatcher().with_some_of_message_ids(all_ids - read_ids)
            )
            if result:
                read_ids.update(extract_message_ids(result))

        assert_that(
            len(read_ids), greater_than(pack_size),
            "Wrote {packs} packs of size {size}, but got only {total_read} messages after all read attempts".format(
                packs=total_packs, size=pack_size, total_read=len(read_ids)
            )
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_visibility_timeout_works(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        total_msg_count = 10
        visibility_timeout = 5

        before_read_time = time.time()
        self._create_queue_send_x_messages_read_y_messages(
            self.queue_name, send_count=total_msg_count, read_count=1, visibility_timeout=visibility_timeout,
            msg_body_template=self._msg_body_template, is_fifo=False
        )
        msg_data = [self._msg_body_template.format(i) for i in range(total_msg_count)]

        already_read_id = extract_message_ids(self.read_result)[0]
        read_result = self._read_messages_and_assert(
            self.queue_url, messages_count=5, visibility_timeout=visibility_timeout, wait_timeout=1,
            matcher=ReadResponseMatcher().with_some_of_message_ids(self.message_ids)
                .with_n_messages(5)
        )

        read_time_2 = time.time()
        if read_time_2 - before_read_time < visibility_timeout:
            read_ids = set(extract_message_ids(read_result))
            assert_that(read_ids, not_(has_item(already_read_id)))

        time.sleep(visibility_timeout + 0.1)
        self._read_messages_and_assert(
            self.queue_url, messages_count=10, visibility_timeout=1000,
            matcher=ReadResponseMatcher().with_message_ids(self.message_ids).with_messages_data(msg_data)
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_visibility_timeout_expires_on_wait_timeout(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=False)
        message_ids = self._send_messages(
            queue_url, message_count=10, msg_body_template=self._msg_body_template
        )
        # noinspection PyTypeChecker
        self._read_messages_and_assert(
            queue_url, messages_count=10, matcher=ReadResponseMatcher().with_message_ids(message_ids),
            visibility_timeout=9
        )
        self._read_messages_and_assert(
            queue_url, messages_count=10, visibility_timeout=1000, matcher=ReadResponseMatcher().with_some_of_message_ids(message_ids), wait_timeout=10,
        )

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_zero_visibility_timeout_works(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        self._create_queue_send_x_messages_read_y_messages(
            self.queue_name, send_count=1, read_count=1, visibility_timeout=0,
            msg_body_template=self._msg_body_template, is_fifo=is_fifo
        )
        self._read_messages_and_assert(
            self.queue_url, messages_count=1, visibility_timeout=0, wait_timeout=1,
            matcher=ReadResponseMatcher().with_n_messages(1)
        )

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_change_visibility_works(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        self._create_queue_send_x_messages_read_y_messages(
            self.queue_name, send_count=1, read_count=1, visibility_timeout=1000,
            msg_body_template=self._msg_body_template, is_fifo=is_fifo
        )
        receipt_handle = self.read_result[0]['ReceiptHandle']
        logging.debug('First received receipt handle: {}'.format(receipt_handle))
        # decrease
        self._sqs_api.change_message_visibility(self.queue_url, receipt_handle, 1)
        time.sleep(2)

        before_receive = time.time()
        receive_visibility_timeout = 5
        read_result_2 = self._read_messages_and_assert(
            self.queue_url, messages_count=1, visibility_timeout=receive_visibility_timeout, wait_timeout=1,
            matcher=ReadResponseMatcher().with_n_messages(1)
        )
        receipt_handle = read_result_2[0]['ReceiptHandle']
        logging.debug('Second received receipt handle: {}'.format(receipt_handle))
        # increase
        changed = False
        try:
            self._sqs_api.change_message_visibility(self.queue_url, receipt_handle, 1000)
            changed = True
        except Exception as ex:
            seconds_passed = time.time() - before_receive
            logging.info('Exception while changing message visibility: {}'.format(ex))
            if seconds_passed < receive_visibility_timeout:
                raise
            elif str(ex).find('AWS.SimpleQueueService.MessageNotInflight') == -1:
                raise

        if changed:
            time.sleep(receive_visibility_timeout + 0.1)
            self._read_messages_and_assert(
                self.queue_url, messages_count=1, visibility_timeout=100, wait_timeout=0,
                matcher=ReadResponseMatcher().with_n_messages(0)
            )

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_change_visibility_batch_works(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        self._create_queue_and_assert(self.queue_name, is_fifo)

        if is_fifo:
            self._send_messages(
                self.queue_url, 1, self._msg_body_template, is_fifo=True, group_id='group_1'
            )
            self.seq_no += 1
            self._send_messages(
                self.queue_url, 1, self._msg_body_template, is_fifo=True, group_id='group_2'
            )
        else:
            self._send_messages(
                self.queue_url, 2, self._msg_body_template
            )

        read_result = self._read_messages_and_assert(
            self.queue_url, messages_count=2,
            matcher=ReadResponseMatcher().with_n_messages(2),
            visibility_timeout=1000, wait_timeout=10
        )

        receipt_handle_1 = read_result[0]['ReceiptHandle']
        receipt_handle_2 = read_result[1]['ReceiptHandle']
        self._sqs_api.change_message_visibility_batch(self.queue_url, [
            SqsChangeMessageVisibilityParams(receipt_handle_1, 1),
            SqsChangeMessageVisibilityParams(receipt_handle_2, 10),
        ])
        time.sleep(2)
        read_result = self._read_messages_and_assert(
            self.queue_url, messages_count=10, visibility_timeout=1000, wait_timeout=1,
            matcher=ReadResponseMatcher().with_n_or_more_messages(1)  # test machine may be slow and visibility timeout may expire
        )
        if len(read_result) == 1:
            time.sleep(9)
            self._read_messages_and_assert(
                self.queue_url, messages_count=1, visibility_timeout=1000, wait_timeout=1,
                matcher=ReadResponseMatcher().with_n_messages(1)
            )

        # Test error handling
        def call_change_message_visibility_batch_with_greater_than_ten_handles():
            requests = []
            for i in range(11):
                requests.append(SqsChangeMessageVisibilityParams('Handle{}'.format(i), 1))
            self._sqs_api.change_message_visibility_batch(self.queue_url, requests)

        assert_that(
            call_change_message_visibility_batch_with_greater_than_ten_handles,
            raises(
                RuntimeError,
                pattern='AWS.SimpleQueueService.TooManyEntriesInBatchRequest'
            )
        )

        def call_change_message_visibility_batch_with_no_handles():
            requests = []
            self._sqs_api.change_message_visibility_batch(self.queue_url, requests)

        assert_that(
            call_change_message_visibility_batch_with_no_handles,
            raises(
                RuntimeError,
                pattern='AWS.SimpleQueueService.EmptyBatchRequest'
            )
        )

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_change_visibility_to_zero_works(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        self._create_queue_send_x_messages_read_y_messages(
            self.queue_name, send_count=1, read_count=1, visibility_timeout=1000,
            msg_body_template=self._msg_body_template, is_fifo=is_fifo
        )
        receipt_handle = self.read_result[0]['ReceiptHandle']
        self._sqs_api.change_message_visibility(self.queue_url, receipt_handle, 0)
        self._read_messages_and_assert(
            self.queue_url, messages_count=1, visibility_timeout=0, wait_timeout=1,
            matcher=ReadResponseMatcher().with_n_messages(1)
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_change_message_visibility_with_very_big_timeout(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name)

        self._create_queue_send_x_messages_read_y_messages(
            self.queue_name, send_count=2, read_count=2, visibility_timeout=1000,
            msg_body_template=self._msg_body_template, is_fifo=False
        )
        receipt_handle_1 = self.read_result[0]['ReceiptHandle']
        receipt_handle_2 = self.read_result[1]['ReceiptHandle']

        def call_with_very_big_visibility_timeout():
            self._sqs_api.change_message_visibility(queue_url, receipt_handle_1, 100500)

        assert_that(
            call_with_very_big_visibility_timeout,
            raises(RuntimeError, pattern='InvalidParameter')
        )

        result = self._sqs_api.change_message_visibility_batch(self.queue_url, [
            SqsChangeMessageVisibilityParams(receipt_handle_1, 100500),
            SqsChangeMessageVisibilityParams(receipt_handle_2, 10),
        ])
        assert_that(len(result), equal_to(2))
        assert_that(result[0]['BatchResultErrorEntry']['Code'], equal_to('InvalidParameterValue'))

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_does_not_change_visibility_not_in_flight(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        self._create_queue_send_x_messages_read_y_messages(
            self.queue_name, send_count=1, read_count=1, visibility_timeout=1,
            msg_body_template=self._msg_body_template, is_fifo=is_fifo
        )
        receipt_handle = self.read_result[0]['ReceiptHandle']
        logging.debug('Received receipt handle: {}'.format(receipt_handle))
        time.sleep(2)

        def call_change_visibility():
            self._sqs_api.change_message_visibility(self.queue_url, receipt_handle, 1000)

        assert_that(
            call_change_visibility,
            raises(RuntimeError, pattern='.*\n.*AWS\\.SimpleQueueService\\.MessageNotInflight')
        )

        # Check that we can receive message after change message visibility to big timeout failed
        self._read_messages_and_assert(
            self.queue_url, messages_count=1, visibility_timeout=10, wait_timeout=10,
            matcher=ReadResponseMatcher().with_n_messages(1)
        )

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_does_not_change_visibility_for_deleted_message(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        self._create_queue_send_x_messages_read_y_messages(
            self.queue_name, send_count=1, read_count=1, visibility_timeout=1000,
            msg_body_template=self._msg_body_template, is_fifo=is_fifo
        )
        receipt_handle = self.read_result[0]['ReceiptHandle']
        logging.debug('Received receipt handle: {}'.format(receipt_handle))

        self._sqs_api.delete_message(self.queue_url, receipt_handle)

        def call_change_visibility():
            self._sqs_api.change_message_visibility(self.queue_url, receipt_handle, 10)

        assert_that(
            call_change_visibility,
            raises(RuntimeError, pattern='InvalidParameterValue')
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_delete_message_works(self, tables_format):
        self._init_with_params(tables_format=tables_format)

        self._create_queue_send_x_messages_read_y_messages(
            self.queue_name, send_count=10, read_count=2, visibility_timeout=0,
            msg_body_template=self._msg_body_template
        )
        handle = self.read_result[0]['ReceiptHandle']
        self.message_ids.remove(self.read_result[0]['MessageId'])

        assert_that(
            self._sqs_api.delete_message(self.queue_url, handle), not_none()
        )
        # check double deletion
        assert_that(
            self._sqs_api.delete_message(self.queue_url, handle), not_none()
        )
        self._read_messages_and_assert(
            self.queue_url, messages_count=10, visibility_timeout=1000,
            matcher=ReadResponseMatcher().with_message_ids(self.message_ids)
        )

        counters = self._get_sqs_counters()
        delete_counter_labels = {
            'subsystem': 'core',
            'user': self._username,
            'queue': self.queue_name,
            'sensor': 'DeleteMessage_Count',
        }
        assert_that(self._get_counter_value(counters, delete_counter_labels, 0), equal_to(1))

        if tables_format != 0:
            return

        # break a queue and check failure
        self._break_queue(self._username, self.queue_name, False)

        handle_2 = self.read_result[0]['ReceiptHandle']

        def call_delete():
            self._sqs_api.delete_message(self.queue_url, handle_2)

        assert_that(
            call_delete,
            raises(RuntimeError, pattern='InternalFailure')
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_delete_message_batch_works(self, tables_format):
        self._init_with_params(tables_format=tables_format)

        self._create_queue_send_x_messages_read_y_messages(
            self.queue_name, send_count=9, read_count=9,
            msg_body_template=self._msg_body_template
        )
        handles = []
        for result in self.read_result:
            handles.append(result['ReceiptHandle'])
        handles.insert(5, 'not_a_receipt_handle')

        assert_that(len(handles), equal_to(10))

        delete_message_batch_result = self._sqs_api.delete_message_batch(self.queue_url, handles)
        assert_that(len(delete_message_batch_result), equal_to(10))

        for i in range(len(delete_message_batch_result)):
            res = delete_message_batch_result[i]
            if i == 5:
                assert_that(res['BatchResultErrorEntry']['Code'], equal_to('ReceiptHandleIsInvalid'))
            else:
                assert_that(res, has_item('DeleteMessageBatchResultEntry'))

        self._read_messages_and_assert(
            self.queue_url, messages_count=10, visibility_timeout=1000,
            matcher=ReadResponseMatcher().with_n_messages(0)
        )

        # Test error handling
        def call_delete_message_batch_with_greater_than_ten_handles():
            handles = []
            for i in range(11):
                handles.append('Handle{}'.format(i))
            self._sqs_api.delete_message_batch(self.queue_url, handles)

        assert_that(
            call_delete_message_batch_with_greater_than_ten_handles,
            raises(
                RuntimeError,
                pattern='AWS.SimpleQueueService.TooManyEntriesInBatchRequest'
            )
        )

        def call_delete_message_batch_with_no_handles():
            handles = []
            self._sqs_api.delete_message_batch(self.queue_url, handles)

        assert_that(
            call_delete_message_batch_with_no_handles,
            raises(
                RuntimeError,
                pattern='AWS.SimpleQueueService.EmptyBatchRequest'
            )
        )

        counters = self._get_sqs_counters()
        delete_counter_labels = {
            'subsystem': 'core',
            'user': self._username,
            'queue': self.queue_name,
            'sensor': 'DeleteMessage_Count',
        }
        assert_that(self._get_counter_value(counters, delete_counter_labels, 0), equal_to(9))

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_delete_message_batch_deduplicates_receipt_handle(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        self._create_queue_send_x_messages_read_y_messages(
            self.queue_name, send_count=1, read_count=1,
            msg_body_template=self._msg_body_template, is_fifo=is_fifo
        )
        handle = self.read_result[0]['ReceiptHandle']
        handles = [handle for i in range(5)]

        delete_message_batch_result = self._sqs_api.delete_message_batch(self.queue_url, handles)
        assert_that(len(delete_message_batch_result), equal_to(5))

        for i in range(len(delete_message_batch_result)):
            res = delete_message_batch_result[i]
            assert_that(res, has_item('DeleteMessageBatchResultEntry'))

        labels = {
            'subsystem': 'core',
            'user': self._username,
            'queue': self.queue_name,
            'sensor': 'MessagesCount'
        }

        attempts = 30
        while attempts:
            attempts -= 1
            counters = self._get_sqs_counters()
            messages_count_metric = self._get_counter_value(counters, labels)
            if (messages_count_metric is None or messages_count_metric != 0) and attempts:
                time.sleep(1)
            else:
                assert_that(messages_count_metric, equal_to(0))

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_can_read_new_written_data_on_visibility_timeout(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        visibility_timeout = 15
        self._create_queue_send_x_messages_read_y_messages(
            self.queue_name, send_count=7, read_count=4, visibility_timeout=visibility_timeout,
            msg_body_template=self._msg_body_template
        )
        begin_time = time.time()
        first_pack_ids = self.message_ids
        second_pack_ids = self._send_messages(
            self.queue_url, message_count=3, msg_body_template=self._msg_body_template
        )
        message_ids = set(first_pack_ids) - set(extract_message_ids(self.read_result))
        message_ids.update(second_pack_ids)

        visibility_timeout_2 = 15
        self._read_messages_and_assert(
            self.queue_url, messages_count=10, visibility_timeout=visibility_timeout_2,
            matcher=ReadResponseMatcher().with_these_or_more_message_ids(message_ids).with_n_or_more_messages(6),
        )
        remaining_time = visibility_timeout - (time.time() - begin_time)  # for first pack read
        time.sleep(max(remaining_time + 0.1, visibility_timeout_2 + 0.1, 0))
        self._read_messages_and_assert(
            self.queue_url, messages_count=10, visibility_timeout=1000, matcher=ReadResponseMatcher().with_message_ids(
                first_pack_ids + second_pack_ids
            )
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_partial_delete_works(self, tables_format):
        self._init_with_params(tables_format=tables_format)

        self._create_queue_send_x_messages_read_y_messages(
            self.queue_name, send_count=10, read_count=5, visibility_timeout=5,
            msg_body_template=self._msg_body_template
        )
        handle = self.read_result[4]['ReceiptHandle']  # select the last read message to avoid race with visibility timeout while reading
        self.message_ids.remove(self.read_result[4]['MessageId'])
        assert_that(
            self._sqs_api.delete_message(self.queue_url, handle),
            not_none()
        )
        time.sleep(6)
        self._read_messages_and_assert(
            self.queue_url, messages_count=10, visibility_timeout=1000,
            matcher=ReadResponseMatcher().with_message_ids(self.message_ids)
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_wrong_delete_fails(self, tables_format):
        self._init_with_params(tables_format=tables_format)

        self._create_queue_send_x_messages_read_y_messages(
            self.queue_name, send_count=1, read_count=1, visibility_timeout=5,
            msg_body_template=self._msg_body_template
        )
        handle = self.read_result[0]['ReceiptHandle']

        def call_delete_invalid_handle():
            self._sqs_api.delete_message(self.queue_url, handle + handle),  # wrong handle
        assert_that(
            call_delete_invalid_handle,
            raises(RuntimeError, pattern='.*\n.*ReceiptHandleIsInvalid')
        )

        def call_delete_without_handle():
            logging.debug('Calling delete message without receipt handle on queue {}'.format(self.queue_url))
            self._sqs_api.execute_request(
                action='DeleteMessage',
                extract_result_method=lambda x: x['DeleteMessageResponse']['ResponseMetadata']['RequestId'],
                QueueUrl=self.queue_url  # no receipt handle
            )
        assert_that(
            call_delete_without_handle,
            raises(RuntimeError)
        )

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_queue_attributes(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)

        # assert empty response when no attribute names are provided
        attributes = self._sqs_api.get_queue_attributes(queue_url, attributes=[])
        assert_that(attributes, equal_to({}))

        # check common case
        attributes = self._sqs_api.get_queue_attributes(queue_url, attributes=['All'])
        if is_fifo:
            assert_that(attributes, has_item('FifoQueue'))
            assert_that(attributes, has_item('ContentBasedDeduplication'))
        else:
            assert_that(attributes, is_not(has_item('FifoQueue')))
            assert_that(attributes, is_not(has_item('ContentBasedDeduplication')))

        assert_that(attributes, has_items(
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
        assert_that(attributes['ReceiveMessageWaitTimeSeconds'], equal_to('0'))
        if is_fifo:
            assert_that(attributes['ContentBasedDeduplication'], equal_to('false'))

        self._sqs_api.set_queue_attributes(queue_url, {'ReceiveMessageWaitTimeSeconds': '10', 'MaximumMessageSize': '111111'})
        attributes = self._sqs_api.get_queue_attributes(queue_url)
        assert_that(attributes['ReceiveMessageWaitTimeSeconds'], equal_to('10'))
        assert_that(attributes['MaximumMessageSize'], equal_to('111111'))

        if is_fifo:
            self._sqs_api.set_queue_attributes(queue_url, {'ContentBasedDeduplication': 'true'})
            attributes = self._sqs_api.get_queue_attributes(queue_url)
            assert_that(attributes['ContentBasedDeduplication'], equal_to('true'))

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_set_very_big_visibility_timeout(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name)

        def call_with_very_big_visibility_timeout():
            self._sqs_api.set_queue_attributes(queue_url, {'VisibilityTimeout': '100500'})

        assert_that(
            call_with_very_big_visibility_timeout,
            raises(RuntimeError, pattern='InvalidAttributeValue')
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_wrong_attribute_name(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name)

        def call_with_wrong_attribute_name():
            self._sqs_api.get_queue_attributes(queue_url, ['All', 'UnknownAttributeName'])

        assert_that(
            call_with_wrong_attribute_name,
            raises(RuntimeError, pattern='InvalidAttributeName')
        )

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_get_queue_attributes_only_runtime_attributes(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)
        attributes = self._sqs_api.get_queue_attributes(queue_url, ['ApproximateNumberOfMessagesDelayed'])
        assert_that(int(attributes['ApproximateNumberOfMessagesDelayed']), equal_to(0))

        self._send_message_and_assert(queue_url, 'test', delay_seconds=900, seq_no='1' if is_fifo else None, group_id='group' if is_fifo else None)
        attributes = self._sqs_api.get_queue_attributes(queue_url, ['ApproximateNumberOfMessagesDelayed'])
        assert_that(int(attributes['ApproximateNumberOfMessagesDelayed']), equal_to(1))

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_get_queue_attributes_only_attributes_table(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)
        attributes = self._sqs_api.get_queue_attributes(queue_url, ['MaximumMessageSize'])
        assert_that(int(attributes['MaximumMessageSize']), equal_to(256 * 1024))

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_queue_attributes_batch(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        # Create > 10 queues to check that private commands for UI will work
        queue_urls = [self._create_queue_and_assert("{}-{}".format(self.queue_name, i)) for i in range(10)]
        created_queue_url2 = self._create_queue_and_assert(self.queue_name + '1.fifo', is_fifo=True)
        queue_urls.append(created_queue_url2)
        queue_urls.append(created_queue_url2 + '_nonexistent_queue_url')
        batch_result = self._sqs_api.private_get_queue_attributes_batch(queue_urls)
        assert_that(
            batch_result['GetQueueAttributesBatchResultEntry'], instance_of(list)
        )
        assert_that(
            len(batch_result['GetQueueAttributesBatchResultEntry']), equal_to(11)
        )
        assert_that(
            batch_result['BatchResultErrorEntry'], instance_of(dict)  # that means that we have only one entry for error
        )

        for entry in batch_result['GetQueueAttributesBatchResultEntry']:
            assert_that(
                entry['__AttributesDict'],
                has_items(
                    'ReceiveMessageWaitTimeSeconds',
                    'VisibilityTimeout',
                    'ApproximateNumberOfMessages',
                    'ApproximateNumberOfMessagesNotVisible',
                    'CreatedTimestamp',
                    'MaximumMessageSize',
                    'MessageRetentionPeriod',
                )
            )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_create_queue_by_nonexistent_user_fails(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        api = self._create_api_for_user('unknown_user')
        try:
            api.create_queue('known_queue_name')
            assert False, 'Exception is expected'
        except RuntimeError as ex:
            # Check that exception pattern will not give us any internal information
            assert_that(str(ex).find('.cpp:'), equal_to(-1))

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_list_queues_of_nonexistent_user(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        api = self._create_api_for_user('unknown_user')

        def call_list_queues():
            api.list_queues()

        assert_that(
            call_list_queues,
            raises(RuntimeError, pattern='OptInRequired')
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_invalid_queue_url(self, tables_format):
        self._init_with_params(tables_format=tables_format)

        def call_with_invalid_queue_url():
            self._sqs_api.get_queue_attributes('invalid_queue_url')

        assert_that(
            call_with_invalid_queue_url,
            raises(RuntimeError, pattern='InvalidParameterValue')
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_empty_queue_url(self, tables_format):
        self._init_with_params(tables_format=tables_format)

        def call_with_empty_queue_url():
            self._sqs_api.send_message(queue_url='', message_body='body')

        assert_that(
            call_with_empty_queue_url,
            raises(RuntimeError, pattern='MissingParameter')
        )

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_delay_one_message(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)
        created_queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)
        self._send_message_and_assert(created_queue_url, 'test_delay_message', delay_seconds=900, seq_no='1' if is_fifo else None, group_id='group' if is_fifo else None)

        self._read_messages_and_assert(
            created_queue_url, 10, ReadResponseMatcher().with_n_messages(0)
        )

        message_id = self._send_message_and_assert(created_queue_url, 'test_delay_message_2', delay_seconds=2, seq_no='2' if is_fifo else None, group_id='group_2' if is_fifo else None)
        time.sleep(3)
        self._read_messages_and_assert(
            created_queue_url, 10, visibility_timeout=1000, matcher=ReadResponseMatcher().with_message_ids([message_id])
        )

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_delay_message_batch(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)
        created_queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)

        def get_group_id(i):
            group_id = 'group_{}'.format(i) if is_fifo else None
            return group_id

        batch_request = [
            SqsSendMessageParams('body 1', group_id=get_group_id(1), deduplication_id='1' if is_fifo else None),
            SqsSendMessageParams('body 2', delay_seconds=900, group_id=get_group_id(2), deduplication_id='2' if is_fifo else None),
            SqsSendMessageParams('body 3', delay_seconds=2, group_id=get_group_id(3), deduplication_id='3' if is_fifo else None),
        ]
        send_message_batch_result = self._sqs_api.send_message_batch(created_queue_url, batch_request)
        assert_that(len(send_message_batch_result), equal_to(3))

        assert_that(send_message_batch_result[0], has_item('SendMessageBatchResultEntry'))
        assert_that(send_message_batch_result[1], has_item('SendMessageBatchResultEntry'))
        assert_that(send_message_batch_result[2], has_item('SendMessageBatchResultEntry'))

        time.sleep(3)

        self._read_messages_and_assert(
            created_queue_url, 10, visibility_timeout=1000, matcher=ReadResponseMatcher().with_message_ids([
                send_message_batch_result[0]['SendMessageBatchResultEntry']['MessageId'],
                send_message_batch_result[2]['SendMessageBatchResultEntry']['MessageId'],
            ])
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_validates_message_body(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        created_queue_url = self._create_queue_and_assert(self.queue_name)

        def call_send():
            self._sqs_api.send_message(
                created_queue_url, 'invalid body: \x02'
            )

        assert_that(
            call_send,
            raises(
                RuntimeError,
                pattern='InvalidParameterValue'
            )
        )

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_validates_message_attribute_value(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        created_queue_url = self._create_queue_and_assert(self.queue_name)

        def call_send():
            self._sqs_api.send_message(
                created_queue_url, 'body', attributes=[SqsMessageAttribute('invalid', 'String', 'invaid value: \x1F')]
            )

        assert_that(
            call_send,
            raises(
                RuntimeError,
                pattern='InvalidParameterValue'
            )
        )


class TestYandexAttributesPrefix(KikimrSqsTestBase):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestYandexAttributesPrefix, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['allow_yandex_attribute_prefix'] = True
        return config_generator

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_allows_yandex_message_attribute_prefix(self, tables_format):
        self._init_with_params(tables_format=tables_format)
        created_queue_url = self._create_queue_and_assert(self.queue_name)

        self._sqs_api.send_message(
            created_queue_url, 'msg_body', attributes={
                SqsMessageAttribute('Ya.attr', 'String', 'a'),
                SqsMessageAttribute('YC.attr', 'String', 'b'),
                SqsMessageAttribute('Yandex.attr', 'String', 'c'),
            }
        )


class TestSqsGenericMessagingWithTenant(get_test_with_sqs_tenant_installation(SqsGenericMessagingTest)):
    pass


class TestSqsGenericMessagingWithPath(get_test_with_sqs_installation_by_path(SqsGenericMessagingTest)):
    pass
