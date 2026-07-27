#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import botocore
import time

from hamcrest import assert_that, equal_to, has_item, has_length, not_none, raises

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicDeleteMessage(KikimrSqsTopicTestBase):
    @staticmethod
    def _make_receipt_handle(partition, offset):
        def encode_varint(value):
            parts = []
            while value > 0x7f:
                parts.append((value & 0x7f) | 0x80)
                value >>= 7
            parts.append(value)
            return bytes(parts)

        proto = bytes([8]) + encode_varint(partition) + bytes([16]) + encode_varint(offset)
        return base64.b64encode(proto).decode('ascii')

    def _send_and_receive_message(self, queue_name, message_body='hello from sqs', visibility_timeout=None):
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message_body,
        )

        receive_kwargs = {
            'QueueUrl': self._queue_url,
            'WaitTimeSeconds': 20,
            'MaxNumberOfMessages': 1,
        }
        if visibility_timeout is not None:
            receive_kwargs['VisibilityTimeout'] = visibility_timeout

        response = self._boto_client.receive_message(**receive_kwargs)

        messages = response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages, has_length(1))
        assert_that(messages[0]['Body'], equal_to(message_body))
        return messages[0]['ReceiptHandle']

    def test_delete_message_invalid_receipt_handle(self):
        queue_name = self._make_queue_name('delete_message_invalid_receipt_handle')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        def delete_message_with_invalid_handle():
            self._boto_client.delete_message(
                QueueUrl=self._queue_url,
                ReceiptHandle='not_a_receipt_handle',
            )

        assert_that(
            delete_message_with_invalid_handle,
            raises(
                botocore.exceptions.ClientError,
                pattern='ReceiptHandleIsInvalid',
            ),
        )

    def test_delete_message_malformed_receipt_handle(self):
        queue_name = self._make_queue_name('delete_message_malformed_receipt_handle')
        receipt_handle = self._send_and_receive_message(queue_name)

        def delete_message_with_malformed_handle():
            self._boto_client.delete_message(
                QueueUrl=self._queue_url,
                ReceiptHandle=receipt_handle[:-1],
            )

        assert_that(
            delete_message_with_malformed_handle,
            raises(
                botocore.exceptions.ClientError,
                pattern='ReceiptHandleIsInvalid',
            ),
        )

    def test_delete_message_success(self):
        queue_name = self._make_queue_name('delete_message_success')
        receipt_handle = self._send_and_receive_message(queue_name)

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(1),
        )

        response = self._boto_client.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=receipt_handle,
        )

        assert_that(response, not_none())
        assert_that(response, has_item('ResponseMetadata'))

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(0),
        )

    def test_delete_message_already_deleted(self):
        queue_name = self._make_queue_name('delete_message_already_deleted')
        receipt_handle = self._send_and_receive_message(queue_name)

        self._boto_client.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=receipt_handle,
        )

        response = self._boto_client.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=receipt_handle,
        )

        assert_that(response, not_none())
        assert_that(response, has_item('ResponseMetadata'))

    def test_delete_message_not_found(self):
        queue_name = self._make_queue_name('delete_message_not_found')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        response = self._boto_client.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=self._make_receipt_handle(partition=0, offset=999999),
        )

        assert_that(response, not_none())
        assert_that(response, has_item('ResponseMetadata'))

    def test_delete_message_after_visibility_timeout_succeeds(self):
        queue_name = self._make_queue_name('delete_message_after_visibility_timeout')
        receipt_handle = self._send_and_receive_message(
            queue_name,
            visibility_timeout=1,
        )

        time.sleep(2)

        response = self._boto_client.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=receipt_handle,
        )

        assert_that(response, not_none())
        assert_that(response, has_item('ResponseMetadata'))
        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(0),
        )

    def test_delete_message_fifo_queue(self):
        queue_name = self._create_fifo_queue('delete_message_fifo_queue')

        message_body = 'hello from fifo sqs'
        self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message_body,
            MessageGroupId='message-group-1',
        )

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(1),
        )

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1,
        )

        messages = response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages, has_length(1))
        assert_that(messages[0]['Body'], equal_to(message_body))
        receipt_handle = messages[0]['ReceiptHandle']

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(1),
        )

        self._boto_client.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=receipt_handle,
        )

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(0),
        )

    def test_delete_message_batch_fifo_queue(self):
        queue_name = self._create_fifo_queue('delete_message_batch_fifo_queue')

        message_bodies = ['message-0', 'message-1']
        for index, message_body in enumerate(message_bodies):
            self._boto_client.send_message(
                QueueUrl=self._queue_url,
                MessageBody=message_body,
                MessageGroupId='message-group-{}'.format(index),
            )

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(len(message_bodies)),
        )

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=len(message_bodies),
        )

        messages = response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages, has_length(len(message_bodies)))

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(len(message_bodies)),
        )

        batch_response = self._boto_client.delete_message_batch(
            QueueUrl=self._queue_url,
            Entries=[
                {
                    'Id': str(index),
                    'ReceiptHandle': message['ReceiptHandle'],
                }
                for index, message in enumerate(messages)
            ],
        )

        assert_that(batch_response['Successful'], has_length(len(message_bodies)))
        assert_that(batch_response.get('Failed', []), has_length(0))

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(0),
        )
