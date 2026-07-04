#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base64
import time

from hamcrest import assert_that, equal_to, has_item, has_length, not_none

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicDeleteMessageBatch(KikimrSqsTopicTestBase):
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

    def test_delete_message_batch_invalid_receipt_handle(self):
        queue_name = self._make_queue_name('delete_message_batch_invalid_receipt_handle')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        batch_response = self._boto_client.delete_message_batch(
            QueueUrl=self._queue_url,
            Entries=[
                {
                    'Id': '0',
                    'ReceiptHandle': 'not_a_receipt_handle',
                },
            ],
        )

        assert_that(batch_response.get('Successful', []), has_length(0))
        assert_that(batch_response['Failed'], has_length(1))
        assert_that(batch_response['Failed'][0]['Id'], equal_to('0'))
        assert_that(batch_response['Failed'][0]['Code'], equal_to('ReceiptHandleIsInvalid'))

    def test_delete_message_batch_malformed_receipt_handle(self):
        queue_name = self._make_queue_name('delete_message_batch_malformed_receipt_handle')
        receipt_handle = self._send_and_receive_message(queue_name)

        batch_response = self._boto_client.delete_message_batch(
            QueueUrl=self._queue_url,
            Entries=[
                {
                    'Id': '0',
                    'ReceiptHandle': receipt_handle[:-1],
                },
            ],
        )

        assert_that(batch_response.get('Successful', []), has_length(0))
        assert_that(batch_response['Failed'], has_length(1))
        assert_that(batch_response['Failed'][0]['Id'], equal_to('0'))
        assert_that(batch_response['Failed'][0]['Code'], equal_to('ReceiptHandleIsInvalid'))

    def test_delete_message_batch(self):
        queue_name = self._make_queue_name('delete_message_batch')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        message_bodies = ['message-0', 'message-1']
        for message_body in message_bodies:
            self._boto_client.send_message(
                QueueUrl=self._queue_url,
                MessageBody=message_body,
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

        assert_that(batch_response, not_none())
        assert_that(batch_response, has_item('ResponseMetadata'))
        assert_that(batch_response['Successful'], has_length(len(message_bodies)))
        assert_that(batch_response.get('Failed', []), has_length(0))

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(0),
        )

    def test_delete_message_batch_already_deleted(self):
        queue_name = self._make_queue_name('delete_message_batch_already_deleted')
        receipt_handle = self._send_and_receive_message(queue_name)

        self._boto_client.delete_message(
            QueueUrl=self._queue_url,
            ReceiptHandle=receipt_handle,
        )

        batch_response = self._boto_client.delete_message_batch(
            QueueUrl=self._queue_url,
            Entries=[
                {
                    'Id': '0',
                    'ReceiptHandle': receipt_handle,
                },
            ],
        )

        assert_that(batch_response.get('Successful', []), has_length(0))
        assert_that(batch_response['Failed'], has_length(1))
        assert_that(batch_response['Failed'][0]['Id'], equal_to('0'))
        assert_that(batch_response['Failed'][0]['Code'], equal_to('InvalidParameterValue'))

    def test_delete_message_batch_not_found(self):
        queue_name = self._make_queue_name('delete_message_batch_not_found')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        batch_response = self._boto_client.delete_message_batch(
            QueueUrl=self._queue_url,
            Entries=[
                {
                    'Id': '0',
                    'ReceiptHandle': self._make_receipt_handle(partition=0, offset=999999),
                },
            ],
        )

        assert_that(batch_response.get('Successful', []), has_length(0))
        assert_that(batch_response['Failed'], has_length(1))
        assert_that(batch_response['Failed'][0]['Id'], equal_to('0'))
        assert_that(batch_response['Failed'][0]['Code'], equal_to('InvalidParameterValue'))

    def test_delete_message_batch_mixed_valid_and_invalid(self):
        queue_name = self._make_queue_name('delete_message_batch_mixed_valid_and_invalid')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        message_bodies = ['message-0', 'message-1']
        for message_body in message_bodies:
            self._boto_client.send_message(
                QueueUrl=self._queue_url,
                MessageBody=message_body,
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

        batch_response = self._boto_client.delete_message_batch(
            QueueUrl=self._queue_url,
            Entries=[
                {
                    'Id': '0',
                    'ReceiptHandle': messages[0]['ReceiptHandle'],
                },
                {
                    'Id': '1',
                    'ReceiptHandle': 'not_a_receipt_handle',
                },
            ],
        )

        assert_that(batch_response['Successful'], has_length(1))
        assert_that(batch_response['Successful'][0]['Id'], equal_to('0'))
        assert_that(batch_response['Failed'], has_length(1))
        assert_that(batch_response['Failed'][0]['Id'], equal_to('1'))
        assert_that(batch_response['Failed'][0]['Code'], equal_to('ReceiptHandleIsInvalid'))

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(1),
        )

    def test_delete_message_batch_after_visibility_timeout_succeeds(self):
        queue_name = self._make_queue_name('delete_message_batch_after_visibility_timeout')
        receipt_handle = self._send_and_receive_message(
            queue_name,
            visibility_timeout=1,
        )

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(1),
        )

        time.sleep(2)

        batch_response = self._boto_client.delete_message_batch(
            QueueUrl=self._queue_url,
            Entries=[
                {
                    'Id': '0',
                    'ReceiptHandle': receipt_handle,
                },
            ],
        )

        assert_that(batch_response, not_none())
        assert_that(batch_response, has_item('ResponseMetadata'))
        assert_that(batch_response['Successful'], has_length(1))
        assert_that(batch_response.get('Failed', []), has_length(0))

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(0),
        )
