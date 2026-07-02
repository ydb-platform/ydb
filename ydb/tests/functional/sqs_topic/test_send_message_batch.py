#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time

from hamcrest import assert_that, equal_to, has_length, not_none

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicSendMessageBatch(KikimrSqsTopicTestBase):
    def test_send_message_batch(self):
        queue_name = self._make_queue_name('send_message_batch')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        message_bodies = ['message-0', 'message-1', 'message-2']
        response = self._boto_client.send_message_batch(
            QueueUrl=self._queue_url,
            Entries=[
                {
                    'Id': str(index),
                    'MessageBody': message_body,
                }
                for index, message_body in enumerate(message_bodies)
            ],
        )

        assert_that(response['Successful'], has_length(len(message_bodies)))
        for entry in response['Successful']:
            assert_that(entry['MessageId'], not_none())

        messages = self._read_messages_from_topic_without_consumer(queue_name, len(message_bodies))
        received_bodies = [message.data.decode('utf-8') for message in messages]
        assert_that(received_bodies, equal_to(message_bodies))

    def test_send_message_batch_with_delay_seconds(self):
        queue_name = self._make_queue_name('send_message_batch_with_delay_seconds')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        message_bodies = ['message-0', 'message-1']
        response = self._boto_client.send_message_batch(
            QueueUrl=self._queue_url,
            Entries=[
                {
                    'Id': str(index),
                    'MessageBody': message_body,
                    'DelaySeconds': 3,
                }
                for index, message_body in enumerate(message_bodies)
            ],
        )

        assert_that(response['Successful'], has_length(len(message_bodies)))

        receive_response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=1,
            MaxNumberOfMessages=len(message_bodies),
        )
        assert_that(receive_response.get('Messages'), equal_to(None))

        time.sleep(3)

        receive_response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=len(message_bodies),
        )

        messages = receive_response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages, has_length(len(message_bodies)))
        received_bodies = sorted(message['Body'] for message in messages)
        assert_that(received_bodies, equal_to(sorted(message_bodies)))

    def test_send_message_batch_fifo_queue(self):
        queue_name = self._create_fifo_queue('send_message_batch_fifo_queue')

        message_bodies = ['message-0', 'message-1', 'message-2']
        response = self._boto_client.send_message_batch(
            QueueUrl=self._queue_url,
            Entries=[
                {
                    'Id': str(index),
                    'MessageBody': message_body,
                    'MessageGroupId': 'message-group-1',
                }
                for index, message_body in enumerate(message_bodies)
            ],
        )

        assert_that(response['Successful'], has_length(len(message_bodies)))
        for entry in response['Successful']:
            assert_that(entry['MessageId'], not_none())

        messages = self._read_messages_from_topic_without_consumer(queue_name, len(message_bodies))
        received_bodies = [message.data.decode('utf-8') for message in messages]
        assert_that(received_bodies, equal_to(message_bodies))
