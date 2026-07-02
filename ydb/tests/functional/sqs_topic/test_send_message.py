#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time

from hamcrest import assert_that, equal_to, not_none

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicSendMessage(KikimrSqsTopicTestBase):
    def test_send_message(self):
        queue_name = self._make_queue_name('send_message')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        message_body = 'hello from sqs'
        response = self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message_body,
        )

        assert_that(response['MessageId'], not_none())

        message = self._read_message_from_topic_without_consumer(queue_name)
        assert_that(message.data.decode('utf-8'), equal_to(message_body))

        message = self._read_message_from_topic_without_consumer(queue_name)
        assert_that(message.data.decode('utf-8'), equal_to(message_body))

    def test_send_message_fifo_queue(self):
        queue_name = self._create_fifo_queue('send_message_fifo_queue')

        message_body = 'hello from fifo sqs'
        response = self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message_body,
            MessageGroupId='message-group-1',
        )

        assert_that(response['MessageId'], not_none())

        message = self._read_message_from_topic_without_consumer(queue_name)
        assert_that(message.data.decode('utf-8'), equal_to(message_body))

    def test_send_message_with_delay_seconds(self):
        queue_name = self._make_queue_name('send_message_with_delay_seconds')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        message_body = 'delayed message'
        self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message_body,
            DelaySeconds=3,
        )

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=1,
            MaxNumberOfMessages=1,
        )
        assert_that(response.get('Messages'), equal_to(None))

        time.sleep(3)

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1,
        )

        messages = response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages[0]['Body'], equal_to(message_body))

    def test_send_message_fifo_with_message_deduplication_id(self):
        self._create_fifo_queue('send_message_fifo_with_message_deduplication_id')

        message_body = 'hello from fifo sqs'
        deduplication_id = 'deduplication-id-1'
        response = self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message_body,
            MessageGroupId='message-group-1',
            MessageDeduplicationId=deduplication_id,
        )

        assert_that(response['MessageId'], not_none())
        assert_that(response['SequenceNumber'], not_none())

        duplicate_response = self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message_body,
            MessageGroupId='message-group-1',
            MessageDeduplicationId=deduplication_id,
        )

        assert_that(duplicate_response['MessageId'], equal_to(response['MessageId']))
