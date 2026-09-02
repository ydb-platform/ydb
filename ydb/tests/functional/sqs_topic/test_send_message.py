#!/usr/bin/env python
# -*- coding: utf-8 -*-

import botocore

from hamcrest import assert_that, equal_to, not_, not_none, raises

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

    def test_send_message_fifo_queue(self):
        queue_name = self._create_fifo_queue('send_message_fifo_queue')

        message_body = 'hello from fifo sqs'
        response = self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message_body,
            MessageGroupId='message-group-1',
            MessageDeduplicationId='deduplication-id-1',
        )

        assert_that(response['MessageId'], not_none())

        message = self._read_message_from_topic_without_consumer(queue_name)
        assert_that(message.data.decode('utf-8'), equal_to(message_body))

    def test_send_message_with_delay_seconds(self):
        queue_name = self._make_queue_name('send_message_with_delay_seconds')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        message_body = 'delayed message'
        response = self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message_body,
            DelaySeconds=3,
        )

        # TODO: Per-message DelaySeconds does not delay delivery on receive. Fix it.
        assert_that(response['MessageId'], not_none())
        assert_that(response['MD5OfMessageBody'], not_none())

        receive_response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1,
        )

        messages = receive_response.get('Messages')
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

    def test_send_message_fifo_with_content_based_deduplication(self):
        queue_name = self._make_fifo_queue_name('send_message_fifo_with_content_based_deduplication')
        self._queue_url = self._boto_client.create_queue(
            QueueName=queue_name,
            Attributes={
                'FifoQueue': 'true',
                'ContentBasedDeduplication': 'true',
            },
        )['QueueUrl']

        message_body = 'hello from fifo sqs'
        response = self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message_body,
            MessageGroupId='message-group-1',
        )

        assert_that(response['MessageId'], not_none())
        assert_that(response['SequenceNumber'], not_none())

        # Same body without MessageDeduplicationId: SHA-256 of body is used, so duplicate is dropped.
        duplicate_response = self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message_body,
            MessageGroupId='message-group-1',
        )

        assert_that(duplicate_response['MessageId'], equal_to(response['MessageId']))

        # Different body produces a different content hash and is not deduplicated.
        other_response = self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody='other body',
            MessageGroupId='message-group-1',
        )

        assert_that(other_response['MessageId'], not_(equal_to(response['MessageId'])))

        # Explicit MessageDeduplicationId overrides content-based hash.
        explicit_response = self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody='yet another body',
            MessageGroupId='message-group-1',
            MessageDeduplicationId='explicit-deduplication-id',
        )
        explicit_duplicate_response = self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody='completely different body',
            MessageGroupId='message-group-1',
            MessageDeduplicationId='explicit-deduplication-id',
        )

        assert_that(explicit_duplicate_response['MessageId'], equal_to(explicit_response['MessageId']))

    def test_send_message_fifo_without_content_based_deduplication(self):
        self._create_fifo_queue('send_message_fifo_without_content_based_deduplication')

        def send_without_message_deduplication_id():
            self._boto_client.send_message(
                QueueUrl=self._queue_url,
                MessageBody='hello from fifo sqs',
                MessageGroupId='message-group-1',
            )

        # Without ContentBasedDeduplication, MessageDeduplicationId is required.
        assert_that(
            send_without_message_deduplication_id,
            raises(
                botocore.exceptions.ClientError,
                pattern='MissingParameter',
            ),
        )
