#!/usr/bin/env python
# -*- coding: utf-8 -*-

from hamcrest import assert_that, equal_to, has_length

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicPurgeQueue(KikimrSqsTopicTestBase):
    def test_purge_queue(self):
        queue_name = self._make_queue_name('purge_queue')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody='hello from sqs',
        )

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(1),
        )

        self._boto_client.purge_queue(QueueUrl=self._queue_url)

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=1,
            MaxNumberOfMessages=1,
        )

        assert_that(response.get('Messages', []), has_length(0))
        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(0),
        )

    def test_purge_queue_fifo_queue(self):
        queue_name = self._create_fifo_queue('purge_queue_fifo_queue')

        self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody='hello from fifo sqs',
            MessageGroupId='message-group-1',
        )

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(1),
        )

        self._boto_client.purge_queue(QueueUrl=self._queue_url)

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=1,
            MaxNumberOfMessages=1,
        )

        assert_that(response.get('Messages', []), has_length(0))
        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(0),
        )
