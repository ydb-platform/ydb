#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
