#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time

from hamcrest import assert_that, equal_to, has_length, not_none

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicChangeMessageVisibility(KikimrSqsTopicTestBase):
    def test_change_message_visibility(self):
        queue_name = self._make_queue_name('change_message_visibility')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        message_body = 'hello from sqs'
        self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message_body,
        )

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1,
            VisibilityTimeout=600,
        )

        messages = response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages, has_length(1))
        assert_that(messages[0]['Body'], equal_to(message_body))
        receipt_handle = messages[0]['ReceiptHandle']

        self._boto_client.change_message_visibility(
            QueueUrl=self._queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=1,
        )

        time.sleep(2)

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1,
        )

        messages = response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages, has_length(1))
        assert_that(messages[0]['Body'], equal_to(message_body))

    def test_change_message_visibility_fifo_queue(self):
        self._create_fifo_queue('change_message_visibility_fifo_queue')

        message_body = 'hello from fifo sqs'
        self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message_body,
            MessageGroupId='message-group-1',
        )

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1,
            VisibilityTimeout=600,
        )

        messages = response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages, has_length(1))
        assert_that(messages[0]['Body'], equal_to(message_body))
        receipt_handle = messages[0]['ReceiptHandle']

        self._boto_client.change_message_visibility(
            QueueUrl=self._queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=1,
        )

        time.sleep(2)

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1,
        )

        messages = response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages, has_length(1))
        assert_that(messages[0]['Body'], equal_to(message_body))
