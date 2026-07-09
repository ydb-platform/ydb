#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time

from hamcrest import assert_that, contains_string, equal_to, has_entries

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicGetQueueAttributes(KikimrSqsTopicTestBase):
    EXPECTED_ATTRIBUTE_NAMES = {
        'ApproximateNumberOfMessages',
        'ApproximateNumberOfMessagesDelayed',
        'ApproximateNumberOfMessagesNotVisible',
        'CreatedTimestamp',
        'LastModifiedTimestamp',
        'DelaySeconds',
        'MaximumMessageSize',
        'MessageRetentionPeriod',
        'ReceiveMessageWaitTimeSeconds',
        'VisibilityTimeout',
        'FifoQueue',
        'ContentBasedDeduplication',
        'QueueArn',
    }

    EXPECTED_ATTRIBUTE_VALUES = {
        'ApproximateNumberOfMessages': '0',
        'ApproximateNumberOfMessagesDelayed': '0',
        'ApproximateNumberOfMessagesNotVisible': '0',
        'DelaySeconds': '0',
        'MaximumMessageSize': '262144',
        'MessageRetentionPeriod': '86400',
        'ReceiveMessageWaitTimeSeconds': '0',
        'VisibilityTimeout': '30',
        'FifoQueue': 'false',
        'ContentBasedDeduplication': 'false',
    }

    def test_get_queue_attributes(self):
        queue_name = self._make_queue_name('get_queue_attributes')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        response = self._boto_client.get_queue_attributes(
            QueueUrl=self._queue_url,
            AttributeNames=['All'],
        )
        attributes = response['Attributes']

        assert_that(set(attributes.keys()), equal_to(self.EXPECTED_ATTRIBUTE_NAMES))
        assert_that(attributes, has_entries(self.EXPECTED_ATTRIBUTE_VALUES))
        assert_that(attributes['CreatedTimestamp'], equal_to(attributes['LastModifiedTimestamp']))
        assert_that(attributes['QueueArn'], contains_string(queue_name))

    def test_get_queue_attributes_approximate_number_of_messages_delayed(self):
        queue_name = self._make_queue_name('get_queue_attributes_delayed')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        attributes = self._boto_client.get_queue_attributes(
            QueueUrl=self._queue_url,
            AttributeNames=['ApproximateNumberOfMessagesDelayed'],
        )['Attributes']
        assert_that(int(attributes['ApproximateNumberOfMessagesDelayed']), equal_to(0))

        self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody='delayed message',
            DelaySeconds=60,
        )

        attempts = 10
        while attempts:
            attempts -= 1
            attributes = self._boto_client.get_queue_attributes(
                QueueUrl=self._queue_url,
                AttributeNames=['ApproximateNumberOfMessagesDelayed'],
            )['Attributes']
            delayed_count = int(attributes['ApproximateNumberOfMessagesDelayed'])
            if delayed_count != 1 and attempts:
                time.sleep(0.5)
                continue
            assert_that(delayed_count, equal_to(1))
            break

    EXPECTED_FIFO_ATTRIBUTE_VALUES = {
        'ApproximateNumberOfMessages': '0',
        'ApproximateNumberOfMessagesDelayed': '0',
        'ApproximateNumberOfMessagesNotVisible': '0',
        'DelaySeconds': '0',
        'MaximumMessageSize': '262144',
        'MessageRetentionPeriod': '86400',
        'ReceiveMessageWaitTimeSeconds': '0',
        'VisibilityTimeout': '30',
        'FifoQueue': 'true',
        'ContentBasedDeduplication': 'false',
    }

    def test_get_queue_attributes_fifo_queue(self):
        queue_name = self._create_fifo_queue('get_queue_attributes_fifo_queue')

        response = self._boto_client.get_queue_attributes(
            QueueUrl=self._queue_url,
            AttributeNames=['All'],
        )
        attributes = response['Attributes']

        assert_that(set(attributes.keys()), equal_to(self.EXPECTED_ATTRIBUTE_NAMES))
        assert_that(attributes, has_entries(self.EXPECTED_FIFO_ATTRIBUTE_VALUES))
        assert_that(attributes['CreatedTimestamp'], equal_to(attributes['LastModifiedTimestamp']))
        assert_that(attributes['QueueArn'], contains_string(queue_name))
