#!/usr/bin/env python
# -*- coding: utf-8 -*-
import uuid

import botocore

from hamcrest import assert_that, equal_to, raises

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicGetQueueUrl(KikimrSqsTopicTestBase):
    def test_get_queue_url(self):
        queue_name = self._make_queue_name('get_queue_url')
        create_response = self._boto_client.create_queue(QueueName=queue_name)
        self._queue_url = create_response['QueueUrl']

        get_response = self._boto_client.get_queue_url(QueueName=queue_name)

        assert_that(get_response['QueueUrl'], equal_to(create_response['QueueUrl']))

    def test_get_queue_url_fifo_queue(self):
        queue_name = self._create_fifo_queue('get_queue_url_fifo_queue')
        create_response = {'QueueUrl': self._queue_url}

        get_response = self._boto_client.get_queue_url(QueueName=queue_name)

        assert_that(get_response['QueueUrl'], equal_to(create_response['QueueUrl']))

    def test_get_queue_url_in_directory(self):
        unique_suffix = uuid.uuid1()
        directory = 'directory_{}'.format(unique_suffix)
        topic_name = 'topic_{}'.format(unique_suffix)
        queue_name = '{}/{}'.format(directory, topic_name)

        create_response = self._boto_client.create_queue(QueueName=queue_name)
        self._queue_url = create_response['QueueUrl']

        get_response = self._boto_client.get_queue_url(QueueName=queue_name)

        assert_that(get_response['QueueUrl'], equal_to(create_response['QueueUrl']))

    def test_get_queue_url_in_directory_with_consumer(self):
        unique_suffix = uuid.uuid1()
        directory = 'directory_{}'.format(unique_suffix)
        topic_name = 'topic_{}'.format(unique_suffix)
        consumer_name = 'consumer_{}'.format(unique_suffix)
        queue_name = '{}/{}@{}'.format(directory, topic_name, consumer_name)

        create_response = self._boto_client.create_queue(QueueName=queue_name)
        self._queue_url = create_response['QueueUrl']

        get_response = self._boto_client.get_queue_url(QueueName=queue_name)

        assert_that(get_response['QueueUrl'], equal_to(create_response['QueueUrl']))

    def test_get_queue_url_same_topic_different_consumers(self):
        topic_name = self._make_queue_name('get_queue_url_same_topic_different_consumers')
        queue_name_1 = '{}@consumer_1'.format(topic_name)
        queue_name_2 = '{}@consumer_2'.format(topic_name)

        create_response_1 = self._boto_client.create_queue(QueueName=queue_name_1)
        create_response_2 = self._boto_client.create_queue(QueueName=queue_name_2)
        self._queue_url = create_response_2['QueueUrl']

        get_response_1 = self._boto_client.get_queue_url(QueueName=queue_name_1)
        get_response_2 = self._boto_client.get_queue_url(QueueName=queue_name_2)

        assert_that(get_response_1['QueueUrl'], equal_to(create_response_1['QueueUrl']))
        assert_that(get_response_2['QueueUrl'], equal_to(create_response_2['QueueUrl']))

        self._boto_client.delete_queue(QueueUrl=create_response_1['QueueUrl'])

    def test_get_queue_url_existing_queue(self):
        queue_name = self._make_queue_name('get_queue_url_existing_queue')

        create_response_1 = self._boto_client.create_queue(QueueName=queue_name)
        create_response_2 = self._boto_client.create_queue(QueueName=queue_name)
        self._queue_url = create_response_2['QueueUrl']

        get_response = self._boto_client.get_queue_url(QueueName=queue_name)

        assert_that(create_response_2['QueueUrl'], equal_to(create_response_1['QueueUrl']))
        assert_that(get_response['QueueUrl'], equal_to(create_response_1['QueueUrl']))

    def test_get_queue_url_with_at_in_name(self):
        topic_name = self._make_queue_name('get_queue_url_with_at_in_name').replace('queue_', 'que@ue_', 1)
        queue_name = '{}@ydb-sqs-consumer'.format(topic_name)

        def get_queue_url():
            self._boto_client.get_queue_url(QueueName=queue_name)

        assert_that(
            get_queue_url,
            raises(
                botocore.exceptions.ClientError,
                pattern='InvalidParameterValue',
            ),
        )
