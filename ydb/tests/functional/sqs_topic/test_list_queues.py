#!/usr/bin/env python
# -*- coding: utf-8 -*-
import uuid

from hamcrest import assert_that, has_item, not_none

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicListQueues(KikimrSqsTopicTestBase):
    def test_list_queues(self):
        queue_name = self._make_queue_name('list_queues')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        assert_that(self._queue_url, not_none())

        response = self._boto_client.list_queues()
        queue_urls = response.get('QueueUrls', [])

        assert_that(queue_urls, has_item(self._queue_url))

    def test_list_queues_in_directory(self):
        unique_suffix = uuid.uuid1()
        directory = 'directory_{}'.format(unique_suffix)
        topic_name = 'topic_{}'.format(unique_suffix)
        queue_name = '{}/{}'.format(directory, topic_name)

        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        assert_that(self._queue_url, not_none())

        response = self._boto_client.list_queues()
        queue_urls = response.get('QueueUrls', [])

        assert_that(queue_urls, has_item(self._queue_url))

    def test_list_queues_with_custom_consumer(self):
        topic_name = self._make_queue_name('list_queues_with_custom_consumer')
        consumer_name = 'consumer_{}'.format(uuid.uuid1())
        queue_name = '{}@{}'.format(topic_name, consumer_name)

        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        assert_that(self._queue_url, not_none())

        response = self._boto_client.list_queues()
        queue_urls = response.get('QueueUrls', [])

        assert_that(queue_urls, has_item(self._queue_url))

    def test_list_queues_multiple_queues(self):
        queue_name_1 = self._make_queue_name('list_queues_multiple_1')
        queue_name_2 = self._make_queue_name('list_queues_multiple_2')

        queue_url_1 = self._boto_client.create_queue(QueueName=queue_name_1)['QueueUrl']
        queue_url_2 = self._boto_client.create_queue(QueueName=queue_name_2)['QueueUrl']
        self._queue_url = queue_url_2

        response = self._boto_client.list_queues()
        queue_urls = response.get('QueueUrls', [])

        assert_that(queue_urls, has_item(queue_url_1))
        assert_that(queue_urls, has_item(queue_url_2))

        self._boto_client.delete_queue(QueueUrl=queue_url_1)
