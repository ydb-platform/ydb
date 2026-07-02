#!/usr/bin/env python
# -*- coding: utf-8 -*-
import uuid

from hamcrest import assert_that, not_none

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicDeleteQueue(KikimrSqsTopicTestBase):
    def test_delete_queue(self):
        queue_name = self._make_queue_name('delete_queue')
        queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        assert_that(queue_url, not_none())

        topic_path = '{}/{}'.format(self.database, queue_name)
        self._assert_topic_exists(topic_path, expected_name=queue_name)

        self._boto_client.delete_queue(QueueUrl=queue_url)

        self._assert_topic_not_exists(topic_path)

    def test_delete_queue_with_consumer(self):
        unique_suffix = uuid.uuid1()
        topic_name = 'topic_{}'.format(unique_suffix)
        consumer_name = 'consumer_{}'.format(unique_suffix)
        queue_name = '{}@{}'.format(topic_name, consumer_name)

        queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        assert_that(queue_url, not_none())

        topic_path = '{}/{}'.format(self.database, topic_name)
        self._assert_topic_exists(topic_path, expected_name=topic_name)

        self._boto_client.delete_queue(QueueUrl=queue_url)

        self._assert_topic_not_exists(topic_path)
