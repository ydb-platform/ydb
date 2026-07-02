#!/usr/bin/env python
# -*- coding: utf-8 -*-
import uuid

import botocore

from hamcrest import assert_that, not_none, contains_string, all_of, raises, equal_to, is_not

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicCreateQueue(KikimrSqsTopicTestBase):
    def test_create_queue(self):
        queue_name = self._make_queue_name('create_queue')
        response = self._boto_client.create_queue(QueueName=queue_name)
        self._queue_url = response['QueueUrl']

        assert_that(self._queue_url, not_none())
        assert_that(
            self._queue_url,
            all_of(
                contains_string(queue_name),
                contains_string('ydb-sqs-consumer'),
            ),
        )
        self._assert_topic_exists(
            '{}/{}'.format(self.database, queue_name),
            expected_name=queue_name,
        )

    def test_create_queue_in_directory(self):
        unique_suffix = uuid.uuid1()
        directory = 'directory_{}'.format(unique_suffix)
        topic_name = 'topic_{}'.format(unique_suffix)
        queue_name = '{}/{}'.format(directory, topic_name)

        response = self._boto_client.create_queue(QueueName=queue_name)
        self._queue_url = response['QueueUrl']

        assert_that(self._queue_url, not_none())
        assert_that(
            self._queue_url,
            all_of(
                contains_string(directory),
                contains_string(topic_name),
                contains_string('ydb-sqs-consumer'),
            ),
        )
        directory_path = '{}/{}'.format(self.database, directory)
        self._assert_topic_exists(
            '{}/{}'.format(directory_path, topic_name),
            expected_name=topic_name,
            parent_directory=directory_path,
        )

    def test_create_queue_in_directory_with_consumer(self):
        unique_suffix = uuid.uuid1()
        directory = 'directory_{}'.format(unique_suffix)
        topic_name = 'topic_{}'.format(unique_suffix)
        consumer_name = 'consumer_{}'.format(unique_suffix)
        queue_name = '{}/{}@{}'.format(directory, topic_name, consumer_name)

        response = self._boto_client.create_queue(QueueName=queue_name)
        self._queue_url = response['QueueUrl']

        assert_that(self._queue_url, not_none())
        assert_that(
            self._queue_url,
            all_of(
                contains_string(directory),
                contains_string(topic_name),
                contains_string(consumer_name),
            ),
        )

    def test_create_queue_same_topic_different_consumers(self):
        topic_name = self._make_queue_name('same_topic_different_consumers')
        queue_name_1 = '{}@consumer_1'.format(topic_name)
        queue_name_2 = '{}@consumer_2'.format(topic_name)

        queue_url_1 = self._boto_client.create_queue(QueueName=queue_name_1)['QueueUrl']
        queue_url_2 = self._boto_client.create_queue(QueueName=queue_name_2)['QueueUrl']

        assert_that(queue_url_1, not_none())
        assert_that(queue_url_2, not_none())
        assert_that(queue_url_1, is_not(equal_to(queue_url_2)))
        assert_that(
            queue_url_1,
            all_of(
                contains_string(topic_name),
                contains_string('consumer_1'),
            ),
        )
        assert_that(
            queue_url_2,
            all_of(
                contains_string(topic_name),
                contains_string('consumer_2'),
            ),
        )

        self._boto_client.delete_queue(QueueUrl=queue_url_1)
        self._boto_client.delete_queue(QueueUrl=queue_url_2)

    def test_create_existing_queue(self):
        queue_name = self._make_queue_name('create_existing_queue')

        queue_url_1 = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']
        queue_url_2 = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']
        self._queue_url = queue_url_2

        assert_that(queue_url_1, not_none())
        assert_that(queue_url_2, equal_to(queue_url_1))

    def test_create_queue_with_at_in_name(self):
        topic_name = self._make_queue_name('create_queue_with_at_in_name').replace('queue_', 'que@ue_', 1)
        queue_name = '{}@ydb-sqs-consumer'.format(topic_name)

        def create_queue():
            self._boto_client.create_queue(QueueName=queue_name)

        assert_that(
            create_queue,
            raises(
                botocore.exceptions.ClientError,
                pattern='InvalidParameterValue',
            ),
        )
