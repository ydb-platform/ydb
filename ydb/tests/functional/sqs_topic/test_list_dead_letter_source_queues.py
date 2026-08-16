#!/usr/bin/env python
# -*- coding: utf-8 -*-

import botocore

from hamcrest import assert_that, raises

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicListDeadLetterSourceQueues(KikimrSqsTopicTestBase):
    def test_list_dead_letter_source_queues(self):
        queue_name = self._make_queue_name('list_dead_letter_source_queues')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        def list_dead_letter_source_queues():
            self._boto_client.list_dead_letter_source_queues(
                QueueUrl=self._queue_url,
            )

        assert_that(
            list_dead_letter_source_queues,
            raises(
                botocore.exceptions.ClientError,
                pattern='InvalidAction',
            ),
        )
