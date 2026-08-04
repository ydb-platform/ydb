#!/usr/bin/env python
# -*- coding: utf-8 -*-

import botocore

from hamcrest import assert_that, raises

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicListQueueTags(KikimrSqsTopicTestBase):
    def test_list_queue_tags(self):
        queue_name = self._make_queue_name('list_queue_tags')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        def list_queue_tags():
            self._boto_client.list_queue_tags(
                QueueUrl=self._queue_url,
            )

        assert_that(
            list_queue_tags,
            raises(
                botocore.exceptions.ClientError,
                pattern='InvalidAction',
            ),
        )
