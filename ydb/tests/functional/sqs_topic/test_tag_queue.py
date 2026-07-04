#!/usr/bin/env python
# -*- coding: utf-8 -*-

import botocore

from hamcrest import assert_that, raises

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicTagQueue(KikimrSqsTopicTestBase):
    def test_tag_queue(self):
        queue_name = self._make_queue_name('tag_queue')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        def tag_queue():
            self._boto_client.tag_queue(
                QueueUrl=self._queue_url,
                Tags={
                    'key': 'value',
                },
            )

        assert_that(
            tag_queue,
            raises(
                botocore.exceptions.ClientError,
                pattern='InvalidAction',
            ),
        )
