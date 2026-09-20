#!/usr/bin/env python
# -*- coding: utf-8 -*-

import botocore

from hamcrest import assert_that, raises

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicUntagQueue(KikimrSqsTopicTestBase):
    def test_untag_queue(self):
        queue_name = self._make_queue_name('untag_queue')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        def untag_queue():
            self._boto_client.untag_queue(
                QueueUrl=self._queue_url,
                TagKeys=['key'],
            )

        assert_that(
            untag_queue,
            raises(
                botocore.exceptions.ClientError,
                pattern='InvalidAction',
            ),
        )
