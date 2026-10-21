#!/usr/bin/env python
# -*- coding: utf-8 -*-

import botocore

from hamcrest import assert_that, raises

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicAddPermission(KikimrSqsTopicTestBase):
    def test_add_permission(self):
        queue_name = self._make_queue_name('add_permission')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        def add_permission():
            self._boto_client.add_permission(
                QueueUrl=self._queue_url,
                Label='test-label',
                AWSAccountIds=['123456789012'],
                Actions=['ReceiveMessage'],
            )

        assert_that(
            add_permission,
            raises(
                botocore.exceptions.ClientError,
                pattern='InvalidAction',
            ),
        )
