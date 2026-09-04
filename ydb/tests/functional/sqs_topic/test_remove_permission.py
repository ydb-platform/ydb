#!/usr/bin/env python
# -*- coding: utf-8 -*-

import botocore

from hamcrest import assert_that, raises

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicRemovePermission(KikimrSqsTopicTestBase):
    def test_remove_permission(self):
        queue_name = self._make_queue_name('remove_permission')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        def remove_permission():
            self._boto_client.remove_permission(
                QueueUrl=self._queue_url,
                Label='test-label',
            )

        assert_that(
            remove_permission,
            raises(
                botocore.exceptions.ClientError,
                pattern='InvalidAction',
            ),
        )
