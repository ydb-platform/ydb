#!/usr/bin/env python
# -*- coding: utf-8 -*-

import botocore

from hamcrest import assert_that, raises

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicStartMessageMoveTask(KikimrSqsTopicTestBase):
    def test_start_message_move_task(self):
        queue_name = self._make_queue_name('start_message_move_task')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']
        queue_arn = self._boto_client.get_queue_attributes(
            QueueUrl=self._queue_url,
            AttributeNames=['QueueArn'],
        )['Attributes']['QueueArn']

        def start_message_move_task():
            self._boto_client.start_message_move_task(
                SourceArn=queue_arn,
            )

        assert_that(
            start_message_move_task,
            raises(
                botocore.exceptions.ClientError,
                pattern='InvalidAction',
            ),
        )
