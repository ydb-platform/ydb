#!/usr/bin/env python
# -*- coding: utf-8 -*-

import botocore

from hamcrest import assert_that, raises

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicListMessageMoveTasks(KikimrSqsTopicTestBase):
    def test_list_message_move_tasks(self):
        queue_name = self._make_queue_name('list_message_move_tasks')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']
        queue_arn = self._boto_client.get_queue_attributes(
            QueueUrl=self._queue_url,
            AttributeNames=['QueueArn'],
        )['Attributes']['QueueArn']

        def list_message_move_tasks():
            self._boto_client.list_message_move_tasks(
                SourceArn=queue_arn,
            )

        assert_that(
            list_message_move_tasks,
            raises(
                botocore.exceptions.ClientError,
                pattern='InvalidAction',
            ),
        )
