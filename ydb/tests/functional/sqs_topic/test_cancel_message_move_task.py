#!/usr/bin/env python
# -*- coding: utf-8 -*-

import botocore

from hamcrest import assert_that, raises

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicCancelMessageMoveTask(KikimrSqsTopicTestBase):
    def test_cancel_message_move_task(self):
        def cancel_message_move_task():
            self._boto_client.cancel_message_move_task(
                TaskHandle='task-handle',
            )

        assert_that(
            cancel_message_move_task,
            raises(
                botocore.exceptions.ClientError,
                pattern='InvalidAction',
            ),
        )
