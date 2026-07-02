#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time

from hamcrest import assert_that, equal_to, has_length, not_none

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicChangeMessageVisibilityBatch(KikimrSqsTopicTestBase):
    def test_change_message_visibility_batch(self):
        queue_name = self._make_queue_name('change_message_visibility_batch')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        message_bodies = ['message-0', 'message-1']
        for message_body in message_bodies:
            self._boto_client.send_message(
                QueueUrl=self._queue_url,
                MessageBody=message_body,
            )

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=2,
            VisibilityTimeout=10,
        )

        messages = response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages, has_length(2))

        batch_response = self._boto_client.change_message_visibility_batch(
            QueueUrl=self._queue_url,
            Entries=[
                {
                    'Id': str(index),
                    'ReceiptHandle': message['ReceiptHandle'],
                    'VisibilityTimeout': 1,
                }
                for index, message in enumerate(messages)
            ],
        )

        assert_that(batch_response['Successful'], has_length(2))
        assert_that(batch_response.get('Failed', []), has_length(0))

        time.sleep(2)

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=2,
        )

        messages = response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages, has_length(2))
        received_bodies = sorted(message['Body'] for message in messages)
        assert_that(received_bodies, equal_to(sorted(message_bodies)))
