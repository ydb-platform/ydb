#!/usr/bin/env python
# -*- coding: utf-8 -*-

from hamcrest import assert_that, equal_to, has_length, not_none

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicDeleteMessageBatch(KikimrSqsTopicTestBase):
    def test_delete_message_batch(self):
        queue_name = self._make_queue_name('delete_message_batch')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        message_bodies = ['message-0', 'message-1']
        for message_body in message_bodies:
            self._boto_client.send_message(
                QueueUrl=self._queue_url,
                MessageBody=message_body,
            )

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(len(message_bodies)),
        )

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=len(message_bodies),
        )

        messages = response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages, has_length(len(message_bodies)))

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(len(message_bodies)),
        )

        batch_response = self._boto_client.delete_message_batch(
            QueueUrl=self._queue_url,
            Entries=[
                {
                    'Id': str(index),
                    'ReceiptHandle': message['ReceiptHandle'],
                }
                for index, message in enumerate(messages)
            ],
        )

        assert_that(batch_response['Successful'], has_length(len(message_bodies)))
        assert_that(batch_response.get('Failed', []), has_length(0))

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(0),
        )
