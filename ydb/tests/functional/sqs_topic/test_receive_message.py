#!/usr/bin/env python
# -*- coding: utf-8 -*-

from hamcrest import assert_that, equal_to, has_length, not_none

from ydb.tests.library.sqs_topic.test_base import KikimrSqsTopicTestBase


class TestSqsTopicReceiveMessage(KikimrSqsTopicTestBase):
    def test_receive_message(self):
        queue_name = self._make_queue_name('receive_message')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        message_body = 'hello from sqs'
        self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message_body,
        )

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1,
        )

        messages = response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages, has_length(1))
        assert_that(messages[0]['Body'], equal_to(message_body))

    def test_receive_message_fifo_queue(self):
        queue_name = self._create_fifo_queue('receive_message_fifo_queue')

        message_body = 'hello from fifo sqs'
        self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message_body,
            MessageGroupId='message-group-1',
        )

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1,
            AttributeNames=['All'],
        )

        messages = response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages, has_length(1))
        assert_that(messages[0]['Body'], equal_to(message_body))
        assert_that(
            messages[0]['Attributes']['MessageGroupId'],
            equal_to('message-group-1'),
        )

    def test_receive_message_with_max_number_of_messages(self):
        queue_name = self._make_queue_name('receive_message_with_max_number_of_messages')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        message_bodies = [f'message-{i}' for i in range(5)]
        for message_body in message_bodies:
            self._boto_client.send_message(
                QueueUrl=self._queue_url,
                MessageBody=message_body,
            )

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=3,
        )

        messages = response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages, has_length(3))
        received_bodies = {message['Body'] for message in messages}
        assert_that(received_bodies.issubset(set(message_bodies)), equal_to(True))

    def test_receive_message_with_message_group_id(self):
        queue_name = self._make_queue_name('receive_message_with_message_group_id')
        self._queue_url = self._boto_client.create_queue(QueueName=queue_name)['QueueUrl']

        message_body = 'hello from sqs'
        message_group_id = 'message-group-1'
        self._boto_client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=message_body,
            MessageGroupId=message_group_id,
        )

        response = self._boto_client.receive_message(
            QueueUrl=self._queue_url,
            WaitTimeSeconds=20,
            MaxNumberOfMessages=1,
            AttributeNames=['All'],
        )

        messages = response.get('Messages')
        assert_that(messages, not_none())
        assert_that(messages, has_length(1))
        assert_that(messages[0]['Body'], equal_to(message_body))
        assert_that(messages[0]['Attributes']['MessageGroupId'], equal_to(message_group_id))

    def test_receive_message_with_string_message_attribute(self):
        attribute_name = 'string_attr'
        attribute_value = {
            'DataType': 'String',
            'StringValue': 'string',
        }

        received_attribute = self._send_and_receive_message_attribute(
            'receive_message_with_string_message_attribute',
            {attribute_name: attribute_value},
            attribute_name,
        )

        assert_that(received_attribute['DataType'], equal_to('String'))
        assert_that(received_attribute['StringValue'], equal_to('string'))

    def test_receive_message_with_binary_message_attribute(self):
        attribute_name = 'binary_attr'
        binary_value = b'blob'
        attribute_value = {
            'DataType': 'Binary',
            'BinaryValue': binary_value,
        }

        received_attribute = self._send_and_receive_message_attribute(
            'receive_message_with_binary_message_attribute',
            {attribute_name: attribute_value},
            attribute_name,
        )

        assert_that(received_attribute['DataType'], equal_to('Binary'))
        assert_that(received_attribute['BinaryValue'], equal_to(binary_value))

    def test_receive_message_with_string_list_message_attribute(self):
        attribute_name = 'string_list_attr'
        string_list_values = ['first', 'second', 'third']
        attribute_value = {
            'DataType': 'String.Array',
            'StringListValues': string_list_values,
        }

        received_attribute = self._send_and_receive_message_attribute(
            'receive_message_with_string_list_message_attribute',
            {attribute_name: attribute_value},
            attribute_name,
        )

        assert_that(received_attribute['DataType'], equal_to('String.Array'))
        assert_that(received_attribute['StringListValues'], equal_to(string_list_values))

    def test_receive_message_with_binary_list_message_attribute(self):
        attribute_name = 'binary_list_attr'
        binary_list_values = [b'first-blob', b'second-blob', b'third-blob']
        attribute_value = {
            'DataType': 'Binary.Array',
            'BinaryListValues': binary_list_values,
        }

        received_attribute = self._send_and_receive_message_attribute(
            'receive_message_with_binary_list_message_attribute',
            {attribute_name: attribute_value},
            attribute_name,
        )

        assert_that(received_attribute['DataType'], equal_to('Binary.Array'))
        assert_that(received_attribute['BinaryListValues'], equal_to(binary_list_values))
