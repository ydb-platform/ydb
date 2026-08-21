#!/usr/bin/env python
# -*- coding: utf-8 -*-
from urllib.parse import urlsplit

from hamcrest import assert_that, equal_to, has_item, has_length, is_not, not_none

from ydb.tests.library.topic_sqs.test_base import KikimrSqsTopicTestBase


class TestSqsTopicRequestEndpoint(KikimrSqsTopicTestBase):
    def test_queue_lifecycle_uses_request_endpoint(self):
        queue_name = self._make_queue_name('queue_lifecycle_uses_request_endpoint')
        request_host = 'sqs-queue-lifecycle.test'
        message_body = 'hello from custom request endpoint'

        with self._boto_client_with_request_host(request_host) as (origin, client):
            created_url = client.create_queue(QueueName=queue_name)['QueueUrl']
            self._queue_url = created_url

            created_path = urlsplit(created_url).path
            assert_that(created_url, equal_to(origin + created_path))
            assert_that(created_path.startswith('/v1/'), equal_to(True))

            get_url = client.get_queue_url(QueueName=queue_name)['QueueUrl']
            assert_that(get_url, equal_to(created_url))

            send_response = client.send_message(
                QueueUrl=created_url,
                MessageBody=message_body,
            )
            assert_that(send_response['MessageId'], not_none())

            receive_response = client.receive_message(
                QueueUrl=created_url,
                WaitTimeSeconds=20,
                MaxNumberOfMessages=1,
            )
            messages = receive_response.get('Messages')
            assert_that(messages, not_none())
            assert_that(messages, has_length(1))
            assert_that(messages[0]['Body'], equal_to(message_body))
            assert_that(messages[0]['ReceiptHandle'], not_none())

            client.delete_message(
                QueueUrl=created_url,
                ReceiptHandle=messages[0]['ReceiptHandle'],
            )

        assert_that(
            self._get_consumer_uncommitted_messages_count(queue_name),
            equal_to(0),
        )

    def test_queue_urls_use_forwarded_host(self):
        queue_name = self._make_queue_name('queue_urls_use_forwarded_host')
        public_host = 'lbkx.example.net:8443'

        with self._boto_client_with_forwarded_host(public_host) as (origin, client):
            created_url = client.create_queue(QueueName=queue_name)['QueueUrl']
            self._queue_url = created_url

            created_path = urlsplit(created_url).path
            assert_that(created_url, equal_to(origin + created_path))
            assert_that(created_url.startswith('https://'), equal_to(True))

            get_url = client.get_queue_url(QueueName=queue_name)['QueueUrl']
            assert_that(get_url, equal_to(created_url))

            listed = client.list_queues(QueueNamePrefix=queue_name).get('QueueUrls', [])
            assert_that(listed, has_item(created_url))

        default_url = self._boto_client.get_queue_url(QueueName=queue_name)['QueueUrl']
        assert_that(default_url, is_not(equal_to(created_url)))
        assert_that(urlsplit(default_url).path, equal_to(created_path))
        assert_that(default_url.startswith('https://'), equal_to(False))
