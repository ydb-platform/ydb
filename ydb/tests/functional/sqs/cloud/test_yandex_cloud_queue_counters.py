#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ydb.tests.library.sqs.test_base import get_test_with_sqs_tenant_installation
from ydb.tests.library.sqs.cloud_test_base import YandexCloudSqsTestBase


class TestYmqQueueCounters(get_test_with_sqs_tenant_installation(YandexCloudSqsTestBase)):
    def test_ymq_send_read_delete(self):
        self._sqs_api = self._create_api_for_user(self._username, raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)

        queue_url = self._sqs_api.create_queue(self.queue_name)
        queue_resource_id = self._get_queue_resource_id(queue_url, self.queue_name)

        message_payload = "foobar"

        self._sqs_api.send_message(queue_url, message_payload)
        handle = self._read_while_not_empty(queue_url, 1)[0]["ReceiptHandle"]
        self._sqs_api.delete_message(queue_url, handle)

        def assert_counters(ymq_counters):
            send_message_count = self._get_counter_value(ymq_counters, {
                'queue': queue_resource_id,
                'name': 'queue.messages.sent_count_per_second',
            })
            assert send_message_count == 1

            send_message_bytes_written = self._get_counter_value(ymq_counters, {
                'queue': queue_resource_id,
                'name': 'queue.messages.sent_bytes_per_second',
            })
            assert send_message_bytes_written == len(message_payload)

            message_receive_attempts = self._get_counter(ymq_counters, {
                'queue': queue_resource_id,
                'name': 'queue.messages.receive_attempts_count_rate',
            })
            assert message_receive_attempts['hist']['buckets'][0] == 1

            receive_message_count = self._get_counter_value(ymq_counters, {
                'queue': queue_resource_id,
                'name': 'queue.messages.received_count_per_second',
            })
            assert receive_message_count == 1

            recide_duration_buckets = self._get_counter(ymq_counters, {
                'queue': queue_resource_id,
                'name': 'queue.messages.reside_duration_milliseconds',
            })['hist']['buckets']
            assert any(map(lambda x: x > 0, recide_duration_buckets))

            receive_message_bytes_read = self._get_counter_value(ymq_counters, {
                'queue': queue_resource_id,
                'name': 'queue.messages.received_bytes_per_second',
            })
            assert receive_message_bytes_read > 0

            client_message_processing_duration_buckets = self._get_counter(ymq_counters, {
                'queue': queue_resource_id,
                'name': 'queue.messages.client_processing_duration_milliseconds',
            })['hist']['buckets']
            assert any(map(lambda x: x > 0, client_message_processing_duration_buckets))

        self._wait_for_ymq_counters(assert_counters, self.cloud_id, self.folder_id)

    def test_counters_when_sending_duplicates(self):
        self._sqs_api = self._create_api_for_user(self._username, raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)

        fifo_queue_name = self.queue_name + ".fifo"
        queue_url = self._sqs_api.create_queue(queue_name=fifo_queue_name, is_fifo=True)
        queue_resource_id = self._get_queue_resource_id(queue_url, fifo_queue_name)

        for _ in range(2):
            self._sqs_api.send_message(
                queue_url=queue_url,
                message_body="foobar",
                deduplication_id="deduplication_id",
                group_id="group_id",
            )

        def assert_counters(ymq_counters):
            send_message_deduplication_count = self._get_counter_value(ymq_counters, {
                'queue': queue_resource_id,
                'name': 'queue.messages.deduplicated_count_per_second',
            })
            assert send_message_deduplication_count == 1

        self._wait_for_ymq_counters(assert_counters, self.cloud_id, self.folder_id)

    def test_counters_when_reading_from_empty_queue(self):
        self._sqs_api = self._create_api_for_user(self._username, raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)

        queue_url = self._sqs_api.create_queue(self.queue_name)
        queue_resource_id = self._get_queue_resource_id(queue_url, self.queue_name)

        self._read_while_not_empty(queue_url, 1)

        def assert_counters(ymq_counters):
            receive_message_empty_count = self._get_counter_value(ymq_counters, {
                'queue': queue_resource_id,
                'name': 'queue.messages.empty_receive_attempts_count_per_second',
            })
            assert receive_message_empty_count == 1

        self._wait_for_ymq_counters(assert_counters, self.cloud_id, self.folder_id)

    def test_sqs_action_counters(self):

        self._sqs_api = self._create_api_for_user(self._username, raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)

        queue_url = self._sqs_api.create_queue(self.queue_name)
        queue_resource_id = self._get_queue_resource_id(queue_url, self.queue_name)

        message_payload = "foobar"

        self._sqs_api.send_message(queue_url, message_payload)
        self._read_while_not_empty(queue_url, 1)

        def assert_counters(ymq_counters):
            successes = self._get_counter_value(ymq_counters, {
                'queue': queue_resource_id,
                'method': 'receive_message',
                'name': 'api.http.requests_count_per_second',
            })
            assert successes == 1

            durations = self._get_counter(ymq_counters, {
                'queue': queue_resource_id,
                'method': 'receive_message',
                'name': 'api.http.request_duration_milliseconds',
            })
            duration_buckets = durations['hist']['buckets']
            assert any(map(lambda x: x > 0, duration_buckets))

        self._wait_for_ymq_counters(assert_counters, self.cloud_id, self.folder_id)

    def test_purge_queue_counters(self):

        self._sqs_api = self._create_api_for_user(self._username, raise_on_error=True, force_private=True, iam_token=self.iam_token, folder_id=self.folder_id)

        queue_url = self._sqs_api.create_queue(self.queue_name)
        queue_resource_id = self._get_queue_resource_id(queue_url, self.queue_name)

        for _ in range(20):
            self._sqs_api.send_message(queue_url, "foobar")
            self._sqs_api.purge_queue(queue_url)

        def assert_counters(ymq_counters):
            purged_derivative = self._get_counter_value(ymq_counters, {
                'queue': queue_resource_id,
                'name': 'queue.messages.purged_count_per_second',
            })
            assert purged_derivative > 0

        self._wait_for_ymq_counters(assert_counters, self.cloud_id, self.folder_id)
