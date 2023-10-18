#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ydb.tests.library.sqs.test_base import KikimrSqsTestBase


class TestSqsGettingCounters(KikimrSqsTestBase):

    def test_counters_when_sending_reading_deleting(self):
        queue_url = self._create_queue_and_assert(self.queue_name, False, True)

        message_payload = "foobar"
        self._sqs_api.send_message(queue_url, message_payload)
        handle = self._read_while_not_empty(queue_url, 1)[0]["ReceiptHandle"]
        self._sqs_api.delete_message(queue_url, handle)

        sqs_counters = self._get_sqs_counters()

        send_message_count = self._get_counter_value(sqs_counters, {
            'queue': self.queue_name,
            'sensor': 'SendMessage_Count',
        })
        assert send_message_count == 1

        send_message_bytes_written = self._get_counter_value(sqs_counters, {
            'queue': self.queue_name,
            'sensor': 'SendMessage_BytesWritten',
        })
        assert send_message_bytes_written == len(message_payload)

        message_receive_attempts = self._get_counter(sqs_counters, {
            'queue': self.queue_name,
            'sensor': 'MessageReceiveAttempts',
        })
        assert message_receive_attempts['hist']['buckets'][0] == 1

        receive_message_count = self._get_counter_value(sqs_counters, {
            'queue': self.queue_name,
            'sensor': 'ReceiveMessage_Count',
        })
        assert receive_message_count == 1

        receive_message_bytes_read = self._get_counter_value(sqs_counters, {
            'queue': self.queue_name,
            'sensor': 'ReceiveMessage_BytesRead',
        })
        assert receive_message_bytes_read > 0

        delete_message_count = self._get_counter_value(sqs_counters, {
            'queue': self.queue_name,
            'sensor': 'DeleteMessage_Count',
        })
        assert delete_message_count == 1

    def test_counters_when_sending_duplicates(self):
        fifo_queue_name = self.queue_name + ".fifo"
        queue_url = self._create_queue_and_assert(queue_name=fifo_queue_name, is_fifo=True, use_http=True)

        for _ in range(2):
            self._sqs_api.send_message(
                queue_url=queue_url,
                message_body="foobar",
                deduplication_id="deduplication_id",
                group_id="group_id",
            )

        sqs_counters = self._get_sqs_counters()
        send_message_deduplication_count = self._get_counter_value(sqs_counters, {
            'queue': fifo_queue_name,
            'sensor': 'SendMessage_DeduplicationCount',
        })

        assert send_message_deduplication_count == 1

    def test_counters_when_reading_from_empty_queue(self):
        queue_url = self._create_queue_and_assert(self.queue_name, False, True)
        self._read_while_not_empty(queue_url, 1)

        sqs_counters = self._get_sqs_counters()
        receive_message_empty_count = self._get_counter_value(sqs_counters, {
            'queue': self.queue_name,
            'sensor': 'ReceiveMessage_EmptyCount',
        })
        assert receive_message_empty_count == 1
