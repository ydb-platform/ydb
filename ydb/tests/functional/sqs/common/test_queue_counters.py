#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ydb.tests.library.sqs.test_base import KikimrSqsTestBase

from ydb.tests.library.sqs.requests_client import SqsSendMessageParams


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

        message_recide_duration_buckets = self._get_counter(sqs_counters, {
            'queue': self.queue_name,
            'sensor': 'MessageReside_Duration',
        })['hist']['buckets']
        assert any(map(lambda x: x > 0, message_recide_duration_buckets))

        delete_message_count = self._get_counter_value(sqs_counters, {
            'queue': self.queue_name,
            'sensor': 'DeleteMessage_Count',
        })
        assert delete_message_count == 1

        client_message_processing_duration_buckets = self._get_counter(sqs_counters, {
            'queue': self.queue_name,
            'sensor': 'ClientMessageProcessing_Duration',
        })['hist']['buckets']
        assert any(map(lambda x: x > 0, client_message_processing_duration_buckets))

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

    def test_sqs_action_counters(self):
        queue_url = self._create_queue_and_assert(self.queue_name, False, True)
        message_payload = "foobar"
        self._sqs_api.send_message(queue_url, message_payload)
        self._read_while_not_empty(queue_url, 1)

        sqs_counters = self._get_sqs_counters()

        successes = self._get_counter_value(sqs_counters, {
            'queue': self.queue_name,
            'sensor': 'ReceiveMessage_Success',
        })
        assert successes == 1

        durations = self._get_counter(sqs_counters, {
            'queue': self.queue_name,
            'sensor': 'ReceiveMessage_Duration',
        })
        duration_buckets = durations['hist']['buckets']
        assert any(map(lambda x: x > 0, duration_buckets))

        working_durations = self._get_counter(sqs_counters, {
            'queue': self.queue_name,
            'sensor': 'ReceiveMessage_WorkingDuration',
        })
        working_duration_buckets = working_durations['hist']['buckets']
        assert any(map(lambda x: x > 0, working_duration_buckets))

    def test_receive_message_immediate_duration_counter(self):
        queue_url = self._create_queue_and_assert(self.queue_name, False, True)

        # ReceiveMessageImmediate_Duration doesn't happen on every receive_message, so we need to read enough messages
        for i in range(100):
            message_payload = "foobar" + str(i)
            self._sqs_api.send_message(queue_url, message_payload)
            self._read_while_not_empty(queue_url, 1)

        sqs_counters = self._get_sqs_counters()

        receive_message_immediate_duration_buckets = self._get_counter(sqs_counters, {
            'queue': 'total',
            'sensor': 'ReceiveMessageImmediate_Duration',
        })['hist']['buckets']
        assert any(map(lambda x: x > 0, receive_message_immediate_duration_buckets))

    def test_purge_queue_counters(self):
        queue_url = self._create_queue_and_assert(self.queue_name, False, True)

        for _ in range(20):
            self._sqs_api.send_message(queue_url, "foobar")
            self._sqs_api.purge_queue(queue_url)

        sqs_counters = self._get_sqs_counters()

        purged_derivative = self._get_counter_value(sqs_counters, {
            'queue': self.queue_name,
            'sensor': 'MessagesPurged',
        })
        assert purged_derivative > 0

    def test_action_duration_being_not_immediate(self):
        queue_url = self._create_queue_and_assert(self.queue_name, False, True)

        for i in range(100):
            message_payload = "foobar" + str(i)
            self._sqs_api.send_message(queue_url, message_payload)
            self._read_while_not_empty(queue_url, 1)

        sqs_counters = self._get_sqs_counters()

        durations = self._get_counter(sqs_counters, {
            'queue': self.queue_name,
            'sensor': 'ReceiveMessage_Duration',
        })
        buckets_longer_than_5ms = durations['hist']['buckets'][1:]
        assert any(map(lambda x: x > 0, buckets_longer_than_5ms))

    def test_receive_attempts_are_counted_separately_for_messages_in_one_batch(self):
        queue_url = self._create_queue_and_assert(self.queue_name, False, True)
        self._sqs_api.send_message_batch(queue_url, [SqsSendMessageParams('data0'), SqsSendMessageParams('data1')])
        self._read_while_not_empty(queue_url, 2)

        sqs_counters = self._get_sqs_counters()
        message_receive_attempts = self._get_counter(sqs_counters, {
            'queue': self.queue_name,
            'sensor': 'MessageReceiveAttempts',
        })
        assert message_receive_attempts['hist']['buckets'][0] == 2
