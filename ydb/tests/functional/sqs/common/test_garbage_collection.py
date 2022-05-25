#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time
import multiprocessing
import random

import pytest
from hamcrest import assert_that, equal_to, less_than_or_equal_to

from ydb.tests.library.sqs.requests_client import SqsHttpApi

from ydb.tests.library.sqs.matchers import ReadResponseMatcher

from ydb.tests.library.sqs.test_base import to_bytes
from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, VISIBILITY_CHANGE_METHOD_PARAMS, IS_FIFO_PARAMS, TABLES_FORMAT_PARAMS


def send_message(server, username, queue_url, sqs_port, body, seq_no, group_id):
    is_fifo = to_bytes(queue_url).endswith(to_bytes('.fifo'))
    api = SqsHttpApi(
        server,
        sqs_port,
        username,
        raise_on_error=True,
        timeout=None
    )
    api.send_message(
        queue_url,
        body,
        group_id=group_id if is_fifo else None,
        deduplication_id=seq_no if is_fifo else None)


def send_message_pack(args):
    server, username, queue_url, sqs_port, body, seq_no, group_id = args
    send_message(server, username, queue_url, sqs_port, body, seq_no, group_id)


def delete_message(server, username, queue_url, sqs_port, receipt_handle):
    api = SqsHttpApi(
        server,
        sqs_port,
        username,
        raise_on_error=True,
        timeout=None
    )
    api.delete_message(to_bytes(queue_url), receipt_handle)


def delete_message_pack(args):
    server, username, queue_url, sqs_port, receipt_handle = args
    delete_message(server, username, queue_url, sqs_port, receipt_handle)


class TestSqsGarbageCollection(KikimrSqsTestBase):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestSqsGarbageCollection, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['min_message_retention_period_ms'] = 3000
        config_generator.yaml_config['sqs_config']['cleanup_batch_size'] = 100
        config_generator.yaml_config['sqs_config']['cleanup_period_ms'] = 2000
        config_generator.yaml_config['sqs_config']['deduplication_period_ms'] = 2000
        config_generator.yaml_config['sqs_config']['groups_read_attempt_ids_period_ms'] = 2000
        return config_generator

    def fill_queue_with_messages(self, number_of_mesages_to_write, is_fifo, processes=None, random_groups_max_count=50):
        args = [
            (
                self.cluster.nodes[1].host,
                self._username,
                self.queue_url,
                self.cluster_nodes[0].sqs_port,
                self._msg_body_template.format(i),
                self.seq_no + i,
                'group_{}'.format(random.randint(1, random_groups_max_count))
            ) for i in range(number_of_mesages_to_write)
        ]
        self.seq_no += number_of_mesages_to_write
        processes_to_write = processes
        if processes_to_write is None:
            processes_to_write = 50
        pool = multiprocessing.Pool(processes=processes_to_write)
        pool.map(send_message_pack, args)

    def delete_messages(self, queue_url, receipt_handles):
        args = [
            (
                self.cluster.nodes[1].host,
                self._username,
                self.queue_url,
                self.cluster_nodes[0].sqs_port,
                receipt_handle
            ) for receipt_handle in receipt_handles
        ]
        pool = multiprocessing.Pool(processes=50)
        pool.map(delete_message_pack, args)

    def wait_fifo_table_empty(self, table_name):
        queue_version = self._get_queue_version_number(self._username, self.queue_name)
        table_path = self._smart_make_table_path(self._username, self.queue_name, queue_version, None, table_name)
        attempts = 150
        while attempts:
            attempts -= 1

            lines_count = self._get_table_lines_count(table_path)
            logging.debug('Received "{}" table rows count: {}'.format(table_name, lines_count))
            if lines_count > 0 and attempts:
                time.sleep(0.2)
                continue

            assert_that(lines_count, equal_to(0))
            break

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_removes_messages_by_retention_time(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)

        number_of_mesages_to_write = 110 if is_fifo else 220
        processes = 10 if is_fifo else 30
        self.fill_queue_with_messages(number_of_mesages_to_write, is_fifo, processes=processes)
        time_after_send = time.time()

        attempts = 150
        while attempts:
            attempts -= 1
            attributes = self._sqs_api.get_queue_attributes(self.queue_url)
            number_of_messages = int(attributes['ApproximateNumberOfMessages'])

            if abs(number_of_messages - number_of_mesages_to_write) > processes and attempts:
                time.sleep(0.2)
                continue

            assert_that(
                abs(number_of_messages - number_of_mesages_to_write),
                less_than_or_equal_to(processes)
            )

        # Read a message. It will trigger moving a batch of messages from one shard to infly table (for std queue).
        self._read_messages_and_assert(
            self.queue_url, 1, ReadResponseMatcher().with_n_messages(1)
        )

        retention = 3
        self._sqs_api.set_queue_attributes(self.queue_url, {'MessageRetentionPeriod': str(retention)})
        assert_that(
            self._sqs_api.get_queue_attributes(self.queue_url)['MessageRetentionPeriod'],
            equal_to(str(retention))
        )

        now = time.time()
        if now < time_after_send + retention:
            time.sleep(time_after_send + retention - now)

        number_of_messages = None
        for i in range(100):
            attributes = self._sqs_api.get_queue_attributes(self.queue_url)
            number_of_messages = int(attributes['ApproximateNumberOfMessages'])
            if number_of_messages == 0:
                break
            else:
                time.sleep(0.5)

        assert_that(number_of_messages, equal_to(0))
        self._check_queue_tables_are_empty()

    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_cleanups_deduplication_table(self, tables_format):
        self._init_with_params(is_fifo=True, tables_format=tables_format)
        self._create_queue_and_assert(self.queue_name, is_fifo=True)

        # do the same again to ensure that this process will not stop
        for i in range(2):
            self.fill_queue_with_messages(150, True)
            time.sleep(2)
            self.wait_fifo_table_empty('Deduplication')

    @pytest.mark.parametrize('random_groups_max_count', [30, 200])
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_cleanups_reads_table(self, random_groups_max_count, tables_format):
        self._init_with_params(is_fifo=True, tables_format=tables_format)
        self._create_queue_and_assert(self.queue_name, is_fifo=True)

        # do the same again to ensure that this process will not stop
        for i in range(2):
            self.fill_queue_with_messages(150, True, processes=None, random_groups_max_count=random_groups_max_count)

            read_result = self._read_messages_and_assert(self.queue_url, 500, matcher=ReadResponseMatcher().with_n_or_more_messages(1), visibility_timeout=1000, wait_timeout=1)

            time.sleep(2)
            self.wait_fifo_table_empty('Reads')

            # delete received messages
            self.delete_messages(self.queue_url, [r['ReceiptHandle'] for r in read_result])

    @pytest.mark.parametrize(**VISIBILITY_CHANGE_METHOD_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_visibility_change_cleanups_proper_receive_attempt_id(self, delete_message, tables_format):
        self._init_with_params(is_fifo=True, tables_format=tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=True)
        groups_count = 5
        for group_number in range(groups_count):
            self._send_messages(queue_url, 1, is_fifo=True, group_id='group_{}'.format(group_number))

        receive_attempt_id = 'my_attempt'
        read_result_1 = self._sqs_api.receive_message(queue_url, max_number_of_messages=1, visibility_timeout=1000, receive_request_attempt_id=receive_attempt_id)
        assert_that(len(read_result_1), equal_to(1))
        receipt_handle = read_result_1[0]['ReceiptHandle']
        time.sleep(2)  # oversleep reads valid period
        time_before_read = time.time()
        read_result_2 = self._sqs_api.receive_message(queue_url, max_number_of_messages=10, visibility_timeout=1000, receive_request_attempt_id=receive_attempt_id)
        if read_result_2 is None:
            read_result_2 = []
        assert_that(len(read_result_1) + len(read_result_2), less_than_or_equal_to(groups_count))
        if delete_message:
            self._sqs_api.delete_message(queue_url, receipt_handle)
        else:
            self._sqs_api.change_message_visibility(queue_url, receipt_handle, visibility_timeout=1500)
        read_result_3 = self._sqs_api.receive_message(queue_url, max_number_of_messages=10, visibility_timeout=1000, receive_request_attempt_id=receive_attempt_id)
        time_after_read = time.time()
        if read_result_3 is None:
            read_result_3 = []

        if read_result_2 and (time_after_read - time_before_read) <= (self.config_generator.yaml_config['sqs_config']['groups_read_attempt_ids_period_ms'] / 1000):
            message_set_2 = set([res['MessageId'] for res in read_result_2])
            message_set_3 = set([res['MessageId'] for res in read_result_3])
            assert_that(len(message_set_2 & message_set_3), equal_to(len(message_set_2)))
