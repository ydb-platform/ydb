#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time
import threading

import pytest
from hamcrest import assert_that, equal_to, not_none, raises, not_

from ydb.tests.library.common.types import Erasure

from ydb.tests.library.sqs.matchers import ReadResponseMatcher

from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, STOP_NODE_PARAMS, IS_FIFO_PARAMS, TABLES_FORMAT_PARAMS


class TestSqsMultinodeCluster(KikimrSqsTestBase):
    erasure = Erasure.BLOCK_4_2
    use_in_memory_pdisks = False

    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestSqsMultinodeCluster, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['masters_describer_update_time_ms'] = 1000
        config_generator.yaml_config['sqs_config']['background_metrics_update_time_ms'] = 1000
        return config_generator

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_sqs_writes_through_proxy_on_each_node(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)
        self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)
        message_ids = []
        for i in range(self.cluster_nodes_count * 3):
            if is_fifo:
                group_id = str(i)
                seq_no = self.seq_no
                self.seq_no += 1
            else:
                group_id = None
                seq_no = None
            if i:
                time.sleep(0.3)  # sleep such a time that node caches master info in the second request, but doesn't do so in the third one
            node_index = i % self.cluster_nodes_count
            result = self._sqs_apis[node_index].send_message(self.queue_url, self._msg_body_template.format(next(self.counter)), deduplication_id=seq_no, group_id=group_id)
            assert_that(
                result, not_none()
            )
            logging.info('Message with id {} is sent to queue through proxy {}:{}'.format(result, self.server_fqdn, self.sqs_ports[node_index]))
            message_ids.append(result)

        self._read_messages_and_assert(
            self.queue_url, 50,
            ReadResponseMatcher().with_message_ids(message_ids),
            wait_timeout=1,
            visibility_timeout=1000
        )

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**STOP_NODE_PARAMS)
    def test_has_messages_counters(self, is_fifo, stop_node):
        self._init_with_params(is_fifo, tables_format=0)
        self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)
        node_index = self._get_queue_master_node_index()
        logging.debug('Master node for queue "{}" is {}'.format(self.queue_name, node_index))

        # send one message
        if is_fifo:
            group_id = "1"
            seq_no = self.seq_no
            self.seq_no += 1
        else:
            group_id = None
            seq_no = None
        message_id = self._sqs_api.send_message(self.queue_url, self._msg_body_template.format(next(self.counter)), deduplication_id=seq_no, group_id=group_id)
        assert_that(
            message_id, not_none()
        )

        def get_messages_counters(node_index):
            counters = self._get_sqs_counters(node_index)

            labels = {
                'subsystem': 'core',
                'user': self._username,
                'queue': self.queue_name,
            }

            labels['sensor'] = 'MessagesCount'
            messages = self._get_counter(counters, labels)

            labels['sensor'] = 'InflyMessagesCount'
            infly_messages = self._get_counter(counters, labels)

            labels['sensor'] = 'OldestMessageAgeSeconds'
            age = self._get_counter(counters, labels)

            return (messages, infly_messages, age)

        def check_master_node_counters(node_index):
            attempts = 100
            while attempts:
                attempts -= 1
                messages, infly_messages, age = get_messages_counters(node_index)

                if messages is None or messages['value'] != 1 or not is_fifo and infly_messages is None or not is_fifo and infly_messages['value'] != 0 or age is None or age['value'] == 0:
                    assert attempts, 'No attempts left to see right counter values for queue'
                    time.sleep(0.5)
                else:
                    break

        check_master_node_counters(node_index)

        def check_no_message_counters(node_index):
            attempts = 100
            while attempts:
                attempts -= 1
                messages, infly_messages, age = get_messages_counters(node_index)

                if messages is not None or infly_messages is not None or age is not None:
                    assert attempts, 'No attempts left to see no counter values for queue'
                    time.sleep(0.5)
                else:
                    break

        check_no_message_counters(self._other_node(node_index))

        # remove master from node
        if stop_node:
            self.cluster.nodes[node_index + 1].stop()  # nodes indices are one-based
            logging.debug('Killed node {}'.format(node_index + 1))
        else:
            self._kick_tablets_from_node(node_index)

        new_node_index = self._get_queue_master_node_index()
        logging.debug('Previous master node: {}. New master node: {}'.format(node_index, new_node_index))
        assert_that(new_node_index, not_(equal_to(node_index)))

        if not stop_node:
            check_no_message_counters(node_index)

        logging.debug('New master node for queue "{}" is {}'.format(self.queue_name, new_node_index))
        check_master_node_counters(new_node_index)

    @pytest.mark.parametrize(**STOP_NODE_PARAMS)
    def test_reassign_master(self, stop_node):
        self._init_with_params(tables_format=0)
        self._create_queue_and_assert(self.queue_name)
        node_index = self._get_queue_master_node_index()
        proxy_node_index = self._other_node(node_index)
        assert_that(proxy_node_index, not_(equal_to(node_index)))
        result = self._sqs_apis[proxy_node_index].send_message(self.queue_url, self._msg_body_template.format(next(self.counter)))
        assert_that(
            result, not_none()
        )

        # remove master from node
        if stop_node:
            self.cluster.nodes[node_index + 1].stop()  # nodes indices are one-based
            logging.debug('Killed node {}'.format(node_index + 1))
        else:
            self._kick_tablets_from_node(node_index)

        retries = 50
        while retries:
            retries -= 1
            time.sleep(0.5)
            try:
                result = self._sqs_apis[proxy_node_index].send_message(self.queue_url, self._msg_body_template.format(next(self.counter)))
                assert_that(
                    result, not_none()
                )
                break
            except RuntimeError:
                continue

    def _run_receive_message(self):
        logging.debug('_run_receive_message started')
        node_index = self._get_queue_master_node_index()
        proxy_node_index = self._other_node(node_index)

        def call_receive():
            self._sqs_apis[proxy_node_index].receive_message(
                self.queue_url, max_number_of_messages=10,
                visibility_timeout=1000, wait_timeout=20
            )

        assert_that(
            call_receive,
            raises(RuntimeError, pattern='failed with status 50.*\n.*Queue leader session error.')
        )
        logging.debug('_run_receive_message finished')
        self.receive_message_finished = True

    def test_ends_request_after_kill(self):
        self._init_with_params(tables_format=0)
        self._create_queue_and_assert(self.queue_name)
        node_index = self._get_queue_master_node_index()
        self.receive_message_finished = False
        thread = threading.Thread(target=self._run_receive_message)
        thread.start()
        time.sleep(3)
        self.cluster.nodes[node_index + 1].stop()  # nodes indices are one-based
        thread.join()
        assert_that(
            self.receive_message_finished,
            equal_to(
                True
            )
        )
