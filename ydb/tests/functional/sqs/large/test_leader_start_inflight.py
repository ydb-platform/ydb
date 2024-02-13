#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time

import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, IS_FIFO_PARAMS, TABLES_FORMAT_PARAMS


class TestSqsMultinodeCluster(KikimrSqsTestBase):
    erasure = Erasure.BLOCK_4_2
    use_in_memory_pdisks = False

    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestSqsMultinodeCluster, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['masters_describer_update_time_ms'] = 1000
        config_generator.yaml_config['sqs_config']['background_metrics_update_time_ms'] = 1000
        config_generator.yaml_config['sqs_config']['start_local_leader_inflight_max'] = 1
        config_generator.yaml_config['sqs_config']['account_settings_defaults'] = {'max_queues_count': 50000}
        return config_generator

    def get_leaders_per_nodes(self):
        nodes = len(self.cluster.nodes)
        leaders = []
        for node_index in range(nodes):
            counters = self._get_counters(node_index, 'utils', counters_format='json', dump_to_log=False)
            labels = {
                'activity': 'SQS_QUEUE_LEADER_ACTOR',
                'sensor': 'ActorsAliveByActivity'
            }
            leader_actors = self._get_counter(counters, labels)
            leaders.append(leader_actors['value'] if leader_actors else 0)
        return leaders

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_limit_leader_start_inflight(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)

        queues = []
        for i in range(20):
            queues.append(self._create_queue_and_assert(f'{i}_{self.queue_name}', is_fifo=is_fifo))

        def send_messages():
            for q in queues:
                self.seq_no += 1
                seq_no = self.seq_no if is_fifo else None
                group_id = 'group' if is_fifo else None
                self._send_message_and_assert(q, f'test_send_message for {q}', seq_no=seq_no, group_id=group_id)

        send_messages()

        while True:
            for node_index in range(len(self.cluster.nodes))[1:]:
                self._kick_tablets_from_node(node_index)
            leaders = self.get_leaders_per_nodes()
            logging.info(f'started leaders {leaders}, expected {len(queues)} only on node=0.')
            if sum(leaders) == len(queues) and leaders[0] == len(queues):
                break
            time.sleep(5)

        self._kick_tablets_from_node(0)
        self._enable_tablets_on_node(1)

        while True:
            leaders = self.get_leaders_per_nodes()
            logging.info(f'started leaders {leaders}, expected {len(queues)} only on node=1.')
            if sum(leaders) == len(queues) and leaders[1] == len(queues):
                break
            time.sleep(5)
        send_messages()
