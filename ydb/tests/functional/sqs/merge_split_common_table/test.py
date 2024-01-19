#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import time

from ydb.tests.oss.ydb_sdk_import import ydb
import random
import string

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.sqs.test_base import KikimrSqsTestBase


def random_string(length):
    return ''.join([random.choice(string.ascii_lowercase) for _ in range(length)])


class TestSqsSplitMergeTables(KikimrSqsTestBase):
    erasure = Erasure.BLOCK_4_2
    use_in_memory_pdisks = False

    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestSqsSplitMergeTables, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['masters_describer_update_time_ms'] = 1000
        config_generator.yaml_config['sqs_config']['background_metrics_update_time_ms'] = 1000
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

    def wait_leader_started(self, queues_count):
        while True:
            leaders_per_node = self.get_leaders_per_nodes()
            logging.debug(f'wait all leaders {sum(leaders_per_node)} vs {queues_count} :  {leaders_per_node}')
            if sum(leaders_per_node) == queues_count:
                return
            time.sleep(1)

    def alter_table(self, table_path):
        logging.info(f'alter table {table_path}...')
        settings = ydb.RetrySettings()
        settings.max_retries = 1
        session = ydb.retry_operation_sync(lambda: self._driver.table_client.session().create(), retry_settings=settings)

        self.__column_to_force_split = 'column_for_tests'
        ydb.retry_operation_sync(lambda: session.alter_table(
            table_path,
            alter_partitioning_settings=ydb.PartitioningSettings()
                                           .with_min_partitions_count(1)
                                           .with_partition_size_mb(1)
                                           .with_partitioning_by_size(ydb.FeatureFlag.ENABLED)
                                           .with_partitioning_by_load(ydb.FeatureFlag.ENABLED),
            add_columns=(
                ydb.Column(
                    self.__column_to_force_split,
                    ydb.OptionalType(ydb.PrimitiveType.String),
                ),
            ),
        ))

    def get_lines_count(self, table_path):
        settings = ydb.RetrySettings()
        settings.max_retries = 1
        session = ydb.retry_operation_sync(lambda: self._driver.table_client.session().create(), retry_settings=settings)
        res = session.transaction().execute(f'select count(*) as rows_cnt FROM `{table_path}`', commit_tx=True)
        return res[0].rows[0]["rows_cnt"]

    def force_split(self, table_path):
        logging.info(f'force split {table_path}...')
        settings = ydb.RetrySettings()
        settings.max_retries = 1
        session = ydb.retry_operation_sync(lambda: self._driver.table_client.session().create(), retry_settings=settings)

        lines = self.get_lines_count(table_path)
        row_size = int(45 * 1024 * 1024 / lines)
        session.transaction().execute(f'update `{table_path}` SET {self.__column_to_force_split}="{random_string(row_size)}"', commit_tx=True)

    def get_nodes_with_leaders(self):
        leaders_per_node = self.get_leaders_per_nodes()
        return len(list(filter(bool, leaders_per_node)))

    def send_messages(self, is_fifo, queue_urls, messages_count=1, message_length=16):
        logging.info('starting to send messages...')
        group_id = 'group' if is_fifo else None
        for i in range(messages_count):
            for queue_url in queue_urls:
                self.seq_no += 1
                self._send_message_and_assert(queue_url, random_string(message_length), seq_no=self.seq_no if is_fifo else None, group_id=group_id)
        logging.info('messages have been sent.')

    def get_partitions(self, table_path):
        session = ydb.retry_operation_sync(lambda: self._driver.table_client.session().create())
        response = session.describe_table(
            table_path,
            ydb.DescribeTableSettings().with_include_table_stats(True)
        )
        return response.table_stats.partitions

    def run_test(self, is_fifo):
        self._init_with_params(is_fifo, tables_format=1)
        balancing_table_path = '/Root/SQS/.' + ('FIFO' if is_fifo else 'STD') + '/Messages'
        queues_count = 10
        queue_urls = []
        partitions = self.get_partitions(balancing_table_path)
        assert partitions > 1, 'incorrect initial partitions count'

        for index in range(queues_count):
            queue_name = f'q_{index}_{self.queue_name}'
            queue_urls.append(self._create_queue_and_assert(queue_name, is_fifo=is_fifo))
        self.wait_leader_started(queues_count)
        logging.info('all leaders started.')
        assert self.get_nodes_with_leaders() > 1

        self.send_messages(is_fifo, queue_urls)
        logging.info('messages have been sent #1')

        self.alter_table(balancing_table_path)

        while True:
            self.send_messages(is_fifo, queue_urls)
            leaders_per_node = self.get_leaders_per_nodes()
            nodes_with_leaders = len(list(filter(bool, leaders_per_node)))
            cur_partitions = self.get_partitions(balancing_table_path)
            logging.info(f'wait merge... partitions={cur_partitions}, nodes_with_leaders={nodes_with_leaders} all_leaders={sum(leaders_per_node)} : {leaders_per_node}')
            if cur_partitions == 1 and nodes_with_leaders == 1 and sum(leaders_per_node) == queues_count:
                break
            time.sleep(5)

        partitions = self.get_partitions(balancing_table_path)
        nodes_with_leaders = self.get_nodes_with_leaders()
        logging.info(f'all leaders on {nodes_with_leaders} node, partitions {partitions}')

        self.force_split(balancing_table_path)

        while True:
            self.send_messages(is_fifo, queue_urls)
            leaders_per_node = self.get_leaders_per_nodes()
            cur_nodes_with_leaders = len(list(filter(bool, leaders_per_node)))
            cur_partitions = self.get_partitions(balancing_table_path)
            logging.info(f'wait split... partitions={cur_partitions}, nodes_with_leaders={cur_nodes_with_leaders} all_leaders={sum(leaders_per_node)} : {leaders_per_node}')
            if cur_partitions > partitions and cur_nodes_with_leaders > nodes_with_leaders and sum(leaders_per_node) == queues_count:
                break
        logging.info(f'test finished. Leaders per node : {leaders_per_node}')
