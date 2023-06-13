#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
import time
from hamcrest import assert_that, not_none

from ydb.tests.library.common.types import Erasure

from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, IS_FIFO_PARAMS, TABLES_FORMAT_PARAMS


class TestSqsRecompilesRequestsForOtherQueue(KikimrSqsTestBase):
    erasure = Erasure.BLOCK_4_2

    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestSqsRecompilesRequestsForOtherQueue, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['enable_queue_master'] = False
        config_generator.yaml_config['sqs_config']['leaders_describer_update_time_ms'] = 1000
        return config_generator

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_recompiles_queries(self, is_fifo, tables_format):
        self._init_with_params(is_fifo, tables_format)
        queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)

        def send(node_index):
            if is_fifo:
                group_id = "0"
                seq_no = self.seq_no
                self.seq_no += 1
            else:
                group_id = None
                seq_no = None
            result = self._sqs_apis[node_index].send_message(
                self.queue_url, self._msg_body_template.format(next(self.counter)), deduplication_id=seq_no, group_id=group_id)
            assert_that(
                result, not_none()
            )

        # cache write requests on each node
        for i in range(self.cluster_nodes_count):
            send(i)

        # delete and create queue through node 0
        self._sqs_api.delete_queue(queue_url)
        self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)

        # waiting for queue cache invalidation
        time.sleep(self.config_generator.yaml_config['sqs_config']['leaders_describer_update_time_ms'] * 2 / 1000.0)

        # send through node 1
        send(1)
