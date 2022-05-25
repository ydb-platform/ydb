#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pytest
from hamcrest import assert_that, equal_to

from ydb.tests.library.sqs.matchers import ReadResponseMatcher

from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, POLLING_PARAMS, IS_FIFO_PARAMS, TABLES_FORMAT_PARAMS


class TestSqsPolling(KikimrSqsTestBase):
    @classmethod
    def _setup_config_generator(cls):
        config_generator = super(TestSqsPolling, cls)._setup_config_generator()
        config_generator.yaml_config['sqs_config']['check_all_shards_in_receive_message'] = False
        return config_generator

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    @pytest.mark.parametrize(**POLLING_PARAMS)
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    def test_receive_message_with_polling(self, is_fifo, polling_wait_timeout, tables_format):
        self._init_with_params(is_fifo, tables_format)

        created_queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo, use_http=False, shards=None if is_fifo else 1)
        empty_queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo, use_http=False, shards=None if is_fifo else 1)

        self.seq_no += 1
        self._send_message_and_assert(created_queue_url, 'test_send_message', seq_no=self.seq_no if is_fifo else None, group_id='group' if is_fifo else None)

        read_result = self._read_messages_and_assert(
            created_queue_url, messages_count=1, visibility_timeout=1000, wait_timeout=polling_wait_timeout,
            matcher=ReadResponseMatcher().with_n_messages(1)
        )
        assert_that(read_result[0]['Body'], equal_to('test_send_message'))

        # check that there's no infinite loop for empty queue
        read_result = self._read_messages_and_assert(
            empty_queue_url, messages_count=3, visibility_timeout=1000, wait_timeout=polling_wait_timeout
        )
        assert_that(len(read_result), equal_to(0))
