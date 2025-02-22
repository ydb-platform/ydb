#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, get_test_with_sqs_installation_by_path, get_test_with_sqs_tenant_installation, IS_FIFO_PARAMS

from ydb.tests.library.sqs.matchers import ReadResponseMatcher


class MultiplexingTablesFormatTest(KikimrSqsTestBase):
    def create_queue(self, is_fifo):
        if is_fifo and not self.queue_name.endswith('.fifo'):
            self.queue_name = self.queue_name + '.fifo'
        return self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)

    def create_queue_must_fail(self, is_fifo):
        try:
            self.create_queue(is_fifo)
        except RuntimeError:
            return
        assert False

    def create_queue_with_wrong_tables_format(self, tables_format):
        self._set_tables_format(tables_format='qwerty')
        self.create_queue_must_fail(True)
        self.create_queue_must_fail(False)

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    def test_create_queue(self, is_fifo):
        self._set_tables_format()
        self.create_queue(is_fifo)

    def test_create_queue_with_incorrect_tables_format(self):
        self.create_queue_with_wrong_tables_format('qwerty')

    def test_create_queue_with_empty_tables_format(self):
        self.create_queue_with_wrong_tables_format('')

    def test_create_queue_with_unsupported_tables_format(self):
        self.create_queue_with_wrong_tables_format(2)

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    def test_send_message(self, is_fifo):
        self._set_tables_format()
        created_queue_url = self.create_queue(is_fifo)
        self._send_message_and_assert(
            created_queue_url,
            'test_send_message',
            seq_no=1 if is_fifo else None,
            group_id='group' if is_fifo else None
        )

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    def test_read_message(self, is_fifo):
        self._set_tables_format()
        created_queue_url = self.create_queue(is_fifo)
        self.seq_no += 1
        message_id = self._send_message_and_assert(
            created_queue_url,
            self._msg_body_template,
            seq_no=self.seq_no if is_fifo else None,
            group_id='group' if is_fifo else None
        )
        self._read_messages_and_assert(
            created_queue_url,
            messages_count=1,
            visibility_timeout=1000,
            matcher=ReadResponseMatcher().with_message_ids([message_id, ])
        )

    def do_test_double_create(self, is_fifo, tables_format):
        self._set_tables_format(tables_format=tables_format)
        self.create_queue(is_fifo)
        self.create_queue(is_fifo)

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    def test_double_create(self, is_fifo):
        self.do_test_double_create(is_fifo, tables_format='1')

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    def test_double_create_old(self, is_fifo):
        self.do_test_double_create(is_fifo, tables_format='0')


class TestMultiplexingTablesFormatWithTenant(get_test_with_sqs_tenant_installation(MultiplexingTablesFormatTest)):
    pass


class TestMultiplexingTablesFormatWithPath(get_test_with_sqs_installation_by_path(MultiplexingTablesFormatTest)):
    pass
