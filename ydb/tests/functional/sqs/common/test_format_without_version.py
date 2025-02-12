#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest

from hamcrest import assert_that, equal_to

from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, get_test_with_sqs_installation_by_path, get_test_with_sqs_tenant_installation, IS_FIFO_PARAMS
from ydb.tests.library.sqs.tables import create_queue_tables
from ydb.tests.library.common.types import TabletTypes

from ydb.tests.library.sqs.matchers import ReadResponseMatcher


class QueueWithoutVersionTest(KikimrSqsTestBase):
    used_tablets = []

    def chose_tablet(self):
        response = self.cluster.client.tablet_state(TabletTypes.FLAT_DATASHARD)
        for info in response.TabletStateInfo:
            if info.TabletId not in self.used_tablets:
                self.used_tablets.append(info.TabletId)
                return info.TabletId
        assert False

    def get_table_path(self, table=None):
        table_path = f'{self.sqs_root}/{self._username}/{self.queue_name}'
        if table is not None:
            table_path += f'/{table}'
        return table_path

    def init_queue(self, is_fifo):
        self._init_with_params(is_fifo)
        queue_tables_path = self.get_table_path()
        session = self._driver.table_client.session().create()

        shards = 1 if is_fifo else 4
        create_queue_tables(queue_tables_path, is_fifo, self._driver, session, shards)

    @pytest.mark.parametrize(**IS_FIFO_PARAMS)
    def test_common(self, is_fifo):
        self.init_queue(is_fifo)

        created_queue_url = self._create_queue_and_assert(self.queue_name, is_fifo=is_fifo)
        got_queue_url = self._sqs_api.get_queue_url(self.queue_name)
        assert_that(got_queue_url, equal_to(created_queue_url))

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

        self._sqs_api.get_queue_url(self.queue_name)
        self._sqs_api.get_queue_attributes(created_queue_url)
        self._sqs_api.set_queue_attributes(created_queue_url, {'MaximumMessageSize': '1024'})


class TestQueueWithoutVersionWithTenant(get_test_with_sqs_tenant_installation(QueueWithoutVersionTest)):
    pass


class TestQueueWithoutVersionWithPath(get_test_with_sqs_installation_by_path(QueueWithoutVersionTest)):
    pass
