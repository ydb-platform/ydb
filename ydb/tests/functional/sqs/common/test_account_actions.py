#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from hamcrest import assert_that, not_none, has_item, is_not

from ydb.tests.library.sqs.test_base import KikimrSqsTestBase, get_test_with_sqs_installation_by_path, get_test_with_sqs_tenant_installation
from ydb.tests.library.sqs.test_base import TABLES_FORMAT_PARAMS, HAS_QUEUES_PARAMS


class AccountActionsTest(KikimrSqsTestBase):
    @pytest.mark.parametrize(**TABLES_FORMAT_PARAMS)
    @pytest.mark.parametrize(**HAS_QUEUES_PARAMS)
    def test_manage_account(self, tables_format, has_queues):
        self._init_with_params(tables_format=tables_format)

        user_name = 'pupkin'
        self._sqs_api = self._create_api_for_user(user_name)

        create_user_result = self._sqs_api.create_user(user_name)
        assert_that(
            create_user_result,
            not_none()
        )
        user_list = self._sqs_api.list_users()
        assert_that(
            user_list, has_item(user_name)
        )

        if has_queues:
            self._sqs_api.create_queue('queue.fifo', is_fifo=True)
            self._sqs_api.create_queue('queue', is_fifo=False)

        delete_user_result = self._sqs_api.delete_user(user_name)
        assert_that(
            delete_user_result,
            not_none()
        )
        user_list = self._sqs_api.list_users()
        assert_that(
            user_list, is_not(has_item(user_name))
        )


class TestAccountActionsWithTenant(get_test_with_sqs_tenant_installation(AccountActionsTest)):
    pass


class TestAccountActionsWithPath(get_test_with_sqs_installation_by_path(AccountActionsTest)):
    pass
