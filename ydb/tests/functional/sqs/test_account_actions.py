#!/usr/bin/env python
# -*- coding: utf-8 -*-
from hamcrest import assert_that, not_none, has_item, is_not

from sqs_test_base import KikimrSqsTestBase, get_test_with_sqs_installation_by_path, get_test_with_sqs_tenant_installation


class AccountActionsTest(KikimrSqsTestBase):
    def test_manage_account(self):
        user_name = 'pupkin'
        create_user_result = self._sqs_api.create_user(user_name)
        assert_that(
            create_user_result,
            not_none()
        )
        user_list = self._sqs_api.list_users()
        assert_that(
            user_list, has_item(user_name)
        )

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
