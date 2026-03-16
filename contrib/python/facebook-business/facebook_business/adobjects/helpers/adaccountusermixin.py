# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.page import Page
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker

class AdAccountUserMixin:

    class Field(object):
        id = 'id'
        name = 'name'
        permissions = 'permissions'
        role = 'role'
    class Permission(object):
        account_admin = 1
        admanager_read = 2
        admanager_write = 3
        billing_read = 4
        billing_write = 5
        reports = 7

    class Role(object):
        administrator = 1001
        analyst = 1003
        manager = 1002

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'users'

    def get_ad_accounts(self, fields=None, params=None):
        """Returns iterator over AdAccounts associated with this user."""
        return self.iterate_edge(AdAccount, fields, params, endpoint='adaccounts')

    def get_ad_account(self, fields=None, params=None):
        """Returns first AdAccount associated with this user."""
        return self.edge_object(AdAccount, fields, params)

    def get_pages(self, fields=None, params=None):
        """Returns iterator over Pages's associated with this user."""
        return self.iterate_edge(Page, fields, params)
