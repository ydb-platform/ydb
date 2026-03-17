# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class McomInvoiceBankAccount(
    AbstractObject,
):

    def __init__(self, api=None):
        super(McomInvoiceBankAccount, self).__init__()
        self._isMcomInvoiceBankAccount = True
        self._api = api

    class Field(AbstractObject.Field):
        num_pending_verification_accounts = 'num_pending_verification_accounts'
        num_verified_accounts = 'num_verified_accounts'
        pending_verification_accounts = 'pending_verification_accounts'
        verified_accounts = 'verified_accounts'

    _field_types = {
        'num_pending_verification_accounts': 'int',
        'num_verified_accounts': 'int',
        'pending_verification_accounts': 'list<Object>',
        'verified_accounts': 'list<Object>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


