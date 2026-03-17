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

class CommercePayout(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CommercePayout, self).__init__()
        self._isCommercePayout = True
        self._api = api

    class Field(AbstractObject.Field):
        amount = 'amount'
        payout_date = 'payout_date'
        payout_reference_id = 'payout_reference_id'
        status = 'status'
        transfer_id = 'transfer_id'

    _field_types = {
        'amount': 'Object',
        'payout_date': 'string',
        'payout_reference_id': 'string',
        'status': 'string',
        'transfer_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


