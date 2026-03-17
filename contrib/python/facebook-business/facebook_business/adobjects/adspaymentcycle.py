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

class AdsPaymentCycle(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdsPaymentCycle, self).__init__()
        self._isAdsPaymentCycle = True
        self._api = api

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        created_time = 'created_time'
        multiplier = 'multiplier'
        requested_threshold_amount = 'requested_threshold_amount'
        threshold_amount = 'threshold_amount'
        updated_time = 'updated_time'

    _field_types = {
        'account_id': 'string',
        'created_time': 'datetime',
        'multiplier': 'unsigned int',
        'requested_threshold_amount': 'unsigned int',
        'threshold_amount': 'unsigned int',
        'updated_time': 'datetime',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


