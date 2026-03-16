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

class AdAccountSubsidyAmountDetails(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAccountSubsidyAmountDetails, self).__init__()
        self._isAdAccountSubsidyAmountDetails = True
        self._api = api

    class Field(AbstractObject.Field):
        entered_amount = 'entered_amount'
        fee_amount = 'fee_amount'
        total_amount = 'total_amount'

    _field_types = {
        'entered_amount': 'CurrencyAmount',
        'fee_amount': 'CurrencyAmount',
        'total_amount': 'CurrencyAmount',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


