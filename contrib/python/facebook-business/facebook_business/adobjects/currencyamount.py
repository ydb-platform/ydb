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

class CurrencyAmount(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CurrencyAmount, self).__init__()
        self._isCurrencyAmount = True
        self._api = api

    class Field(AbstractObject.Field):
        amount = 'amount'
        amount_in_hundredths = 'amount_in_hundredths'
        currency = 'currency'
        offsetted_amount = 'offsetted_amount'

    _field_types = {
        'amount': 'string',
        'amount_in_hundredths': 'string',
        'currency': 'string',
        'offsetted_amount': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


