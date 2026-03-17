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

class AdAccountPrepayDetails(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAccountPrepayDetails, self).__init__()
        self._isAdAccountPrepayDetails = True
        self._api = api

    class Field(AbstractObject.Field):
        default_funding_amount = 'default_funding_amount'
        max_acceptable_amount = 'max_acceptable_amount'
        min_acceptable_amount = 'min_acceptable_amount'
        should_collect_business_details = 'should_collect_business_details'

    _field_types = {
        'default_funding_amount': 'CurrencyAmount',
        'max_acceptable_amount': 'CurrencyAmount',
        'min_acceptable_amount': 'CurrencyAmount',
        'should_collect_business_details': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


