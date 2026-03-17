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

class UserPaymentMobilePricepoints(
    AbstractObject,
):

    def __init__(self, api=None):
        super(UserPaymentMobilePricepoints, self).__init__()
        self._isUserPaymentMobilePricepoints = True
        self._api = api

    class Field(AbstractObject.Field):
        mobile_country = 'mobile_country'
        phone_number_last4 = 'phone_number_last4'
        pricepoints = 'pricepoints'
        user_currency = 'user_currency'

    _field_types = {
        'mobile_country': 'string',
        'phone_number_last4': 'string',
        'pricepoints': 'list<Object>',
        'user_currency': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


