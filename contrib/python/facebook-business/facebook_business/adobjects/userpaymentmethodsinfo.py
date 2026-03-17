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

class UserPaymentMethodsInfo(
    AbstractObject,
):

    def __init__(self, api=None):
        super(UserPaymentMethodsInfo, self).__init__()
        self._isUserPaymentMethodsInfo = True
        self._api = api

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        available_card_types = 'available_card_types'
        available_payment_methods = 'available_payment_methods'
        available_payment_methods_details = 'available_payment_methods_details'
        country = 'country'
        currency = 'currency'
        existing_payment_methods = 'existing_payment_methods'

    _field_types = {
        'account_id': 'string',
        'available_card_types': 'list<string>',
        'available_payment_methods': 'list<string>',
        'available_payment_methods_details': 'list<Object>',
        'country': 'string',
        'currency': 'string',
        'existing_payment_methods': 'list<Object>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


