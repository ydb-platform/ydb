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

class AdAccountPaymentOptions(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAccountPaymentOptions, self).__init__()
        self._isAdAccountPaymentOptions = True
        self._api = api

    class Field(AbstractObject.Field):
        available_altpay_options = 'available_altpay_options'
        available_card_types = 'available_card_types'
        available_payment_options = 'available_payment_options'
        existing_payment_methods = 'existing_payment_methods'

    _field_types = {
        'available_altpay_options': 'list<Object>',
        'available_card_types': 'list<string>',
        'available_payment_options': 'list<string>',
        'existing_payment_methods': 'list<Object>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


