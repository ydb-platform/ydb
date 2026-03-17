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

class ProductItemShipping(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ProductItemShipping, self).__init__()
        self._isProductItemShipping = True
        self._api = api

    class Field(AbstractObject.Field):
        shipping_country = 'shipping_country'
        shipping_price_currency = 'shipping_price_currency'
        shipping_price_value = 'shipping_price_value'
        shipping_region = 'shipping_region'
        shipping_service = 'shipping_service'

    _field_types = {
        'shipping_country': 'string',
        'shipping_price_currency': 'string',
        'shipping_price_value': 'float',
        'shipping_region': 'string',
        'shipping_service': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


