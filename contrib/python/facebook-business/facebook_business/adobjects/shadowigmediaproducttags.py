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

class ShadowIGMediaProductTags(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ShadowIGMediaProductTags, self).__init__()
        self._isShadowIGMediaProductTags = True
        self._api = api

    class Field(AbstractObject.Field):
        image_url = 'image_url'
        is_checkout = 'is_checkout'
        merchant_id = 'merchant_id'
        name = 'name'
        price_string = 'price_string'
        product_id = 'product_id'
        review_status = 'review_status'
        stripped_price_string = 'stripped_price_string'
        stripped_sale_price_string = 'stripped_sale_price_string'
        x = 'x'
        y = 'y'

    _field_types = {
        'image_url': 'string',
        'is_checkout': 'bool',
        'merchant_id': 'int',
        'name': 'string',
        'price_string': 'string',
        'product_id': 'int',
        'review_status': 'string',
        'stripped_price_string': 'string',
        'stripped_sale_price_string': 'string',
        'x': 'float',
        'y': 'float',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


