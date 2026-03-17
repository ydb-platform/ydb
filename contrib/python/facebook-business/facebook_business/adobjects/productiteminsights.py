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

class ProductItemInsights(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ProductItemInsights, self).__init__()
        self._isProductItemInsights = True
        self._api = api

    class Field(AbstractObject.Field):
        ad_click_count = 'ad_click_count'
        ad_impression_count = 'ad_impression_count'
        add_to_cart_count = 'add_to_cart_count'
        purchase_count = 'purchase_count'
        view_content_count = 'view_content_count'

    _field_types = {
        'ad_click_count': 'int',
        'ad_impression_count': 'int',
        'add_to_cart_count': 'int',
        'purchase_count': 'int',
        'view_content_count': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


