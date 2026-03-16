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

class AdAssetOnsiteDestinations(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAssetOnsiteDestinations, self).__init__()
        self._isAdAssetOnsiteDestinations = True
        self._api = api

    class Field(AbstractObject.Field):
        auto_optimization = 'auto_optimization'
        details_page_product_id = 'details_page_product_id'
        shop_collection_product_set_id = 'shop_collection_product_set_id'
        source = 'source'
        storefront_shop_id = 'storefront_shop_id'

    _field_types = {
        'auto_optimization': 'string',
        'details_page_product_id': 'string',
        'shop_collection_product_set_id': 'string',
        'source': 'string',
        'storefront_shop_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


