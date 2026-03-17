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

class ShadowIGUserCatalogProductSearch(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ShadowIGUserCatalogProductSearch, self).__init__()
        self._isShadowIGUserCatalogProductSearch = True
        self._api = api

    class Field(AbstractObject.Field):
        image_url = 'image_url'
        is_checkout_flow = 'is_checkout_flow'
        merchant_id = 'merchant_id'
        product_id = 'product_id'
        product_name = 'product_name'
        product_variants = 'product_variants'
        retailer_id = 'retailer_id'
        review_status = 'review_status'

    _field_types = {
        'image_url': 'string',
        'is_checkout_flow': 'bool',
        'merchant_id': 'int',
        'product_id': 'int',
        'product_name': 'string',
        'product_variants': 'list<ShadowIGUserCatalogProductVariant>',
        'retailer_id': 'string',
        'review_status': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


