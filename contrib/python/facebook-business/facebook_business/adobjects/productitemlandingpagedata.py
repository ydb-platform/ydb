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

class ProductItemLandingPageData(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ProductItemLandingPageData, self).__init__()
        self._isProductItemLandingPageData = True
        self._api = api

    class Field(AbstractObject.Field):
        availability = 'availability'

    class Availability:
        available_for_order = 'available for order'
        discontinued = 'discontinued'
        in_stock = 'in stock'
        mark_as_sold = 'mark_as_sold'
        out_of_stock = 'out of stock'
        pending = 'pending'
        preorder = 'preorder'

    _field_types = {
        'availability': 'Availability',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Availability'] = ProductItemLandingPageData.Availability.__dict__.values()
        return field_enum_info


