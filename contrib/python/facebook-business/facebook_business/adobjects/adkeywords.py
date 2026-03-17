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

class AdKeywords(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdKeywords, self).__init__()
        self._isAdKeywords = True
        self._api = api

    class Field(AbstractObject.Field):
        brands = 'brands'
        product_categories = 'product_categories'
        product_names = 'product_names'
        search_terms = 'search_terms'

    _field_types = {
        'brands': 'list<string>',
        'product_categories': 'list<string>',
        'product_names': 'list<string>',
        'search_terms': 'list<string>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


