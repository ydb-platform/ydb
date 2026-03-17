# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class ProductCatalogCategory(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isProductCatalogCategory = True
        super(ProductCatalogCategory, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        criteria_value = 'criteria_value'
        description = 'description'
        destination_uri = 'destination_uri'
        image_url = 'image_url'
        name = 'name'
        num_items = 'num_items'
        tokens = 'tokens'
        data = 'data'

    class CategorizationCriteria:
        brand = 'BRAND'
        category = 'CATEGORY'
        product_type = 'PRODUCT_TYPE'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'categories'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.productcatalog import ProductCatalog
        return ProductCatalog(api=self._api, fbid=parent_id).create_category(fields, params, batch, success, failure, pending)

    _field_types = {
        'criteria_value': 'string',
        'description': 'string',
        'destination_uri': 'string',
        'image_url': 'string',
        'name': 'string',
        'num_items': 'int',
        'tokens': 'list<map<string, string>>',
        'data': 'list<map>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['CategorizationCriteria'] = ProductCatalogCategory.CategorizationCriteria.__dict__.values()
        return field_enum_info


