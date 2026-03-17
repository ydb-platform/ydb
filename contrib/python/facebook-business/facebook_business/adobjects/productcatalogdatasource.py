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

class ProductCatalogDataSource(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isProductCatalogDataSource = True
        super(ProductCatalogDataSource, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        app_id = 'app_id'
        id = 'id'
        ingestion_source_type = 'ingestion_source_type'
        name = 'name'
        upload_type = 'upload_type'

    class IngestionSourceType:
        all = 'ALL'
        primary = 'PRIMARY'
        supplementary = 'SUPPLEMENTARY'

    _field_types = {
        'app_id': 'string',
        'id': 'string',
        'ingestion_source_type': 'string',
        'name': 'string',
        'upload_type': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['IngestionSourceType'] = ProductCatalogDataSource.IngestionSourceType.__dict__.values()
        return field_enum_info


