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

class PartnerCategory(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isPartnerCategory = True
        super(PartnerCategory, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        approximate_count = 'approximate_count'
        country = 'country'
        description = 'description'
        details = 'details'
        id = 'id'
        is_private = 'is_private'
        name = 'name'
        parent_category = 'parent_category'
        source = 'source'
        status = 'status'
        targeting_type = 'targeting_type'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'partnercategories'

    _field_types = {
        'approximate_count': 'int',
        'country': 'string',
        'description': 'string',
        'details': 'string',
        'id': 'string',
        'is_private': 'bool',
        'name': 'string',
        'parent_category': 'string',
        'source': 'string',
        'status': 'string',
        'targeting_type': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


