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

class BroadTargetingCategories(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isBroadTargetingCategories = True
        super(BroadTargetingCategories, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        category_description = 'category_description'
        id = 'id'
        name = 'name'
        parent_category = 'parent_category'
        path = 'path'
        size_lower_bound = 'size_lower_bound'
        size_upper_bound = 'size_upper_bound'
        source = 'source'
        type = 'type'
        type_name = 'type_name'
        untranslated_name = 'untranslated_name'
        untranslated_parent_name = 'untranslated_parent_name'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'broadtargetingcategories'

    _field_types = {
        'category_description': 'string',
        'id': 'string',
        'name': 'string',
        'parent_category': 'string',
        'path': 'list<string>',
        'size_lower_bound': 'int',
        'size_upper_bound': 'int',
        'source': 'string',
        'type': 'int',
        'type_name': 'string',
        'untranslated_name': 'string',
        'untranslated_parent_name': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


