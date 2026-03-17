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

class PageCrmsWithLeadsAccess(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isPageCrmsWithLeadsAccess = True
        super(PageCrmsWithLeadsAccess, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        can_access_leads = 'can_access_leads'
        id = 'id'
        integration_type = 'integration_type'
        name = 'name'

    _field_types = {
        'can_access_leads': 'bool',
        'id': 'string',
        'integration_type': 'string',
        'name': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


