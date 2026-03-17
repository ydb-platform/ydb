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

class PageUserWithLeadsAccess(
    AbstractObject,
):

    def __init__(self, api=None):
        super(PageUserWithLeadsAccess, self).__init__()
        self._isPageUserWithLeadsAccess = True
        self._api = api

    class Field(AbstractObject.Field):
        active_on_business = 'active_on_business'
        business_role = 'business_role'
        can_access_leads = 'can_access_leads'
        page_role = 'page_role'

    _field_types = {
        'active_on_business': 'bool',
        'business_role': 'string',
        'can_access_leads': 'bool',
        'page_role': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


