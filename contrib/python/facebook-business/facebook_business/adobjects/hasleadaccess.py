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

class HasLeadAccess(
    AbstractObject,
):

    def __init__(self, api=None):
        super(HasLeadAccess, self).__init__()
        self._isHasLeadAccess = True
        self._api = api

    class Field(AbstractObject.Field):
        app_has_leads_permission = 'app_has_leads_permission'
        can_access_lead = 'can_access_lead'
        enabled_lead_access_manager = 'enabled_lead_access_manager'
        failure_reason = 'failure_reason'
        failure_resolution = 'failure_resolution'
        is_page_admin = 'is_page_admin'
        page_id = 'page_id'
        user_has_leads_permission = 'user_has_leads_permission'
        user_id = 'user_id'

    _field_types = {
        'app_has_leads_permission': 'bool',
        'can_access_lead': 'bool',
        'enabled_lead_access_manager': 'bool',
        'failure_reason': 'string',
        'failure_resolution': 'string',
        'is_page_admin': 'bool',
        'page_id': 'string',
        'user_has_leads_permission': 'bool',
        'user_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


