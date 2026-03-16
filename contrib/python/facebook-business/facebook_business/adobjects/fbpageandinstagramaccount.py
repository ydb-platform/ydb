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

class FBPageAndInstagramAccount(
    AbstractObject,
):

    def __init__(self, api=None):
        super(FBPageAndInstagramAccount, self).__init__()
        self._isFBPageAndInstagramAccount = True
        self._api = api

    class Field(AbstractObject.Field):
        ad_permissions = 'ad_permissions'
        bc_permission_status = 'bc_permission_status'
        bc_permissions = 'bc_permissions'
        is_managed = 'is_managed'
        matched_by = 'matched_by'

    _field_types = {
        'ad_permissions': 'list<string>',
        'bc_permission_status': 'string',
        'bc_permissions': 'list<map<string, string>>',
        'is_managed': 'bool',
        'matched_by': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


