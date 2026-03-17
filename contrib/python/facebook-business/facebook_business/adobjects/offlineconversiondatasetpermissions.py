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

class OfflineConversionDataSetPermissions(
    AbstractObject,
):

    def __init__(self, api=None):
        super(OfflineConversionDataSetPermissions, self).__init__()
        self._isOfflineConversionDataSetPermissions = True
        self._api = api

    class Field(AbstractObject.Field):
        can_edit = 'can_edit'
        can_edit_or_upload = 'can_edit_or_upload'
        can_upload = 'can_upload'
        should_block_vanilla_business_employee_access = 'should_block_vanilla_business_employee_access'

    _field_types = {
        'can_edit': 'bool',
        'can_edit_or_upload': 'bool',
        'can_upload': 'bool',
        'should_block_vanilla_business_employee_access': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


