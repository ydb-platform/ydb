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

class AdAccountUserPermissions(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAccountUserPermissions, self).__init__()
        self._isAdAccountUserPermissions = True
        self._api = api

    class Field(AbstractObject.Field):
        business = 'business'
        business_persona = 'business_persona'
        created_by = 'created_by'
        created_time = 'created_time'
        email = 'email'
        status = 'status'
        tasks = 'tasks'
        updated_by = 'updated_by'
        updated_time = 'updated_time'
        user = 'user'

    _field_types = {
        'business': 'Business',
        'business_persona': 'Object',
        'created_by': 'User',
        'created_time': 'datetime',
        'email': 'string',
        'status': 'string',
        'tasks': 'list<string>',
        'updated_by': 'User',
        'updated_time': 'datetime',
        'user': 'User',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


