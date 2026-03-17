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

class CustomAudienceSalts(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CustomAudienceSalts, self).__init__()
        self._isCustomAudienceSalts = True
        self._api = api

    class Field(AbstractObject.Field):
        app_id = 'app_id'
        public_key = 'public_key'
        salts = 'salts'
        user_id = 'user_id'

    _field_types = {
        'app_id': 'int',
        'public_key': 'string',
        'salts': 'list<Object>',
        'user_id': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


