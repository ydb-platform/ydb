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

class LinkedInstagramAccountData(
    AbstractObject,
):

    def __init__(self, api=None):
        super(LinkedInstagramAccountData, self).__init__()
        self._isLinkedInstagramAccountData = True
        self._api = api

    class Field(AbstractObject.Field):
        access_token = 'access_token'
        analytics_claim = 'analytics_claim'
        full_name = 'full_name'
        profile_picture_url = 'profile_picture_url'
        user_id = 'user_id'
        user_name = 'user_name'

    _field_types = {
        'access_token': 'string',
        'analytics_claim': 'string',
        'full_name': 'string',
        'profile_picture_url': 'string',
        'user_id': 'string',
        'user_name': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


