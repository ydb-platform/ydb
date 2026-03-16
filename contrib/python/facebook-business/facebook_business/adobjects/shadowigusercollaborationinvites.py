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

class ShadowIGUserCollaborationInvites(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ShadowIGUserCollaborationInvites, self).__init__()
        self._isShadowIGUserCollaborationInvites = True
        self._api = api

    class Field(AbstractObject.Field):
        caption = 'caption'
        media_id = 'media_id'
        media_owner_username = 'media_owner_username'
        media_url = 'media_url'

    _field_types = {
        'caption': 'string',
        'media_id': 'string',
        'media_owner_username': 'string',
        'media_url': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


