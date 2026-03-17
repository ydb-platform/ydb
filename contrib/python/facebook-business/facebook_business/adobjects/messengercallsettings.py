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

class MessengerCallSettings(
    AbstractObject,
):

    def __init__(self, api=None):
        super(MessengerCallSettings, self).__init__()
        self._isMessengerCallSettings = True
        self._api = api

    class Field(AbstractObject.Field):
        audio_enabled = 'audio_enabled'
        call_hours = 'call_hours'
        call_routing = 'call_routing'
        icon_enabled = 'icon_enabled'
        video = 'video'

    _field_types = {
        'audio_enabled': 'bool',
        'call_hours': 'Object',
        'call_routing': 'string',
        'icon_enabled': 'bool',
        'video': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


