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

class ChatPlugin(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ChatPlugin, self).__init__()
        self._isChatPlugin = True
        self._api = api

    class Field(AbstractObject.Field):
        alignment = 'alignment'
        desktop_bottom_spacing = 'desktop_bottom_spacing'
        desktop_side_spacing = 'desktop_side_spacing'
        entry_point_icon = 'entry_point_icon'
        entry_point_label = 'entry_point_label'
        greeting_dialog_display = 'greeting_dialog_display'
        guest_chat_mode = 'guest_chat_mode'
        mobile_bottom_spacing = 'mobile_bottom_spacing'
        mobile_chat_display = 'mobile_chat_display'
        mobile_side_spacing = 'mobile_side_spacing'
        theme_color = 'theme_color'
        welcome_screen_greeting = 'welcome_screen_greeting'

    _field_types = {
        'alignment': 'string',
        'desktop_bottom_spacing': 'string',
        'desktop_side_spacing': 'string',
        'entry_point_icon': 'string',
        'entry_point_label': 'string',
        'greeting_dialog_display': 'string',
        'guest_chat_mode': 'string',
        'mobile_bottom_spacing': 'string',
        'mobile_chat_display': 'string',
        'mobile_side_spacing': 'string',
        'theme_color': 'string',
        'welcome_screen_greeting': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


