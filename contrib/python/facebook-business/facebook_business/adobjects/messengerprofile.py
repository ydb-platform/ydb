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

class MessengerProfile(
    AbstractObject,
):

    def __init__(self, api=None):
        super(MessengerProfile, self).__init__()
        self._isMessengerProfile = True
        self._api = api

    class Field(AbstractObject.Field):
        account_linking_url = 'account_linking_url'
        commands = 'commands'
        get_started = 'get_started'
        greeting = 'greeting'
        ice_breakers = 'ice_breakers'
        persistent_menu = 'persistent_menu'
        subject_to_new_eu_privacy_rules = 'subject_to_new_eu_privacy_rules'
        whitelisted_domains = 'whitelisted_domains'

    _field_types = {
        'account_linking_url': 'string',
        'commands': 'list<Object>',
        'get_started': 'Object',
        'greeting': 'list<Object>',
        'ice_breakers': 'list<Object>',
        'persistent_menu': 'list<Object>',
        'subject_to_new_eu_privacy_rules': 'bool',
        'whitelisted_domains': 'list<string>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


