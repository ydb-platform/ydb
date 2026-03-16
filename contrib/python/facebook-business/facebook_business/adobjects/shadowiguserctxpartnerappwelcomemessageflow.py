# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class ShadowIGUserCTXPartnerAppWelcomeMessageFlow(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isShadowIGUserCTXPartnerAppWelcomeMessageFlow = True
        super(ShadowIGUserCTXPartnerAppWelcomeMessageFlow, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        compatible_platforms = 'compatible_platforms'
        eligible_platforms = 'eligible_platforms'
        id = 'id'
        is_ig_only_flow = 'is_ig_only_flow'
        is_used_in_ad = 'is_used_in_ad'
        last_update_time = 'last_update_time'
        name = 'name'
        welcome_message_flow = 'welcome_message_flow'

    _field_types = {
        'compatible_platforms': 'list<string>',
        'eligible_platforms': 'list<string>',
        'id': 'string',
        'is_ig_only_flow': 'bool',
        'is_used_in_ad': 'bool',
        'last_update_time': 'datetime',
        'name': 'string',
        'welcome_message_flow': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


