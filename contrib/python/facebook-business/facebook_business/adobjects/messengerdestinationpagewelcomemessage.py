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

class MessengerDestinationPageWelcomeMessage(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isMessengerDestinationPageWelcomeMessage = True
        super(MessengerDestinationPageWelcomeMessage, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        id = 'id'
        page_welcome_message_body = 'page_welcome_message_body'
        page_welcome_message_type = 'page_welcome_message_type'
        template_name = 'template_name'
        time_created = 'time_created'
        time_last_used = 'time_last_used'

    _field_types = {
        'id': 'string',
        'page_welcome_message_body': 'string',
        'page_welcome_message_type': 'string',
        'template_name': 'string',
        'time_created': 'datetime',
        'time_last_used': 'datetime',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


