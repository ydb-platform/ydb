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

class PrivacyOption(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isPrivacyOption = True
        super(PrivacyOption, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        description = 'description'
        icon_src = 'icon_src'
        id = 'id'
        is_currently_selected = 'is_currently_selected'
        type = 'type'
        user_id = 'user_id'

    _field_types = {
        'description': 'string',
        'icon_src': 'string',
        'id': 'string',
        'is_currently_selected': 'bool',
        'type': 'string',
        'user_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


