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

class AudiencePermissionForActions(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AudiencePermissionForActions, self).__init__()
        self._isAudiencePermissionForActions = True
        self._api = api

    class Field(AbstractObject.Field):
        can_edit = 'can_edit'
        can_see_insight = 'can_see_insight'
        can_share = 'can_share'
        subtype_supports_lookalike = 'subtype_supports_lookalike'
        supports_recipient_lookalike = 'supports_recipient_lookalike'

    _field_types = {
        'can_edit': 'bool',
        'can_see_insight': 'bool',
        'can_share': 'bool',
        'subtype_supports_lookalike': 'bool',
        'supports_recipient_lookalike': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


