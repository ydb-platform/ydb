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

class AdRuleHistoryResultAction(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdRuleHistoryResultAction, self).__init__()
        self._isAdRuleHistoryResultAction = True
        self._api = api

    class Field(AbstractObject.Field):
        action = 'action'
        field = 'field'
        new_value = 'new_value'
        old_value = 'old_value'

    _field_types = {
        'action': 'string',
        'field': 'string',
        'new_value': 'string',
        'old_value': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


