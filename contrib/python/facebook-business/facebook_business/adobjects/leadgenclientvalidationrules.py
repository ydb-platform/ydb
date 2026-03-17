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

class LeadGenClientValidationRules(
    AbstractObject,
):

    def __init__(self, api=None):
        super(LeadGenClientValidationRules, self).__init__()
        self._isLeadGenClientValidationRules = True
        self._api = api

    class Field(AbstractObject.Field):
        exclude_emoji_and_special_chars_enabled = 'exclude_emoji_and_special_chars_enabled'
        max_length_value = 'max_length_value'
        min_length_value = 'min_length_value'

    _field_types = {
        'exclude_emoji_and_special_chars_enabled': 'bool',
        'max_length_value': 'int',
        'min_length_value': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


