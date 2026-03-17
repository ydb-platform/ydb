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

class TargetingAutomation(
    AbstractObject,
):

    def __init__(self, api=None):
        super(TargetingAutomation, self).__init__()
        self._isTargetingAutomation = True
        self._api = api

    class Field(AbstractObject.Field):
        advantage_audience = 'advantage_audience'
        individual_setting = 'individual_setting'
        shared_audiences = 'shared_audiences'
        value_expression = 'value_expression'

    _field_types = {
        'advantage_audience': 'unsigned int',
        'individual_setting': 'Object',
        'shared_audiences': 'unsigned int',
        'value_expression': 'unsigned int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


