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

class TimeSuggestion(
    AbstractObject,
):

    def __init__(self, api=None):
        super(TimeSuggestion, self).__init__()
        self._isTimeSuggestion = True
        self._api = api

    class Field(AbstractObject.Field):
        high_demand_periods = 'high_demand_periods'
        is_enabled = 'is_enabled'

    _field_types = {
        'high_demand_periods': 'list<Object>',
        'is_enabled': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


