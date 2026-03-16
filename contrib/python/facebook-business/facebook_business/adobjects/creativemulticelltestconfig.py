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

class CreativeMulticellTestConfig(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CreativeMulticellTestConfig, self).__init__()
        self._isCreativeMulticellTestConfig = True
        self._api = api

    class Field(AbstractObject.Field):
        budget_percentage = 'budget_percentage'
        configured_cell_count = 'configured_cell_count'
        daily_budget = 'daily_budget'
        entry_source = 'entry_source'
        lifetime_budget = 'lifetime_budget'
        use_existing_daily_budget = 'use_existing_daily_budget'

    _field_types = {
        'budget_percentage': 'int',
        'configured_cell_count': 'int',
        'daily_budget': 'int',
        'entry_source': 'string',
        'lifetime_budget': 'int',
        'use_existing_daily_budget': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


