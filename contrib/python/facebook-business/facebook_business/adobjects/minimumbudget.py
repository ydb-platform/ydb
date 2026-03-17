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

class MinimumBudget(
    AbstractObject,
):

    def __init__(self, api=None):
        super(MinimumBudget, self).__init__()
        self._isMinimumBudget = True
        self._api = api

    class Field(AbstractObject.Field):
        currency = 'currency'
        min_daily_budget_high_freq = 'min_daily_budget_high_freq'
        min_daily_budget_imp = 'min_daily_budget_imp'
        min_daily_budget_low_freq = 'min_daily_budget_low_freq'
        min_daily_budget_video_views = 'min_daily_budget_video_views'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'minimum_budgets'

    _field_types = {
        'currency': 'string',
        'min_daily_budget_high_freq': 'int',
        'min_daily_budget_imp': 'int',
        'min_daily_budget_low_freq': 'int',
        'min_daily_budget_video_views': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


