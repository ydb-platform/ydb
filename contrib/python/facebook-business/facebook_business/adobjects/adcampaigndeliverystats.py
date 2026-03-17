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

class AdCampaignDeliveryStats(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCampaignDeliveryStats, self).__init__()
        self._isAdCampaignDeliveryStats = True
        self._api = api

    class Field(AbstractObject.Field):
        bid_recommendation = 'bid_recommendation'
        current_average_cost = 'current_average_cost'
        last_significant_edit_ts = 'last_significant_edit_ts'
        learning_stage_exit_info = 'learning_stage_exit_info'
        learning_stage_info = 'learning_stage_info'
        unsupported_features = 'unsupported_features'

    _field_types = {
        'bid_recommendation': 'int',
        'current_average_cost': 'float',
        'last_significant_edit_ts': 'int',
        'learning_stage_exit_info': 'Object',
        'learning_stage_info': 'AdCampaignLearningStageInfo',
        'unsupported_features': 'list<map<string, AdCampaignDeliveryStatsUnsupportedReasons>>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


