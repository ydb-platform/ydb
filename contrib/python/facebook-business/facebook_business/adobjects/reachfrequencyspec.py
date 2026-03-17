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

class ReachFrequencySpec(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ReachFrequencySpec, self).__init__()
        self._isReachFrequencySpec = True
        self._api = api

    class Field(AbstractObject.Field):
        countries = 'countries'
        default_creation_data = 'default_creation_data'
        global_io_max_campaign_duration = 'global_io_max_campaign_duration'
        max_campaign_duration = 'max_campaign_duration'
        max_days_to_finish = 'max_days_to_finish'
        max_pause_without_prediction_rerun = 'max_pause_without_prediction_rerun'
        min_campaign_duration = 'min_campaign_duration'
        min_reach_limits = 'min_reach_limits'

    _field_types = {
        'countries': 'list<string>',
        'default_creation_data': 'Object',
        'global_io_max_campaign_duration': 'unsigned int',
        'max_campaign_duration': 'Object',
        'max_days_to_finish': 'Object',
        'max_pause_without_prediction_rerun': 'Object',
        'min_campaign_duration': 'Object',
        'min_reach_limits': 'Object',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


