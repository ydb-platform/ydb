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

class AdCampaignGroupIncrementalConversionOptimizationConfig(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCampaignGroupIncrementalConversionOptimizationConfig, self).__init__()
        self._isAdCampaignGroupIncrementalConversionOptimizationConfig = True
        self._api = api

    class Field(AbstractObject.Field):
        action_type = 'action_type'
        ad_study_end_time = 'ad_study_end_time'
        ad_study_id = 'ad_study_id'
        ad_study_name = 'ad_study_name'
        ad_study_start_time = 'ad_study_start_time'
        cell_id = 'cell_id'
        cell_name = 'cell_name'
        holdout_size = 'holdout_size'
        ico_type = 'ico_type'
        objectives = 'objectives'

    _field_types = {
        'action_type': 'string',
        'ad_study_end_time': 'datetime',
        'ad_study_id': 'string',
        'ad_study_name': 'string',
        'ad_study_start_time': 'datetime',
        'cell_id': 'string',
        'cell_name': 'string',
        'holdout_size': 'float',
        'ico_type': 'string',
        'objectives': 'list<Object>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


