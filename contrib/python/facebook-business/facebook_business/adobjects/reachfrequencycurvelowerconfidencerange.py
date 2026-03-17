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

class ReachFrequencyCurveLowerConfidenceRange(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ReachFrequencyCurveLowerConfidenceRange, self).__init__()
        self._isReachFrequencyCurveLowerConfidenceRange = True
        self._api = api

    class Field(AbstractObject.Field):
        impression_lower = 'impression_lower'
        num_points = 'num_points'
        reach = 'reach'
        reach_lower = 'reach_lower'
        uniq_video_views_2s_lower = 'uniq_video_views_2s_lower'
        video_views_2s_lower = 'video_views_2s_lower'

    _field_types = {
        'impression_lower': 'list<int>',
        'num_points': 'unsigned int',
        'reach': 'list<int>',
        'reach_lower': 'list<int>',
        'uniq_video_views_2s_lower': 'list<int>',
        'video_views_2s_lower': 'list<int>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


