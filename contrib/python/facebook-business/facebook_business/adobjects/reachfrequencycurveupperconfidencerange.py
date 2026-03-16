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

class ReachFrequencyCurveUpperConfidenceRange(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ReachFrequencyCurveUpperConfidenceRange, self).__init__()
        self._isReachFrequencyCurveUpperConfidenceRange = True
        self._api = api

    class Field(AbstractObject.Field):
        impression_upper = 'impression_upper'
        num_points = 'num_points'
        reach = 'reach'
        reach_upper = 'reach_upper'
        uniq_video_views_2s_upper = 'uniq_video_views_2s_upper'
        video_views_2s_upper = 'video_views_2s_upper'

    _field_types = {
        'impression_upper': 'list<int>',
        'num_points': 'unsigned int',
        'reach': 'list<int>',
        'reach_upper': 'list<int>',
        'uniq_video_views_2s_upper': 'list<int>',
        'video_views_2s_upper': 'list<int>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


