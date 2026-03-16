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

class ReachFrequencyEstimatesCurve(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ReachFrequencyEstimatesCurve, self).__init__()
        self._isReachFrequencyEstimatesCurve = True
        self._api = api

    class Field(AbstractObject.Field):
        budget = 'budget'
        conversion = 'conversion'
        impression = 'impression'
        interpolated_reach = 'interpolated_reach'
        num_points = 'num_points'
        raw_impression = 'raw_impression'
        raw_reach = 'raw_reach'
        reach = 'reach'

    _field_types = {
        'budget': 'list<int>',
        'conversion': 'list<int>',
        'impression': 'list<int>',
        'interpolated_reach': 'float',
        'num_points': 'unsigned int',
        'raw_impression': 'list<int>',
        'raw_reach': 'list<int>',
        'reach': 'list<int>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


