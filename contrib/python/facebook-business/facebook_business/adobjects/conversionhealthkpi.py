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

class ConversionHealthKPI(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ConversionHealthKPI, self).__init__()
        self._isConversionHealthKPI = True
        self._api = api

    class Field(AbstractObject.Field):
        health_indicator = 'health_indicator'
        impacted_browsers_match_rate = 'impacted_browsers_match_rate'
        impacted_browsers_match_rate_mom_trend = 'impacted_browsers_match_rate_mom_trend'
        impacted_browsers_traffic_share = 'impacted_browsers_traffic_share'
        impacted_browsers_traffic_share_mom_trend = 'impacted_browsers_traffic_share_mom_trend'
        match_rate = 'match_rate'
        match_rate_mom_trend = 'match_rate_mom_trend'
        match_rate_vertical_benchmark = 'match_rate_vertical_benchmark'
        match_rate_vs_benchmark_mom_trend = 'match_rate_vs_benchmark_mom_trend'

    _field_types = {
        'health_indicator': 'string',
        'impacted_browsers_match_rate': 'float',
        'impacted_browsers_match_rate_mom_trend': 'float',
        'impacted_browsers_traffic_share': 'float',
        'impacted_browsers_traffic_share_mom_trend': 'float',
        'match_rate': 'float',
        'match_rate_mom_trend': 'float',
        'match_rate_vertical_benchmark': 'float',
        'match_rate_vs_benchmark_mom_trend': 'float',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


