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

class MIXInsightsResult(
    AbstractObject,
):

    def __init__(self, api=None):
        super(MIXInsightsResult, self).__init__()
        self._isMIXInsightsResult = True
        self._api = api

    class Field(AbstractObject.Field):
        daily_age_gender_breakdown = 'daily_age_gender_breakdown'
        daily_audio_library_values = 'daily_audio_library_values'
        daily_ugc_values = 'daily_ugc_values'
        daily_values = 'daily_values'
        metric = 'metric'
        monthly_audio_library_values = 'monthly_audio_library_values'
        monthly_ugc_values = 'monthly_ugc_values'
        monthly_values = 'monthly_values'
        percent_growth = 'percent_growth'
        shielded_fields = 'shielded_fields'
        total_age_gender_breakdown = 'total_age_gender_breakdown'
        total_audio_library_value = 'total_audio_library_value'
        total_country_breakdown = 'total_country_breakdown'
        total_locale_breakdown = 'total_locale_breakdown'
        total_product_breakdown = 'total_product_breakdown'
        total_ugc_value = 'total_ugc_value'
        total_value = 'total_value'
        trending_age = 'trending_age'
        trending_gender = 'trending_gender'
        trending_interest = 'trending_interest'
        trending_territory = 'trending_territory'

    _field_types = {
        'daily_age_gender_breakdown': 'list<map<string, list<map<string, int>>>>',
        'daily_audio_library_values': 'list<map<string, int>>',
        'daily_ugc_values': 'list<map<string, int>>',
        'daily_values': 'list<map<string, int>>',
        'metric': 'string',
        'monthly_audio_library_values': 'list<map<string, int>>',
        'monthly_ugc_values': 'list<map<string, int>>',
        'monthly_values': 'list<map<string, int>>',
        'percent_growth': 'float',
        'shielded_fields': 'list<map<string, list<map<string, bool>>>>',
        'total_age_gender_breakdown': 'list<map<string, int>>',
        'total_audio_library_value': 'int',
        'total_country_breakdown': 'list<map<string, int>>',
        'total_locale_breakdown': 'list<map<string, int>>',
        'total_product_breakdown': 'list<map<string, int>>',
        'total_ugc_value': 'int',
        'total_value': 'int',
        'trending_age': 'list<map<string, list<map<string, float>>>>',
        'trending_gender': 'list<map<string, list<map<string, float>>>>',
        'trending_interest': 'list<map<string, list<map<string, float>>>>',
        'trending_territory': 'list<map<string, list<map<string, float>>>>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


