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

class AdsPixelCAPIIntegrationQuality(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdsPixelCAPIIntegrationQuality, self).__init__()
        self._isAdsPixelCAPIIntegrationQuality = True
        self._api = api

    class Field(AbstractObject.Field):
        acr = 'acr'
        data_freshness = 'data_freshness'
        dedupe_key_feedback = 'dedupe_key_feedback'
        event_coverage = 'event_coverage'
        event_match_quality = 'event_match_quality'
        event_name = 'event_name'
        event_potential_aly_acr_increase = 'event_potential_aly_acr_increase'

    _field_types = {
        'acr': 'Object',
        'data_freshness': 'Object',
        'dedupe_key_feedback': 'list<Object>',
        'event_coverage': 'Object',
        'event_match_quality': 'Object',
        'event_name': 'string',
        'event_potential_aly_acr_increase': 'Object',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


