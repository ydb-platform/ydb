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

class AnalyticsPlatformMetricsConfig(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AnalyticsPlatformMetricsConfig, self).__init__()
        self._isAnalyticsPlatformMetricsConfig = True
        self._api = api

    class Field(AbstractObject.Field):
        has_a2u = 'has_a2u'
        has_api_calls = 'has_api_calls'
        has_app_invites = 'has_app_invites'
        has_fb_login = 'has_fb_login'
        has_game_requests = 'has_game_requests'
        has_payments = 'has_payments'
        has_referrals = 'has_referrals'
        has_stories = 'has_stories'
        has_structured_requests = 'has_structured_requests'

    _field_types = {
        'has_a2u': 'bool',
        'has_api_calls': 'bool',
        'has_app_invites': 'bool',
        'has_fb_login': 'bool',
        'has_game_requests': 'bool',
        'has_payments': 'bool',
        'has_referrals': 'bool',
        'has_stories': 'bool',
        'has_structured_requests': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


