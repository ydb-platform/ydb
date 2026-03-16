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

class LiveVideoAdBreakConfig(
    AbstractObject,
):

    def __init__(self, api=None):
        super(LiveVideoAdBreakConfig, self).__init__()
        self._isLiveVideoAdBreakConfig = True
        self._api = api

    class Field(AbstractObject.Field):
        default_ad_break_duration = 'default_ad_break_duration'
        failure_reason_polling_interval = 'failure_reason_polling_interval'
        first_break_eligible_secs = 'first_break_eligible_secs'
        guide_url = 'guide_url'
        is_eligible_to_onboard = 'is_eligible_to_onboard'
        is_enabled = 'is_enabled'
        onboarding_url = 'onboarding_url'
        preparing_duration = 'preparing_duration'
        time_between_ad_breaks_secs = 'time_between_ad_breaks_secs'
        viewer_count_threshold = 'viewer_count_threshold'

    _field_types = {
        'default_ad_break_duration': 'unsigned int',
        'failure_reason_polling_interval': 'unsigned int',
        'first_break_eligible_secs': 'unsigned int',
        'guide_url': 'string',
        'is_eligible_to_onboard': 'bool',
        'is_enabled': 'bool',
        'onboarding_url': 'string',
        'preparing_duration': 'unsigned int',
        'time_between_ad_breaks_secs': 'unsigned int',
        'viewer_count_threshold': 'unsigned int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


