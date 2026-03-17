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

class AdVolume(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdVolume, self).__init__()
        self._isAdVolume = True
        self._api = api

    class Field(AbstractObject.Field):
        ad_volume_break_down = 'ad_volume_break_down'
        ads_running_or_in_review_count = 'ads_running_or_in_review_count'
        future_limit_activation_date = 'future_limit_activation_date'
        future_limit_on_ads_running_or_in_review = 'future_limit_on_ads_running_or_in_review'
        individual_accounts_ad_volume = 'individual_accounts_ad_volume'
        is_gpa_page = 'is_gpa_page'
        limit_on_ads_running_or_in_review = 'limit_on_ads_running_or_in_review'
        owning_business_ad_volume = 'owning_business_ad_volume'
        partner_business_ad_volume = 'partner_business_ad_volume'
        user_role = 'user_role'

    _field_types = {
        'ad_volume_break_down': 'list<Object>',
        'ads_running_or_in_review_count': 'unsigned int',
        'future_limit_activation_date': 'string',
        'future_limit_on_ads_running_or_in_review': 'unsigned int',
        'individual_accounts_ad_volume': 'int',
        'is_gpa_page': 'bool',
        'limit_on_ads_running_or_in_review': 'unsigned int',
        'owning_business_ad_volume': 'int',
        'partner_business_ad_volume': 'int',
        'user_role': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


