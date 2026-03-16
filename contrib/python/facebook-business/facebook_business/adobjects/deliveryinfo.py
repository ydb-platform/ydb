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

class DeliveryInfo(
    AbstractObject,
):

    def __init__(self, api=None):
        super(DeliveryInfo, self).__init__()
        self._isDeliveryInfo = True
        self._api = api

    class Field(AbstractObject.Field):
        active_accelerated_campaign_count = 'active_accelerated_campaign_count'
        active_day_parted_campaign_count = 'active_day_parted_campaign_count'
        ad_penalty_map = 'ad_penalty_map'
        are_all_daily_budgets_spent = 'are_all_daily_budgets_spent'
        credit_needed_ads_count = 'credit_needed_ads_count'
        eligible_for_delivery_insights = 'eligible_for_delivery_insights'
        end_time = 'end_time'
        has_account_hit_spend_limit = 'has_account_hit_spend_limit'
        has_campaign_group_hit_spend_limit = 'has_campaign_group_hit_spend_limit'
        has_no_active_ads = 'has_no_active_ads'
        has_no_ads = 'has_no_ads'
        inactive_ads_count = 'inactive_ads_count'
        inactive_campaign_count = 'inactive_campaign_count'
        is_account_closed = 'is_account_closed'
        is_account_disabled = 'is_account_disabled'
        is_ad_uneconomical = 'is_ad_uneconomical'
        is_adfarm_penalized = 'is_adfarm_penalized'
        is_adgroup_partially_rejected = 'is_adgroup_partially_rejected'
        is_campaign_accelerated = 'is_campaign_accelerated'
        is_campaign_completed = 'is_campaign_completed'
        is_campaign_day_parted = 'is_campaign_day_parted'
        is_campaign_disabled = 'is_campaign_disabled'
        is_campaign_group_disabled = 'is_campaign_group_disabled'
        is_clickbait_penalized = 'is_clickbait_penalized'
        is_daily_budget_spent = 'is_daily_budget_spent'
        is_engagement_bait_penalized = 'is_engagement_bait_penalized'
        is_lqwe_penalized = 'is_lqwe_penalized'
        is_reach_and_frequency_misconfigured = 'is_reach_and_frequency_misconfigured'
        is_sensationalism_penalized = 'is_sensationalism_penalized'
        is_split_test_active = 'is_split_test_active'
        is_split_test_valid = 'is_split_test_valid'
        lift_study_time_period = 'lift_study_time_period'
        needs_credit = 'needs_credit'
        needs_tax_number = 'needs_tax_number'
        non_deleted_ads_count = 'non_deleted_ads_count'
        not_delivering_campaign_count = 'not_delivering_campaign_count'
        pending_ads_count = 'pending_ads_count'
        reach_frequency_campaign_underdelivery_reason = 'reach_frequency_campaign_underdelivery_reason'
        rejected_ads_count = 'rejected_ads_count'
        start_time = 'start_time'
        status = 'status'
        text_penalty_level = 'text_penalty_level'

    _field_types = {
        'active_accelerated_campaign_count': 'int',
        'active_day_parted_campaign_count': 'int',
        'ad_penalty_map': 'list<map<string, bool>>',
        'are_all_daily_budgets_spent': 'bool',
        'credit_needed_ads_count': 'int',
        'eligible_for_delivery_insights': 'bool',
        'end_time': 'datetime',
        'has_account_hit_spend_limit': 'bool',
        'has_campaign_group_hit_spend_limit': 'bool',
        'has_no_active_ads': 'bool',
        'has_no_ads': 'bool',
        'inactive_ads_count': 'int',
        'inactive_campaign_count': 'int',
        'is_account_closed': 'bool',
        'is_account_disabled': 'bool',
        'is_ad_uneconomical': 'bool',
        'is_adfarm_penalized': 'bool',
        'is_adgroup_partially_rejected': 'bool',
        'is_campaign_accelerated': 'bool',
        'is_campaign_completed': 'bool',
        'is_campaign_day_parted': 'bool',
        'is_campaign_disabled': 'bool',
        'is_campaign_group_disabled': 'bool',
        'is_clickbait_penalized': 'bool',
        'is_daily_budget_spent': 'bool',
        'is_engagement_bait_penalized': 'bool',
        'is_lqwe_penalized': 'bool',
        'is_reach_and_frequency_misconfigured': 'bool',
        'is_sensationalism_penalized': 'bool',
        'is_split_test_active': 'bool',
        'is_split_test_valid': 'bool',
        'lift_study_time_period': 'string',
        'needs_credit': 'bool',
        'needs_tax_number': 'bool',
        'non_deleted_ads_count': 'int',
        'not_delivering_campaign_count': 'int',
        'pending_ads_count': 'int',
        'reach_frequency_campaign_underdelivery_reason': 'string',
        'rejected_ads_count': 'int',
        'start_time': 'datetime',
        'status': 'string',
        'text_penalty_level': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


