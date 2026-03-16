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

class ResellerGuidance(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ResellerGuidance, self).__init__()
        self._isResellerGuidance = True
        self._api = api

    class Field(AbstractObject.Field):
        ad_account_first_spend_date = 'ad_account_first_spend_date'
        ad_account_id = 'ad_account_id'
        adopted_guidance_l7d = 'adopted_guidance_l7d'
        advertiser_name = 'advertiser_name'
        attributed_to_reseller_l7d = 'attributed_to_reseller_l7d'
        available_guidance = 'available_guidance'
        guidance_adoption_rate_l7d = 'guidance_adoption_rate_l7d'
        nurtured_by_reseller_l7d = 'nurtured_by_reseller_l7d'
        planning_agency_name = 'planning_agency_name'
        recommendation_time = 'recommendation_time'
        reporting_ds = 'reporting_ds'
        reseller = 'reseller'
        revenue_l30d = 'revenue_l30d'
        ultimate_advertiser_name = 'ultimate_advertiser_name'

    _field_types = {
        'ad_account_first_spend_date': 'string',
        'ad_account_id': 'string',
        'adopted_guidance_l7d': 'list<string>',
        'advertiser_name': 'string',
        'attributed_to_reseller_l7d': 'bool',
        'available_guidance': 'list<string>',
        'guidance_adoption_rate_l7d': 'float',
        'nurtured_by_reseller_l7d': 'bool',
        'planning_agency_name': 'string',
        'recommendation_time': 'datetime',
        'reporting_ds': 'string',
        'reseller': 'Business',
        'revenue_l30d': 'float',
        'ultimate_advertiser_name': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


