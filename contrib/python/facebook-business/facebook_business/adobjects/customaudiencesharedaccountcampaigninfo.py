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

class CustomAudienceSharedAccountCampaignInfo(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CustomAudienceSharedAccountCampaignInfo, self).__init__()
        self._isCustomAudienceSharedAccountCampaignInfo = True
        self._api = api

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        account_name = 'account_name'
        adset_excluding_count = 'adset_excluding_count'
        adset_including_count = 'adset_including_count'
        campaign_delivery_status = 'campaign_delivery_status'
        campaign_objective = 'campaign_objective'
        campaign_pages = 'campaign_pages'
        campaign_schedule = 'campaign_schedule'

    _field_types = {
        'account_id': 'string',
        'account_name': 'string',
        'adset_excluding_count': 'unsigned int',
        'adset_including_count': 'unsigned int',
        'campaign_delivery_status': 'string',
        'campaign_objective': 'string',
        'campaign_pages': 'list<Object>',
        'campaign_schedule': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


