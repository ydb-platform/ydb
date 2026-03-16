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

class AdAccountAAASimilarCampaigns(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAccountAAASimilarCampaigns, self).__init__()
        self._isAdAccountAAASimilarCampaigns = True
        self._api = api

    class Field(AbstractObject.Field):
        similar_campaign_limit = 'similar_campaign_limit'
        similar_campaigns_info = 'similar_campaigns_info'
        used_campaign_slots = 'used_campaign_slots'

    _field_types = {
        'similar_campaign_limit': 'unsigned int',
        'similar_campaigns_info': 'list<map<string, Object>>',
        'used_campaign_slots': 'unsigned int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


