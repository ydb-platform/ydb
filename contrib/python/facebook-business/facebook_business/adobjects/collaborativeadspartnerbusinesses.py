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

class CollaborativeAdsPartnerBusinesses(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CollaborativeAdsPartnerBusinesses, self).__init__()
        self._isCollaborativeAdsPartnerBusinesses = True
        self._api = api

    class Field(AbstractObject.Field):
        collaborative_ads_partner_businesses_info = 'collaborative_ads_partner_businesses_info'
        dedicated_partner_business_info = 'dedicated_partner_business_info'
        dedicated_partners_business_info = 'dedicated_partners_business_info'

    _field_types = {
        'collaborative_ads_partner_businesses_info': 'list<Business>',
        'dedicated_partner_business_info': 'Business',
        'dedicated_partners_business_info': 'list<Business>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


