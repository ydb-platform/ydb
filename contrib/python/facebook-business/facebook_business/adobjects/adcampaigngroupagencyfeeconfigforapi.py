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

class AdCampaignGroupAgencyFeeConfigForApi(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCampaignGroupAgencyFeeConfigForApi, self).__init__()
        self._isAdCampaignGroupAgencyFeeConfigForApi = True
        self._api = api

    class Field(AbstractObject.Field):
        agency_fee_pct = 'agency_fee_pct'
        is_agency_fee_disabled = 'is_agency_fee_disabled'
        is_default_agency_fee = 'is_default_agency_fee'

    _field_types = {
        'agency_fee_pct': 'float',
        'is_agency_fee_disabled': 'bool',
        'is_default_agency_fee': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


