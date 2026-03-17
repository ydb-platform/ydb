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

class FundingSourceDetailsCoupon(
    AbstractObject,
):

    def __init__(self, api=None):
        super(FundingSourceDetailsCoupon, self).__init__()
        self._isFundingSourceDetailsCoupon = True
        self._api = api

    class Field(AbstractObject.Field):
        amount = 'amount'
        campaign_ids = 'campaign_ids'
        child_ad_account_id = 'child_ad_account_id'
        child_bm_id = 'child_bm_id'
        coupon_id = 'coupon_id'
        coupon_tiering = 'coupon_tiering'
        currency = 'currency'
        display_amount = 'display_amount'
        expiration = 'expiration'
        original_amount = 'original_amount'
        original_display_amount = 'original_display_amount'
        start_date = 'start_date'
        vendor_id = 'vendor_id'

    _field_types = {
        'amount': 'int',
        'campaign_ids': 'list<int>',
        'child_ad_account_id': 'string',
        'child_bm_id': 'string',
        'coupon_id': 'string',
        'coupon_tiering': 'FundingSourceDetailsCouponTiering',
        'currency': 'string',
        'display_amount': 'string',
        'expiration': 'datetime',
        'original_amount': 'int',
        'original_display_amount': 'string',
        'start_date': 'datetime',
        'vendor_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


