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

class AdAccountPromotionProgressBar(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAccountPromotionProgressBar, self).__init__()
        self._isAdAccountPromotionProgressBar = True
        self._api = api

    class Field(AbstractObject.Field):
        adaccount_permission = 'adaccount_permission'
        coupon_currency = 'coupon_currency'
        coupon_value = 'coupon_value'
        expiration_time = 'expiration_time'
        progress_completed = 'progress_completed'
        promotion_type = 'promotion_type'
        spend_requirement_in_cent = 'spend_requirement_in_cent'
        spend_since_enrollment = 'spend_since_enrollment'

    _field_types = {
        'adaccount_permission': 'bool',
        'coupon_currency': 'string',
        'coupon_value': 'int',
        'expiration_time': 'datetime',
        'progress_completed': 'bool',
        'promotion_type': 'string',
        'spend_requirement_in_cent': 'int',
        'spend_since_enrollment': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


