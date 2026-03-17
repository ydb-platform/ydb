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

class CommerceMerchantSettingsSetupStatus(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CommerceMerchantSettingsSetupStatus, self).__init__()
        self._isCommerceMerchantSettingsSetupStatus = True
        self._api = api

    class Field(AbstractObject.Field):
        deals_setup = 'deals_setup'
        marketplace_approval_status = 'marketplace_approval_status'
        marketplace_approval_status_details = 'marketplace_approval_status_details'
        payment_setup = 'payment_setup'
        review_status = 'review_status'
        shop_setup = 'shop_setup'

    _field_types = {
        'deals_setup': 'string',
        'marketplace_approval_status': 'string',
        'marketplace_approval_status_details': 'Object',
        'payment_setup': 'string',
        'review_status': 'Object',
        'shop_setup': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


