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

class ProductItemSubscriptionInfo(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ProductItemSubscriptionInfo, self).__init__()
        self._isProductItemSubscriptionInfo = True
        self._api = api

    class Field(AbstractObject.Field):
        is_subscribable = 'is_subscribable'
        subscription_billing_period = 'subscription_billing_period'
        subscription_billing_type = 'subscription_billing_type'

    _field_types = {
        'is_subscribable': 'bool',
        'subscription_billing_period': 'unsigned int',
        'subscription_billing_type': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


