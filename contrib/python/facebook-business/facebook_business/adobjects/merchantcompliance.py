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

class MerchantCompliance(
    AbstractObject,
):

    def __init__(self, api=None):
        super(MerchantCompliance, self).__init__()
        self._isMerchantCompliance = True
        self._api = api

    class Field(AbstractObject.Field):
        active_campaigns = 'active_campaigns'
        compliance_status = 'compliance_status'
        count_down_start_time = 'count_down_start_time'
        purchase = 'purchase'
        purchase_conversion_value = 'purchase_conversion_value'

    _field_types = {
        'active_campaigns': 'int',
        'compliance_status': 'string',
        'count_down_start_time': 'int',
        'purchase': 'int',
        'purchase_conversion_value': 'float',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


