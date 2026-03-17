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

class AdLimitsEnforcementData(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdLimitsEnforcementData, self).__init__()
        self._isAdLimitsEnforcementData = True
        self._api = api

    class Field(AbstractObject.Field):
        ad_limit_on_page = 'ad_limit_on_page'
        ad_limit_on_scope = 'ad_limit_on_scope'
        ad_volume_on_page = 'ad_volume_on_page'
        ad_volume_on_scope = 'ad_volume_on_scope'
        is_admin = 'is_admin'
        page_name = 'page_name'

    _field_types = {
        'ad_limit_on_page': 'int',
        'ad_limit_on_scope': 'int',
        'ad_volume_on_page': 'int',
        'ad_volume_on_scope': 'int',
        'is_admin': 'bool',
        'page_name': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


