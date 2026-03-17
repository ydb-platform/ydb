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

class CustomAudiencesharedAccountInfo(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CustomAudiencesharedAccountInfo, self).__init__()
        self._isCustomAudiencesharedAccountInfo = True
        self._api = api

    class Field(AbstractObject.Field):
        account_id = 'account_id'
        account_name = 'account_name'
        business_id = 'business_id'
        business_name = 'business_name'
        sharing_status = 'sharing_status'

    _field_types = {
        'account_id': 'string',
        'account_name': 'string',
        'business_id': 'string',
        'business_name': 'string',
        'sharing_status': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


