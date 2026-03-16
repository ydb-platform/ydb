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

class ALMGuidance(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ALMGuidance, self).__init__()
        self._isALMGuidance = True
        self._api = api

    class Field(AbstractObject.Field):
        ad_account_id = 'ad_account_id'
        guidances = 'guidances'
        opportunity_score = 'opportunity_score'
        parent_advertiser_id = 'parent_advertiser_id'
        parent_advertiser_name = 'parent_advertiser_name'

    _field_types = {
        'ad_account_id': 'string',
        'guidances': 'list<Object>',
        'opportunity_score': 'float',
        'parent_advertiser_id': 'string',
        'parent_advertiser_name': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


