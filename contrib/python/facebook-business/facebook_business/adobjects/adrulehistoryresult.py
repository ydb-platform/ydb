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

class AdRuleHistoryResult(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdRuleHistoryResult, self).__init__()
        self._isAdRuleHistoryResult = True
        self._api = api

    class Field(AbstractObject.Field):
        actions = 'actions'
        object_id = 'object_id'
        object_type = 'object_type'

    class ObjectType:
        ad = 'AD'
        adset = 'ADSET'
        campaign = 'CAMPAIGN'

    _field_types = {
        'actions': 'list<AdRuleHistoryResultAction>',
        'object_id': 'string',
        'object_type': 'ObjectType',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['ObjectType'] = AdRuleHistoryResult.ObjectType.__dict__.values()
        return field_enum_info


