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

class AdBidAdjustments(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdBidAdjustments, self).__init__()
        self._isAdBidAdjustments = True
        self._api = api

    class Field(AbstractObject.Field):
        age_range = 'age_range'
        page_types = 'page_types'
        user_groups = 'user_groups'

    _field_types = {
        'age_range': 'map<string, float>',
        'page_types': 'Object',
        'user_groups': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


