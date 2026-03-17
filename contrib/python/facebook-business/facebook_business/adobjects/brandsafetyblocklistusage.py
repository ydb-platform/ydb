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

class BrandSafetyBlockListUsage(
    AbstractObject,
):

    def __init__(self, api=None):
        super(BrandSafetyBlockListUsage, self).__init__()
        self._isBrandSafetyBlockListUsage = True
        self._api = api

    class Field(AbstractObject.Field):
        current_usage = 'current_usage'
        new_usage = 'new_usage'
        platform = 'platform'
        position = 'position'
        threshold = 'threshold'

    _field_types = {
        'current_usage': 'int',
        'new_usage': 'int',
        'platform': 'string',
        'position': 'string',
        'threshold': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


