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

class AdAccountReachEstimate(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdAccountReachEstimate, self).__init__()
        self._isAdAccountReachEstimate = True
        self._api = api

    class Field(AbstractObject.Field):
        estimate_ready = 'estimate_ready'
        users_lower_bound = 'users_lower_bound'
        users_upper_bound = 'users_upper_bound'

    _field_types = {
        'estimate_ready': 'bool',
        'users_lower_bound': 'int',
        'users_upper_bound': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


