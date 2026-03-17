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

class AdEntityTargetSpend(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdEntityTargetSpend, self).__init__()
        self._isAdEntityTargetSpend = True
        self._api = api

    class Field(AbstractObject.Field):
        amount = 'amount'
        has_error = 'has_error'
        is_accurate = 'is_accurate'
        is_prorated = 'is_prorated'
        is_updating = 'is_updating'

    _field_types = {
        'amount': 'string',
        'has_error': 'bool',
        'is_accurate': 'bool',
        'is_prorated': 'bool',
        'is_updating': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


