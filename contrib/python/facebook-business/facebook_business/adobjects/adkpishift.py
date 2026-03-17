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

class AdKpiShift(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdKpiShift, self).__init__()
        self._isAdKpiShift = True
        self._api = api

    class Field(AbstractObject.Field):
        ad_set = 'ad_set'
        cost_per_result_shift = 'cost_per_result_shift'
        enough_effective_days = 'enough_effective_days'
        result_indicator = 'result_indicator'
        result_shift = 'result_shift'
        spend_shift = 'spend_shift'

    _field_types = {
        'ad_set': 'AdSet',
        'cost_per_result_shift': 'float',
        'enough_effective_days': 'bool',
        'result_indicator': 'string',
        'result_shift': 'float',
        'spend_shift': 'float',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


