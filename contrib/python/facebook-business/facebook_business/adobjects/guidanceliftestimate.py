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

class GuidanceLiftEstimate(
    AbstractObject,
):

    def __init__(self, api=None):
        super(GuidanceLiftEstimate, self).__init__()
        self._isGuidanceLiftEstimate = True
        self._api = api

    class Field(AbstractObject.Field):
        actual_7d_cpr = 'actual_7d_cpr'
        adoption_date = 'adoption_date'
        guidance_name = 'guidance_name'
        lift_estimation = 'lift_estimation'
        predicted_7d_cpr = 'predicted_7d_cpr'

    _field_types = {
        'actual_7d_cpr': 'float',
        'adoption_date': 'string',
        'guidance_name': 'string',
        'lift_estimation': 'float',
        'predicted_7d_cpr': 'float',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


