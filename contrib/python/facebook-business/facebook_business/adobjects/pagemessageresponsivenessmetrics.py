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

class PageMessageResponsivenessMetrics(
    AbstractObject,
):

    def __init__(self, api=None):
        super(PageMessageResponsivenessMetrics, self).__init__()
        self._isPageMessageResponsivenessMetrics = True
        self._api = api

    class Field(AbstractObject.Field):
        is_very_responsive = 'is_very_responsive'
        response_rate = 'response_rate'
        response_time = 'response_time'

    _field_types = {
        'is_very_responsive': 'bool',
        'response_rate': 'float',
        'response_time': 'float',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


