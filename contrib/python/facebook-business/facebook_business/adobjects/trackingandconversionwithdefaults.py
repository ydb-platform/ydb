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

class TrackingAndConversionWithDefaults(
    AbstractObject,
):

    def __init__(self, api=None):
        super(TrackingAndConversionWithDefaults, self).__init__()
        self._isTrackingAndConversionWithDefaults = True
        self._api = api

    class Field(AbstractObject.Field):
        custom_conversion = 'custom_conversion'
        custom_tracking = 'custom_tracking'
        default_conversion = 'default_conversion'
        default_tracking = 'default_tracking'

    _field_types = {
        'custom_conversion': 'list<Object>',
        'custom_tracking': 'list<Object>',
        'default_conversion': 'list<Object>',
        'default_tracking': 'list<Object>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


