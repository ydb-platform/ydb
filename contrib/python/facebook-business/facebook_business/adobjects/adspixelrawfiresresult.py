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

class AdsPixelRawFiresResult(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdsPixelRawFiresResult, self).__init__()
        self._isAdsPixelRawFiresResult = True
        self._api = api

    class Field(AbstractObject.Field):
        data_json = 'data_json'
        device_type = 'device_type'
        event = 'event'
        event_detection_method = 'event_detection_method'
        event_src = 'event_src'
        placed_url = 'placed_url'
        timestamp = 'timestamp'
        user_pii_keys = 'user_pii_keys'

    _field_types = {
        'data_json': 'string',
        'device_type': 'string',
        'event': 'string',
        'event_detection_method': 'string',
        'event_src': 'string',
        'placed_url': 'string',
        'timestamp': 'datetime',
        'user_pii_keys': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


