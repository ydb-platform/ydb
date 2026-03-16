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

class PhotoMetadata(
    AbstractObject,
):

    def __init__(self, api=None):
        super(PhotoMetadata, self).__init__()
        self._isPhotoMetadata = True
        self._api = api

    class Field(AbstractObject.Field):
        camera_make = 'camera_make'
        camera_model = 'camera_model'
        datetime_modified = 'datetime_modified'
        datetime_taken = 'datetime_taken'
        exposure = 'exposure'
        focal_length = 'focal_length'
        fstop = 'fstop'
        iso_speed = 'iso_speed'
        offline_id = 'offline_id'
        orientation = 'orientation'
        original_height = 'original_height'
        original_width = 'original_width'

    _field_types = {
        'camera_make': 'string',
        'camera_model': 'string',
        'datetime_modified': 'datetime',
        'datetime_taken': 'datetime',
        'exposure': 'string',
        'focal_length': 'string',
        'fstop': 'string',
        'iso_speed': 'int',
        'offline_id': 'string',
        'orientation': 'string',
        'original_height': 'string',
        'original_width': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


