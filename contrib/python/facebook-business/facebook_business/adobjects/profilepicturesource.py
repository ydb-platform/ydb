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

class ProfilePictureSource(
    AbstractObject,
):

    def __init__(self, api=None):
        super(ProfilePictureSource, self).__init__()
        self._isProfilePictureSource = True
        self._api = api

    class Field(AbstractObject.Field):
        bottom = 'bottom'
        cache_key = 'cache_key'
        height = 'height'
        is_silhouette = 'is_silhouette'
        left = 'left'
        right = 'right'
        top = 'top'
        url = 'url'
        width = 'width'

    class Type:
        album = 'album'
        small = 'small'
        thumbnail = 'thumbnail'

    _field_types = {
        'bottom': 'unsigned int',
        'cache_key': 'string',
        'height': 'unsigned int',
        'is_silhouette': 'bool',
        'left': 'unsigned int',
        'right': 'unsigned int',
        'top': 'unsigned int',
        'url': 'string',
        'width': 'unsigned int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Type'] = ProfilePictureSource.Type.__dict__.values()
        return field_enum_info


