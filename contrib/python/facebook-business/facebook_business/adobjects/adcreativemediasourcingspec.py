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

class AdCreativeMediaSourcingSpec(
    AbstractObject,
):

    def __init__(self, api=None):
        super(AdCreativeMediaSourcingSpec, self).__init__()
        self._isAdCreativeMediaSourcingSpec = True
        self._api = api

    class Field(AbstractObject.Field):
        bodies = 'bodies'
        descriptions = 'descriptions'
        images = 'images'
        push_metadata_ids = 'push_metadata_ids'
        related_media = 'related_media'
        titles = 'titles'
        videos = 'videos'

    _field_types = {
        'bodies': 'list<Object>',
        'descriptions': 'list<Object>',
        'images': 'list<Object>',
        'push_metadata_ids': 'list<unsigned int>',
        'related_media': 'Object',
        'titles': 'list<Object>',
        'videos': 'list<Object>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


