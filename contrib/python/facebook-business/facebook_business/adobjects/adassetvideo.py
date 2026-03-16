# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class AdAssetVideo(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isAdAssetVideo = True
        super(AdAssetVideo, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        caption_ids = 'caption_ids'
        id = 'id'
        source_image_url = 'source_image_url'
        tag = 'tag'
        thumbnail_hash = 'thumbnail_hash'
        thumbnail_source = 'thumbnail_source'
        thumbnail_url = 'thumbnail_url'
        url = 'url'
        url_tags = 'url_tags'
        video_id = 'video_id'
        video_name = 'video_name'

    _field_types = {
        'caption_ids': 'list<string>',
        'id': 'string',
        'source_image_url': 'string',
        'tag': 'string',
        'thumbnail_hash': 'string',
        'thumbnail_source': 'string',
        'thumbnail_url': 'string',
        'url': 'string',
        'url_tags': 'string',
        'video_id': 'string',
        'video_name': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


