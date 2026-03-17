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

class PremiumMusicVideo(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isPremiumMusicVideo = True
        super(PremiumMusicVideo, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        creation_time = 'creation_time'
        cross_post_videos = 'cross_post_videos'
        eligible_cross_post_pages = 'eligible_cross_post_pages'
        id = 'id'
        preferred_video_thumbnail_image_uri = 'preferred_video_thumbnail_image_uri'
        premium_music_video_metadata = 'premium_music_video_metadata'
        scheduled_publish_time = 'scheduled_publish_time'
        title = 'title'

    _field_types = {
        'creation_time': 'string',
        'cross_post_videos': 'list<Object>',
        'eligible_cross_post_pages': 'list<Object>',
        'id': 'string',
        'preferred_video_thumbnail_image_uri': 'string',
        'premium_music_video_metadata': 'Object',
        'scheduled_publish_time': 'int',
        'title': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


