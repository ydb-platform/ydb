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

class VideoGroup(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isVideoGroup = True
        super(VideoGroup, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        created_time = 'created_time'
        description = 'description'
        disable_reason = 'disable_reason'
        id = 'id'
        ig_profile_ids = 'ig_profile_ids'
        is_disabled = 'is_disabled'
        is_fb_video_group = 'is_fb_video_group'
        last_used_time = 'last_used_time'
        length = 'length'
        name = 'name'
        page_id = 'page_id'
        page_ids = 'page_ids'
        picture = 'picture'
        placements = 'placements'
        video_group_types = 'video_group_types'
        videos = 'videos'
        views = 'views'

    _field_types = {
        'created_time': 'string',
        'description': 'string',
        'disable_reason': 'string',
        'id': 'string',
        'ig_profile_ids': 'list<string>',
        'is_disabled': 'bool',
        'is_fb_video_group': 'bool',
        'last_used_time': 'string',
        'length': 'float',
        'name': 'string',
        'page_id': 'string',
        'page_ids': 'list<string>',
        'picture': 'string',
        'placements': 'list<string>',
        'video_group_types': 'list<string>',
        'videos': 'list<string>',
        'views': 'unsigned int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


