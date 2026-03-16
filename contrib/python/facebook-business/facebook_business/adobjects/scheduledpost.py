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

class ScheduledPost(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isScheduledPost = True
        super(ScheduledPost, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        admin_creator = 'admin_creator'
        creation_time = 'creation_time'
        feed_audience_description = 'feed_audience_description'
        feed_targeting = 'feed_targeting'
        id = 'id'
        is_post_in_good_state = 'is_post_in_good_state'
        message = 'message'
        modified_time = 'modified_time'
        og_action_summary = 'og_action_summary'
        permalink_url = 'permalink_url'
        place = 'place'
        privacy_description = 'privacy_description'
        scheduled_failure_notice = 'scheduled_failure_notice'
        scheduled_publish_time = 'scheduled_publish_time'
        story_token = 'story_token'
        thumbnail = 'thumbnail'
        video_id = 'video_id'

    _field_types = {
        'admin_creator': 'User',
        'creation_time': 'datetime',
        'feed_audience_description': 'string',
        'feed_targeting': 'Targeting',
        'id': 'string',
        'is_post_in_good_state': 'bool',
        'message': 'string',
        'modified_time': 'datetime',
        'og_action_summary': 'string',
        'permalink_url': 'string',
        'place': 'Place',
        'privacy_description': 'string',
        'scheduled_failure_notice': 'string',
        'scheduled_publish_time': 'datetime',
        'story_token': 'string',
        'thumbnail': 'string',
        'video_id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


