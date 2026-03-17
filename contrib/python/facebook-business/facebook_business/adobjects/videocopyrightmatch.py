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

class VideoCopyrightMatch(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isVideoCopyrightMatch = True
        super(VideoCopyrightMatch, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        created_date = 'created_date'
        id = 'id'
        last_modified_user = 'last_modified_user'
        match_data = 'match_data'
        match_status = 'match_status'
        notes = 'notes'
        permalink = 'permalink'
        policy_eval_modify_reasons = 'policy_eval_modify_reasons'
        ugc_content_format = 'ugc_content_format'

    class Action:
        block = 'BLOCK'
        claim_ad_earnings = 'CLAIM_AD_EARNINGS'
        manual_review = 'MANUAL_REVIEW'
        monitor = 'MONITOR'
        request_takedown = 'REQUEST_TAKEDOWN'

    class ActionReason:
        article_17_preflagging = 'ARTICLE_17_PREFLAGGING'
        artist_objection = 'ARTIST_OBJECTION'
        objectionable_content = 'OBJECTIONABLE_CONTENT'
        premium_music_video = 'PREMIUM_MUSIC_VIDEO'
        prerelease_content = 'PRERELEASE_CONTENT'
        product_parameters = 'PRODUCT_PARAMETERS'
        restricted_content = 'RESTRICTED_CONTENT'
        unauthorized_commercial_use = 'UNAUTHORIZED_COMMERCIAL_USE'

    class MatchContentType:
        audio_only = 'AUDIO_ONLY'
        video_and_audio = 'VIDEO_AND_AUDIO'
        video_only = 'VIDEO_ONLY'

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=VideoCopyrightMatch,
            api_type='NODE',
            response_parser=ObjectParser(reuse_object=self),
        )
        request.add_params(params)
        request.add_fields(fields)

        if batch is not None:
            request.add_to_batch(batch, success=success, failure=failure)
            return request
        elif pending:
            return request
        else:
            self.assure_call()
            return request.execute()

    _field_types = {
        'created_date': 'datetime',
        'id': 'string',
        'last_modified_user': 'User',
        'match_data': 'list<Object>',
        'match_status': 'string',
        'notes': 'string',
        'permalink': 'string',
        'policy_eval_modify_reasons': 'list<Object>',
        'ugc_content_format': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Action'] = VideoCopyrightMatch.Action.__dict__.values()
        field_enum_info['ActionReason'] = VideoCopyrightMatch.ActionReason.__dict__.values()
        field_enum_info['MatchContentType'] = VideoCopyrightMatch.MatchContentType.__dict__.values()
        return field_enum_info


