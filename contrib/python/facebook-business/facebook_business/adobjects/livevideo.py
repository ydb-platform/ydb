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

class LiveVideo(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isLiveVideo = True
        super(LiveVideo, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        ad_break_config = 'ad_break_config'
        ad_break_failure_reason = 'ad_break_failure_reason'
        broadcast_start_time = 'broadcast_start_time'
        copyright = 'copyright'
        creation_time = 'creation_time'
        dash_ingest_url = 'dash_ingest_url'
        dash_preview_url = 'dash_preview_url'
        description = 'description'
        embed_html = 'embed_html'
        field_from = 'from'
        id = 'id'
        ingest_streams = 'ingest_streams'
        is_manual_mode = 'is_manual_mode'
        is_reference_only = 'is_reference_only'
        live_views = 'live_views'
        permalink_url = 'permalink_url'
        planned_start_time = 'planned_start_time'
        recommended_encoder_settings = 'recommended_encoder_settings'
        seconds_left = 'seconds_left'
        secure_stream_url = 'secure_stream_url'
        status = 'status'
        stream_url = 'stream_url'
        targeting = 'targeting'
        title = 'title'
        total_views = 'total_views'
        video = 'video'

    class Projection:
        cubemap = 'CUBEMAP'
        equirectangular = 'EQUIRECTANGULAR'
        half_equirectangular = 'HALF_EQUIRECTANGULAR'

    class SpatialAudioFormat:
        ambix_4 = 'ambiX_4'

    class Status:
        live_now = 'LIVE_NOW'
        scheduled_canceled = 'SCHEDULED_CANCELED'
        scheduled_live = 'SCHEDULED_LIVE'
        scheduled_unpublished = 'SCHEDULED_UNPUBLISHED'
        unpublished = 'UNPUBLISHED'

    class StereoscopicMode:
        left_right = 'LEFT_RIGHT'
        mono = 'MONO'
        top_bottom = 'TOP_BOTTOM'

    class StreamType:
        ambient = 'AMBIENT'
        regular = 'REGULAR'

    class BroadcastStatus:
        live = 'LIVE'
        live_stopped = 'LIVE_STOPPED'
        processing = 'PROCESSING'
        scheduled_canceled = 'SCHEDULED_CANCELED'
        scheduled_expired = 'SCHEDULED_EXPIRED'
        scheduled_live = 'SCHEDULED_LIVE'
        scheduled_unpublished = 'SCHEDULED_UNPUBLISHED'
        unpublished = 'UNPUBLISHED'
        vod = 'VOD'

    class Source:
        owner = 'owner'
        target = 'target'

    class LiveCommentModerationSetting:
        value_default = 'DEFAULT'
        discussion = 'DISCUSSION'
        followed = 'FOLLOWED'
        follower = 'FOLLOWER'
        no_hyperlink = 'NO_HYPERLINK'
        protected_mode = 'PROTECTED_MODE'
        restricted = 'RESTRICTED'
        slow = 'SLOW'
        supporter = 'SUPPORTER'
        tagged = 'TAGGED'

    class PersistentStreamKeyStatus:
        disable = 'DISABLE'
        enable = 'ENABLE'
        regenerate = 'REGENERATE'

    def api_delete(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
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

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'target_token': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=LiveVideo,
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

    def api_update(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'allow_bm_crossposting': 'bool',
            'content_tags': 'list<string>',
            'cross_share_to_group_ids': 'list<string>',
            'crossposting_actions': 'list<map>',
            'custom_labels': 'list<string>',
            'description': 'string',
            'direct_share_status': 'unsigned int',
            'embeddable': 'bool',
            'end_live_video': 'bool',
            'event_params': 'Object',
            'is_audio_only': 'bool',
            'is_manual_mode': 'bool',
            'live_comment_moderation_setting': 'list<live_comment_moderation_setting_enum>',
            'master_ingest_stream_id': 'string',
            'og_icon_id': 'string',
            'og_phrase': 'string',
            'persistent_stream_key_status': 'persistent_stream_key_status_enum',
            'place': 'Object',
            'planned_start_time': 'datetime',
            'privacy': 'string',
            'published': 'bool',
            'schedule_custom_profile_image': 'file',
            'schedule_feed_background_image': 'file',
            'sponsor_id': 'string',
            'sponsor_relationship': 'unsigned int',
            'status': 'status_enum',
            'stream_type': 'stream_type_enum',
            'tags': 'list<int>',
            'targeting': 'Object',
            'title': 'string',
        }
        enums = {
            'live_comment_moderation_setting_enum': LiveVideo.LiveCommentModerationSetting.__dict__.values(),
            'persistent_stream_key_status_enum': LiveVideo.PersistentStreamKeyStatus.__dict__.values(),
            'status_enum': LiveVideo.Status.__dict__.values(),
            'stream_type_enum': LiveVideo.StreamType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=LiveVideo,
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

    def get_blocked_users(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.user import User
        param_types = {
            'uid': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/blocked_users',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=User,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=User, api=self._api),
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

    def get_comments(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.comment import Comment
        param_types = {
            'filter': 'filter_enum',
            'live_filter': 'live_filter_enum',
            'order': 'order_enum',
            'since': 'datetime',
        }
        enums = {
            'filter_enum': Comment.Filter.__dict__.values(),
            'live_filter_enum': Comment.LiveFilter.__dict__.values(),
            'order_enum': Comment.Order.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/comments',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Comment,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Comment, api=self._api),
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

    def get_crosspost_shared_pages(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.page import Page
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/crosspost_shared_pages',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Page,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Page, api=self._api),
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

    def get_crossposted_broadcasts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/crossposted_broadcasts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=LiveVideo,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=LiveVideo, api=self._api),
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

    def get_errors(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.livevideoerror import LiveVideoError
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/errors',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=LiveVideoError,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=LiveVideoError, api=self._api),
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

    def create_input_stream(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.livevideoinputstream import LiveVideoInputStream
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/input_streams',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=LiveVideoInputStream,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=LiveVideoInputStream, api=self._api),
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

    def get_polls(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.videopoll import VideoPoll
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/polls',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=VideoPoll,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=VideoPoll, api=self._api),
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

    def create_poll(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.videopoll import VideoPoll
        param_types = {
            'close_after_voting': 'bool',
            'correct_option': 'unsigned int',
            'default_open': 'bool',
            'options': 'list<string>',
            'question': 'string',
            'show_gradient': 'bool',
            'show_results': 'bool',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/polls',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=VideoPoll,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=VideoPoll, api=self._api),
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

    def get_reactions(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.profile import Profile
        param_types = {
            'type': 'type_enum',
        }
        enums = {
            'type_enum': Profile.Type.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/reactions',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Profile,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Profile, api=self._api),
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
        'ad_break_config': 'LiveVideoAdBreakConfig',
        'ad_break_failure_reason': 'string',
        'broadcast_start_time': 'datetime',
        'copyright': 'VideoCopyright',
        'creation_time': 'datetime',
        'dash_ingest_url': 'string',
        'dash_preview_url': 'string',
        'description': 'string',
        'embed_html': 'Object',
        'from': 'Object',
        'id': 'string',
        'ingest_streams': 'list<LiveVideoInputStream>',
        'is_manual_mode': 'bool',
        'is_reference_only': 'bool',
        'live_views': 'unsigned int',
        'permalink_url': 'string',
        'planned_start_time': 'datetime',
        'recommended_encoder_settings': 'LiveVideoRecommendedEncoderSettings',
        'seconds_left': 'int',
        'secure_stream_url': 'string',
        'status': 'string',
        'stream_url': 'string',
        'targeting': 'LiveVideoTargeting',
        'title': 'string',
        'total_views': 'string',
        'video': 'AdVideo',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Projection'] = LiveVideo.Projection.__dict__.values()
        field_enum_info['SpatialAudioFormat'] = LiveVideo.SpatialAudioFormat.__dict__.values()
        field_enum_info['Status'] = LiveVideo.Status.__dict__.values()
        field_enum_info['StereoscopicMode'] = LiveVideo.StereoscopicMode.__dict__.values()
        field_enum_info['StreamType'] = LiveVideo.StreamType.__dict__.values()
        field_enum_info['BroadcastStatus'] = LiveVideo.BroadcastStatus.__dict__.values()
        field_enum_info['Source'] = LiveVideo.Source.__dict__.values()
        field_enum_info['LiveCommentModerationSetting'] = LiveVideo.LiveCommentModerationSetting.__dict__.values()
        field_enum_info['PersistentStreamKeyStatus'] = LiveVideo.PersistentStreamKeyStatus.__dict__.values()
        return field_enum_info


