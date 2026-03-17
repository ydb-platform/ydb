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

class Group(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isGroup = True
        super(Group, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        archived = 'archived'
        cover = 'cover'
        created_time = 'created_time'
        description = 'description'
        email = 'email'
        icon = 'icon'
        id = 'id'
        install = 'install'
        link = 'link'
        member_count = 'member_count'
        member_request_count = 'member_request_count'
        name = 'name'
        parent = 'parent'
        permissions = 'permissions'
        privacy = 'privacy'
        purpose = 'purpose'
        subdomain = 'subdomain'
        updated_time = 'updated_time'
        venue = 'venue'

    class JoinSetting:
        admin_only = 'ADMIN_ONLY'
        anyone = 'ANYONE'
        none = 'NONE'

    class PostPermissions:
        admin_only = 'ADMIN_ONLY'
        anyone = 'ANYONE'
        none = 'NONE'

    class Purpose:
        casual = 'CASUAL'
        coworkers = 'COWORKERS'
        custom = 'CUSTOM'
        for_sale = 'FOR_SALE'
        for_work = 'FOR_WORK'
        game = 'GAME'
        health_support = 'HEALTH_SUPPORT'
        jobs = 'JOBS'
        learning = 'LEARNING'
        none = 'NONE'
        parenting = 'PARENTING'
        streamer = 'STREAMER'
        work_announcement = 'WORK_ANNOUNCEMENT'
        work_demo_group = 'WORK_DEMO_GROUP'
        work_discussion = 'WORK_DISCUSSION'
        work_ephemeral = 'WORK_EPHEMERAL'
        work_feedback = 'WORK_FEEDBACK'
        work_for_sale = 'WORK_FOR_SALE'
        work_garden = 'WORK_GARDEN'
        work_integrity = 'WORK_INTEGRITY'
        work_learning = 'WORK_LEARNING'
        work_mentorship = 'WORK_MENTORSHIP'
        work_multi_company = 'WORK_MULTI_COMPANY'
        work_recruiting = 'WORK_RECRUITING'
        work_social = 'WORK_SOCIAL'
        work_stages = 'WORK_STAGES'
        work_team = 'WORK_TEAM'
        work_teamwork = 'WORK_TEAMWORK'

    class GroupType:
        casual = 'CASUAL'
        coworkers = 'COWORKERS'
        custom = 'CUSTOM'
        for_sale = 'FOR_SALE'
        for_work = 'FOR_WORK'
        game = 'GAME'
        health_support = 'HEALTH_SUPPORT'
        jobs = 'JOBS'
        learning = 'LEARNING'
        none = 'NONE'
        parenting = 'PARENTING'
        streamer = 'STREAMER'
        work_announcement = 'WORK_ANNOUNCEMENT'
        work_demo_group = 'WORK_DEMO_GROUP'
        work_discussion = 'WORK_DISCUSSION'
        work_ephemeral = 'WORK_EPHEMERAL'
        work_feedback = 'WORK_FEEDBACK'
        work_for_sale = 'WORK_FOR_SALE'
        work_garden = 'WORK_GARDEN'
        work_integrity = 'WORK_INTEGRITY'
        work_learning = 'WORK_LEARNING'
        work_mentorship = 'WORK_MENTORSHIP'
        work_multi_company = 'WORK_MULTI_COMPANY'
        work_recruiting = 'WORK_RECRUITING'
        work_social = 'WORK_SOCIAL'
        work_stages = 'WORK_STAGES'
        work_team = 'WORK_TEAM'
        work_teamwork = 'WORK_TEAMWORK'

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'icon_size': 'icon_size_enum',
        }
        enums = {
            'icon_size_enum': [
                '16',
                '34',
                '50',
                '68',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Group,
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
            'cover': 'string',
            'cover_url': 'string',
            'description': 'string',
            'focus_x': 'float',
            'focus_y': 'float',
            'group_icon': 'string',
            'is_official_group': 'bool',
            'join_setting': 'join_setting_enum',
            'name': 'string',
            'no_feed_story': 'bool',
            'offset_y': 'int',
            'post_permissions': 'post_permissions_enum',
            'post_requires_admin_approval': 'bool',
            'privacy': 'string',
            'purpose': 'purpose_enum',
            'update_view_time': 'bool',
        }
        enums = {
            'join_setting_enum': Group.JoinSetting.__dict__.values(),
            'post_permissions_enum': Group.PostPermissions.__dict__.values(),
            'purpose_enum': Group.Purpose.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Group,
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

    def delete_admins(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'uid': 'int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/admins',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def create_admin(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'uid': 'int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/admins',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Group,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Group, api=self._api),
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

    def get_albums(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.album import Album
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/albums',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Album,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Album, api=self._api),
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

    def get_docs(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/docs',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def get_events(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.event import Event
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/events',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Event,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Event, api=self._api),
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

    def get_feed(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.post import Post
        param_types = {
            'include_hidden': 'bool',
            'q': 'string',
            'show_expired': 'bool',
            'since': 'datetime',
            'until': 'datetime',
            'with': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/feed',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Post,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Post, api=self._api),
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

    def create_feed(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.post import Post
        param_types = {
            'actions': 'Object',
            'album_id': 'string',
            'android_key_hash': 'string',
            'application_id': 'string',
            'asked_fun_fact_prompt_id': 'unsigned int',
            'asset3d_id': 'string',
            'associated_id': 'string',
            'attach_place_suggestion': 'bool',
            'attached_media': 'list<Object>',
            'audience_exp': 'bool',
            'backdated_time': 'datetime',
            'backdated_time_granularity': 'backdated_time_granularity_enum',
            'breaking_news': 'bool',
            'breaking_news_expiration': 'unsigned int',
            'call_to_action': 'Object',
            'caption': 'string',
            'child_attachments': 'list<Object>',
            'client_mutation_id': 'string',
            'composer_entry_picker': 'string',
            'composer_entry_point': 'string',
            'composer_entry_time': 'unsigned int',
            'composer_session_events_log': 'string',
            'composer_session_id': 'string',
            'composer_source_surface': 'string',
            'composer_type': 'string',
            'connection_class': 'string',
            'content_attachment': 'string',
            'coordinates': 'Object',
            'cta_link': 'string',
            'cta_type': 'string',
            'description': 'string',
            'direct_share_status': 'unsigned int',
            'expanded_height': 'unsigned int',
            'expanded_width': 'unsigned int',
            'feed_targeting': 'Object',
            'formatting': 'formatting_enum',
            'fun_fact_prompt_id': 'string',
            'fun_fact_toastee_id': 'unsigned int',
            'height': 'unsigned int',
            'home_checkin_city_id': 'Object',
            'image_crops': 'map',
            'implicit_with_tags': 'list<int>',
            'instant_game_entry_point_data': 'string',
            'ios_bundle_id': 'string',
            'is_backout_draft': 'bool',
            'is_boost_intended': 'bool',
            'is_explicit_location': 'bool',
            'is_explicit_share': 'bool',
            'is_group_linking_post': 'bool',
            'is_photo_container': 'bool',
            'link': 'string',
            'location_source_id': 'string',
            'manual_privacy': 'bool',
            'message': 'string',
            'multi_share_end_card': 'bool',
            'multi_share_optimized': 'bool',
            'name': 'string',
            'nectar_module': 'string',
            'object_attachment': 'string',
            'og_action_type_id': 'string',
            'og_hide_object_attachment': 'bool',
            'og_icon_id': 'string',
            'og_object_id': 'string',
            'og_phrase': 'string',
            'og_set_profile_badge': 'bool',
            'og_suggestion_mechanism': 'string',
            'page_recommendation': 'string',
            'picture': 'string',
            'place': 'Object',
            'place_attachment_setting': 'place_attachment_setting_enum',
            'place_list': 'string',
            'place_list_data': 'list',
            'post_surfaces_blacklist': 'list<post_surfaces_blacklist_enum>',
            'posting_to_redspace': 'posting_to_redspace_enum',
            'privacy': 'string',
            'prompt_id': 'string',
            'prompt_tracking_string': 'string',
            'properties': 'Object',
            'proxied_app_id': 'string',
            'publish_event_id': 'unsigned int',
            'published': 'bool',
            'quote': 'string',
            'ref': 'list<string>',
            'referenceable_image_ids': 'list<string>',
            'referral_id': 'string',
            'scheduled_publish_time': 'datetime',
            'source': 'string',
            'sponsor_id': 'string',
            'sponsor_relationship': 'unsigned int',
            'suggested_place_id': 'Object',
            'tags': 'list<int>',
            'target_surface': 'target_surface_enum',
            'targeting': 'Object',
            'text_format_metadata': 'string',
            'text_format_preset_id': 'string',
            'text_only_place': 'string',
            'thumbnail': 'file',
            'time_since_original_post': 'unsigned int',
            'title': 'string',
            'tracking_info': 'string',
            'unpublished_content_type': 'unpublished_content_type_enum',
            'user_selected_tags': 'bool',
            'video_start_time_ms': 'unsigned int',
            'viewer_coordinates': 'Object',
            'width': 'unsigned int',
        }
        enums = {
            'backdated_time_granularity_enum': Post.BackdatedTimeGranularity.__dict__.values(),
            'formatting_enum': Post.Formatting.__dict__.values(),
            'place_attachment_setting_enum': Post.PlaceAttachmentSetting.__dict__.values(),
            'post_surfaces_blacklist_enum': Post.PostSurfacesBlacklist.__dict__.values(),
            'posting_to_redspace_enum': Post.PostingToRedspace.__dict__.values(),
            'target_surface_enum': Post.TargetSurface.__dict__.values(),
            'unpublished_content_type_enum': Post.UnpublishedContentType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/feed',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Post,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Post, api=self._api),
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

    def get_files(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/files',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def get_groups(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/groups',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Group,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Group, api=self._api),
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

    def create_group(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'admin': 'int',
            'description': 'string',
            'group_icon_id': 'string',
            'group_type': 'group_type_enum',
            'join_setting': 'join_setting_enum',
            'name': 'string',
            'parent_id': 'string',
            'post_permissions': 'post_permissions_enum',
            'post_requires_admin_approval': 'bool',
            'privacy': 'string',
            'ref': 'string',
        }
        enums = {
            'group_type_enum': Group.GroupType.__dict__.values(),
            'join_setting_enum': Group.JoinSetting.__dict__.values(),
            'post_permissions_enum': Group.PostPermissions.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/groups',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Group,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Group, api=self._api),
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

    def get_live_videos(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.livevideo import LiveVideo
        param_types = {
            'broadcast_status': 'list<broadcast_status_enum>',
            'source': 'source_enum',
        }
        enums = {
            'broadcast_status_enum': LiveVideo.BroadcastStatus.__dict__.values(),
            'source_enum': LiveVideo.Source.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/live_videos',
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

    def create_live_video(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.livevideo import LiveVideo
        param_types = {
            'content_tags': 'list<string>',
            'description': 'string',
            'enable_backup_ingest': 'bool',
            'encoding_settings': 'string',
            'event_params': 'Object',
            'fisheye_video_cropped': 'bool',
            'front_z_rotation': 'float',
            'is_audio_only': 'bool',
            'is_spherical': 'bool',
            'original_fov': 'unsigned int',
            'privacy': 'string',
            'projection': 'projection_enum',
            'published': 'bool',
            'schedule_custom_profile_image': 'file',
            'spatial_audio_format': 'spatial_audio_format_enum',
            'status': 'status_enum',
            'stereoscopic_mode': 'stereoscopic_mode_enum',
            'stop_on_delete_stream': 'bool',
            'stream_type': 'stream_type_enum',
            'title': 'string',
        }
        enums = {
            'projection_enum': LiveVideo.Projection.__dict__.values(),
            'spatial_audio_format_enum': LiveVideo.SpatialAudioFormat.__dict__.values(),
            'status_enum': LiveVideo.Status.__dict__.values(),
            'stereoscopic_mode_enum': LiveVideo.StereoscopicMode.__dict__.values(),
            'stream_type_enum': LiveVideo.StreamType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/live_videos',
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

    def delete_members(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'email': 'string',
            'member': 'int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/members',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AbstractCrudObject,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AbstractCrudObject, api=self._api),
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

    def create_member(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'email': 'string',
            'from': 'int',
            'member': 'int',
            'rate': 'unsigned int',
            'source': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/members',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Group,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Group, api=self._api),
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

    def get_opted_in_members(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.user import User
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/opted_in_members',
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

    def create_photo(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.photo import Photo
        param_types = {
            'aid': 'string',
            'allow_spherical_photo': 'bool',
            'alt_text_custom': 'string',
            'android_key_hash': 'string',
            'application_id': 'string',
            'attempt': 'unsigned int',
            'audience_exp': 'bool',
            'backdated_time': 'datetime',
            'backdated_time_granularity': 'backdated_time_granularity_enum',
            'caption': 'string',
            'composer_session_id': 'string',
            'direct_share_status': 'unsigned int',
            'feed_targeting': 'Object',
            'filter_type': 'unsigned int',
            'full_res_is_coming_later': 'bool',
            'initial_view_heading_override_degrees': 'unsigned int',
            'initial_view_pitch_override_degrees': 'unsigned int',
            'initial_view_vertical_fov_override_degrees': 'unsigned int',
            'ios_bundle_id': 'string',
            'is_explicit_location': 'bool',
            'is_explicit_place': 'bool',
            'manual_privacy': 'bool',
            'message': 'string',
            'name': 'string',
            'no_story': 'bool',
            'offline_id': 'unsigned int',
            'og_action_type_id': 'string',
            'og_icon_id': 'string',
            'og_object_id': 'string',
            'og_phrase': 'string',
            'og_set_profile_badge': 'bool',
            'og_suggestion_mechanism': 'string',
            'place': 'Object',
            'privacy': 'string',
            'profile_id': 'int',
            'provenance_info': 'map',
            'proxied_app_id': 'string',
            'published': 'bool',
            'qn': 'string',
            'spherical_metadata': 'map',
            'sponsor_id': 'string',
            'sponsor_relationship': 'unsigned int',
            'tags': 'list<Object>',
            'target_id': 'int',
            'targeting': 'Object',
            'time_since_original_post': 'unsigned int',
            'uid': 'int',
            'unpublished_content_type': 'unpublished_content_type_enum',
            'url': 'string',
            'user_selected_tags': 'bool',
            'vault_image_id': 'string',
        }
        enums = {
            'backdated_time_granularity_enum': Photo.BackdatedTimeGranularity.__dict__.values(),
            'unpublished_content_type_enum': Photo.UnpublishedContentType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/photos',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Photo,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Photo, api=self._api),
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

    def get_picture(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.profilepicturesource import ProfilePictureSource
        param_types = {
            'height': 'int',
            'redirect': 'bool',
            'type': 'type_enum',
            'width': 'int',
        }
        enums = {
            'type_enum': ProfilePictureSource.Type.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/picture',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProfilePictureSource,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProfilePictureSource, api=self._api),
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

    def get_videos(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.advideo import AdVideo
        param_types = {
            'type': 'type_enum',
        }
        enums = {
            'type_enum': AdVideo.Type.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/videos',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdVideo,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdVideo, api=self._api),
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

    def create_video(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.advideo import AdVideo
        param_types = {
            'application_id': 'string',
            'asked_fun_fact_prompt_id': 'unsigned int',
            'audio_story_wave_animation_handle': 'string',
            'composer_entry_picker': 'string',
            'composer_entry_point': 'string',
            'composer_entry_time': 'unsigned int',
            'composer_session_events_log': 'string',
            'composer_session_id': 'string',
            'composer_source_surface': 'string',
            'composer_type': 'string',
            'container_type': 'container_type_enum',
            'content_category': 'content_category_enum',
            'creative_tools': 'string',
            'description': 'string',
            'embeddable': 'bool',
            'end_offset': 'unsigned int',
            'fbuploader_video_file_chunk': 'string',
            'file_size': 'unsigned int',
            'file_url': 'string',
            'fisheye_video_cropped': 'bool',
            'formatting': 'formatting_enum',
            'fov': 'unsigned int',
            'front_z_rotation': 'float',
            'fun_fact_prompt_id': 'string',
            'fun_fact_toastee_id': 'unsigned int',
            'guide': 'list<list<unsigned int>>',
            'guide_enabled': 'bool',
            'initial_heading': 'unsigned int',
            'initial_pitch': 'unsigned int',
            'instant_game_entry_point_data': 'string',
            'is_boost_intended': 'bool',
            'is_explicit_share': 'bool',
            'is_group_linking_post': 'bool',
            'is_partnership_ad': 'bool',
            'is_voice_clip': 'bool',
            'location_source_id': 'string',
            'manual_privacy': 'bool',
            'og_action_type_id': 'string',
            'og_icon_id': 'string',
            'og_object_id': 'string',
            'og_phrase': 'string',
            'og_suggestion_mechanism': 'string',
            'original_fov': 'unsigned int',
            'original_projection_type': 'original_projection_type_enum',
            'partnership_ad_ad_code': 'string',
            'publish_event_id': 'unsigned int',
            'published': 'bool',
            'referenced_sticker_id': 'string',
            'replace_video_id': 'string',
            'scheduled_publish_time': 'unsigned int',
            'slideshow_spec': 'map',
            'source': 'string',
            'source_instagram_media_id': 'string',
            'spherical': 'bool',
            'start_offset': 'unsigned int',
            'swap_mode': 'swap_mode_enum',
            'text_format_metadata': 'string',
            'thumb': 'file',
            'time_since_original_post': 'unsigned int',
            'title': 'string',
            'transcode_setting_properties': 'string',
            'unpublished_content_type': 'unpublished_content_type_enum',
            'upload_phase': 'upload_phase_enum',
            'upload_session_id': 'string',
            'upload_setting_properties': 'string',
            'video_file_chunk': 'string',
            'video_id_original': 'string',
            'video_start_time_ms': 'unsigned int',
            'waterfall_id': 'string',
        }
        enums = {
            'container_type_enum': AdVideo.ContainerType.__dict__.values(),
            'content_category_enum': AdVideo.ContentCategory.__dict__.values(),
            'formatting_enum': AdVideo.Formatting.__dict__.values(),
            'original_projection_type_enum': AdVideo.OriginalProjectionType.__dict__.values(),
            'swap_mode_enum': AdVideo.SwapMode.__dict__.values(),
            'unpublished_content_type_enum': AdVideo.UnpublishedContentType.__dict__.values(),
            'upload_phase_enum': AdVideo.UploadPhase.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/videos',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdVideo,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdVideo, api=self._api),
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
        'archived': 'bool',
        'cover': 'CoverPhoto',
        'created_time': 'datetime',
        'description': 'string',
        'email': 'string',
        'icon': 'string',
        'id': 'string',
        'install': 'Object',
        'link': 'string',
        'member_count': 'unsigned int',
        'member_request_count': 'unsigned int',
        'name': 'string',
        'parent': 'Object',
        'permissions': 'list<string>',
        'privacy': 'string',
        'purpose': 'string',
        'subdomain': 'string',
        'updated_time': 'datetime',
        'venue': 'Location',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['JoinSetting'] = Group.JoinSetting.__dict__.values()
        field_enum_info['PostPermissions'] = Group.PostPermissions.__dict__.values()
        field_enum_info['Purpose'] = Group.Purpose.__dict__.values()
        field_enum_info['GroupType'] = Group.GroupType.__dict__.values()
        return field_enum_info


