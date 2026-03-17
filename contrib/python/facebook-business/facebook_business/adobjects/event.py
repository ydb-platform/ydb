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

class Event(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isEvent = True
        super(Event, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        attending_count = 'attending_count'
        can_guests_invite = 'can_guests_invite'
        category = 'category'
        cover = 'cover'
        created_time = 'created_time'
        declined_count = 'declined_count'
        description = 'description'
        discount_code_enabled = 'discount_code_enabled'
        end_time = 'end_time'
        event_times = 'event_times'
        guest_list_enabled = 'guest_list_enabled'
        id = 'id'
        interested_count = 'interested_count'
        is_canceled = 'is_canceled'
        is_draft = 'is_draft'
        is_online = 'is_online'
        is_page_owned = 'is_page_owned'
        maybe_count = 'maybe_count'
        name = 'name'
        noreply_count = 'noreply_count'
        online_event_format = 'online_event_format'
        online_event_third_party_url = 'online_event_third_party_url'
        owner = 'owner'
        parent_group = 'parent_group'
        place = 'place'
        registration_setting = 'registration_setting'
        scheduled_publish_time = 'scheduled_publish_time'
        start_time = 'start_time'
        ticket_setting = 'ticket_setting'
        ticket_uri = 'ticket_uri'
        ticket_uri_start_sales_time = 'ticket_uri_start_sales_time'
        ticketing_privacy_uri = 'ticketing_privacy_uri'
        ticketing_terms_uri = 'ticketing_terms_uri'
        timezone = 'timezone'
        type = 'type'
        updated_time = 'updated_time'

    class Category:
        classic_literature = 'CLASSIC_LITERATURE'
        comedy = 'COMEDY'
        crafts = 'CRAFTS'
        dance = 'DANCE'
        drinks = 'DRINKS'
        fitness_and_workouts = 'FITNESS_AND_WORKOUTS'
        foods = 'FOODS'
        games = 'GAMES'
        gardening = 'GARDENING'
        healthy_living_and_self_care = 'HEALTHY_LIVING_AND_SELF_CARE'
        health_and_medical = 'HEALTH_AND_MEDICAL'
        home_and_garden = 'HOME_AND_GARDEN'
        music_and_audio = 'MUSIC_AND_AUDIO'
        parties = 'PARTIES'
        professional_networking = 'PROFESSIONAL_NETWORKING'
        religions = 'RELIGIONS'
        shopping_event = 'SHOPPING_EVENT'
        social_issues = 'SOCIAL_ISSUES'
        sports = 'SPORTS'
        theater = 'THEATER'
        tv_and_movies = 'TV_AND_MOVIES'
        visual_arts = 'VISUAL_ARTS'

    class OnlineEventFormat:
        fb_live = 'fb_live'
        horizon_event = 'horizon_event'
        messenger_room = 'messenger_room'
        none = 'none'
        other = 'other'
        third_party = 'third_party'

    class Type:
        community = 'community'
        friends = 'friends'
        group = 'group'
        messenger_community = 'messenger_community'
        private = 'private'
        public = 'public'
        work_company = 'work_company'

    class EventStateFilter:
        canceled = 'canceled'
        draft = 'draft'
        published = 'published'
        scheduled_draft_for_publication = 'scheduled_draft_for_publication'

    class TimeFilter:
        past = 'past'
        upcoming = 'upcoming'

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
            target_class=Event,
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

    def get_comments(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.nullnode import NullNode
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/comments',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=NullNode,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=NullNode, api=self._api),
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
        from facebook_business.adobjects.nullnode import NullNode
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/feed',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=NullNode,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=NullNode, api=self._api),
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
        from facebook_business.adobjects.nullnode import NullNode
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/live_videos',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=NullNode,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=NullNode, api=self._api),
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

    def get_photos(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.nullnode import NullNode
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/photos',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=NullNode,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=NullNode, api=self._api),
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
        from facebook_business.adobjects.nullnode import NullNode
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/picture',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=NullNode,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=NullNode, api=self._api),
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

    def get_posts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.nullnode import NullNode
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/posts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=NullNode,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=NullNode, api=self._api),
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

    def get_roles(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.profile import Profile
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/roles',
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

    def get_ticket_tiers(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.eventtickettier import EventTicketTier
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/ticket_tiers',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=EventTicketTier,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=EventTicketTier, api=self._api),
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
        from facebook_business.adobjects.nullnode import NullNode
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/videos',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=NullNode,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=NullNode, api=self._api),
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
        'attending_count': 'int',
        'can_guests_invite': 'bool',
        'category': 'Category',
        'cover': 'CoverPhoto',
        'created_time': 'datetime',
        'declined_count': 'int',
        'description': 'string',
        'discount_code_enabled': 'bool',
        'end_time': 'string',
        'event_times': 'list<ChildEvent>',
        'guest_list_enabled': 'bool',
        'id': 'string',
        'interested_count': 'int',
        'is_canceled': 'bool',
        'is_draft': 'bool',
        'is_online': 'bool',
        'is_page_owned': 'bool',
        'maybe_count': 'int',
        'name': 'string',
        'noreply_count': 'int',
        'online_event_format': 'OnlineEventFormat',
        'online_event_third_party_url': 'string',
        'owner': 'Object',
        'parent_group': 'Group',
        'place': 'Place',
        'registration_setting': 'EventRegistrationSetting',
        'scheduled_publish_time': 'string',
        'start_time': 'string',
        'ticket_setting': 'EventTicketSetting',
        'ticket_uri': 'string',
        'ticket_uri_start_sales_time': 'string',
        'ticketing_privacy_uri': 'string',
        'ticketing_terms_uri': 'string',
        'timezone': 'string',
        'type': 'Type',
        'updated_time': 'datetime',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Category'] = Event.Category.__dict__.values()
        field_enum_info['OnlineEventFormat'] = Event.OnlineEventFormat.__dict__.values()
        field_enum_info['Type'] = Event.Type.__dict__.values()
        field_enum_info['EventStateFilter'] = Event.EventStateFilter.__dict__.values()
        field_enum_info['TimeFilter'] = Event.TimeFilter.__dict__.values()
        return field_enum_info


