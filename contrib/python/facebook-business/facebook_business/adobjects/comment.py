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

class Comment(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isComment = True
        super(Comment, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        admin_creator = 'admin_creator'
        application = 'application'
        attachment = 'attachment'
        can_comment = 'can_comment'
        can_hide = 'can_hide'
        can_like = 'can_like'
        can_remove = 'can_remove'
        can_reply_privately = 'can_reply_privately'
        comment_count = 'comment_count'
        created_time = 'created_time'
        field_from = 'from'
        id = 'id'
        is_hidden = 'is_hidden'
        is_private = 'is_private'
        like_count = 'like_count'
        live_broadcast_timestamp = 'live_broadcast_timestamp'
        message = 'message'
        message_tags = 'message_tags'
        object = 'object'
        parent = 'parent'
        permalink_url = 'permalink_url'
        private_reply_conversation = 'private_reply_conversation'
        user_likes = 'user_likes'

    class Order:
        chronological = 'chronological'
        reverse_chronological = 'reverse_chronological'

    class CommentPrivacyValue:
        declined_by_admin_assistant = 'DECLINED_BY_ADMIN_ASSISTANT'
        default_privacy = 'DEFAULT_PRIVACY'
        friends_and_post_owner = 'FRIENDS_AND_POST_OWNER'
        friends_only = 'FRIENDS_ONLY'
        graphql_multiple_value_hack_do_not_use = 'GRAPHQL_MULTIPLE_VALUE_HACK_DO_NOT_USE'
        owner_or_commenter = 'OWNER_OR_COMMENTER'
        pending_approval = 'PENDING_APPROVAL'
        removed_by_admin_assistant = 'REMOVED_BY_ADMIN_ASSISTANT'
        side_conversation = 'SIDE_CONVERSATION'
        side_conversation_and_post_owner = 'SIDE_CONVERSATION_AND_POST_OWNER'
        spotlight_tab = 'SPOTLIGHT_TAB'

    class Filter:
        stream = 'stream'
        toplevel = 'toplevel'

    class LiveFilter:
        filter_low_quality = 'filter_low_quality'
        no_filter = 'no_filter'

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
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Comment,
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
            'attachment_id': 'string',
            'attachment_share_url': 'string',
            'attachment_url': 'string',
            'is_hidden': 'bool',
            'message': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Comment,
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

    def create_comment(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'attachment_id': 'string',
            'attachment_share_url': 'string',
            'attachment_url': 'string',
            'comment_privacy_value': 'comment_privacy_value_enum',
            'facepile_mentioned_ids': 'list<string>',
            'feedback_source': 'string',
            'is_offline': 'bool',
            'message': 'string',
            'nectar_module': 'string',
            'object_id': 'string',
            'parent_comment_id': 'Object',
            'text': 'string',
            'tracking': 'string',
        }
        enums = {
            'comment_privacy_value_enum': Comment.CommentPrivacyValue.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
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

    def delete_likes(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'feedback_source': 'string',
            'nectar_module': 'string',
            'tracking': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/likes',
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

    def get_likes(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/likes',
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

    def create_like(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'feedback_source': 'string',
            'nectar_module': 'string',
            'tracking': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/likes',
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
        'admin_creator': 'User',
        'application': 'Application',
        'attachment': 'Object',
        'can_comment': 'bool',
        'can_hide': 'bool',
        'can_like': 'bool',
        'can_remove': 'bool',
        'can_reply_privately': 'bool',
        'comment_count': 'unsigned int',
        'created_time': 'datetime',
        'from': 'Object',
        'id': 'string',
        'is_hidden': 'bool',
        'is_private': 'bool',
        'like_count': 'unsigned int',
        'live_broadcast_timestamp': 'unsigned int',
        'message': 'string',
        'message_tags': 'list<EntityAtTextRange>',
        'object': 'Object',
        'parent': 'Comment',
        'permalink_url': 'string',
        'private_reply_conversation': 'Object',
        'user_likes': 'bool',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Order'] = Comment.Order.__dict__.values()
        field_enum_info['CommentPrivacyValue'] = Comment.CommentPrivacyValue.__dict__.values()
        field_enum_info['Filter'] = Comment.Filter.__dict__.values()
        field_enum_info['LiveFilter'] = Comment.LiveFilter.__dict__.values()
        return field_enum_info


