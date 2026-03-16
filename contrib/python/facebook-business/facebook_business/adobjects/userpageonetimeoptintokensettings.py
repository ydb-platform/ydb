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

class UserPageOneTimeOptInTokenSettings(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isUserPageOneTimeOptInTokenSettings = True
        super(UserPageOneTimeOptInTokenSettings, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        creation_timestamp = 'creation_timestamp'
        custom_audience_ids = 'custom_audience_ids'
        next_eligible_time = 'next_eligible_time'
        next_eligible_time_for_paid_messaging = 'next_eligible_time_for_paid_messaging'
        notification_messages_frequency = 'notification_messages_frequency'
        notification_messages_reoptin = 'notification_messages_reoptin'
        notification_messages_timezone = 'notification_messages_timezone'
        notification_messages_token = 'notification_messages_token'
        recipient_id = 'recipient_id'
        token_expiry_timestamp = 'token_expiry_timestamp'
        topic_title = 'topic_title'
        user_token_status = 'user_token_status'
        id = 'id'

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
            target_class=UserPageOneTimeOptInTokenSettings,
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
        'creation_timestamp': 'int',
        'custom_audience_ids': 'list<string>',
        'next_eligible_time': 'int',
        'next_eligible_time_for_paid_messaging': 'int',
        'notification_messages_frequency': 'string',
        'notification_messages_reoptin': 'string',
        'notification_messages_timezone': 'string',
        'notification_messages_token': 'string',
        'recipient_id': 'string',
        'token_expiry_timestamp': 'int',
        'topic_title': 'string',
        'user_token_status': 'string',
        'id': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


