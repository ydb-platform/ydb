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

class Profile(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isProfile = True
        super(Profile, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        can_post = 'can_post'
        id = 'id'
        link = 'link'
        name = 'name'
        pic = 'pic'
        pic_crop = 'pic_crop'
        pic_large = 'pic_large'
        pic_small = 'pic_small'
        pic_square = 'pic_square'
        profile_type = 'profile_type'
        username = 'username'

    class ProfileType:
        application = 'application'
        event = 'event'
        group = 'group'
        page = 'page'
        user = 'user'

    class Type:
        angry = 'ANGRY'
        care = 'CARE'
        fire = 'FIRE'
        haha = 'HAHA'
        hundred = 'HUNDRED'
        like = 'LIKE'
        love = 'LOVE'
        none = 'NONE'
        pride = 'PRIDE'
        sad = 'SAD'
        thankful = 'THANKFUL'
        wow = 'WOW'

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
            target_class=Profile,
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

    _field_types = {
        'can_post': 'bool',
        'id': 'string',
        'link': 'string',
        'name': 'string',
        'pic': 'string',
        'pic_crop': 'ProfilePictureSource',
        'pic_large': 'string',
        'pic_small': 'string',
        'pic_square': 'string',
        'profile_type': 'ProfileType',
        'username': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['ProfileType'] = Profile.ProfileType.__dict__.values()
        field_enum_info['Type'] = Profile.Type.__dict__.values()
        return field_enum_info


