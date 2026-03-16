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

class BusinessImage(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isBusinessImage = True
        super(BusinessImage, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        business = 'business'
        creation_time = 'creation_time'
        hash = 'hash'
        height = 'height'
        id = 'id'
        media_library_url = 'media_library_url'
        name = 'name'
        url = 'url'
        url_128 = 'url_128'
        width = 'width'
        ad_placements_validation_only = 'ad_placements_validation_only'
        bytes = 'bytes'
        creative_folder_id = 'creative_folder_id'
        validation_ad_placements = 'validation_ad_placements'

    class ValidationAdPlacements:
        audience_network_instream_video = 'AUDIENCE_NETWORK_INSTREAM_VIDEO'
        audience_network_instream_video_mobile = 'AUDIENCE_NETWORK_INSTREAM_VIDEO_MOBILE'
        audience_network_rewarded_video = 'AUDIENCE_NETWORK_REWARDED_VIDEO'
        desktop_feed_standard = 'DESKTOP_FEED_STANDARD'
        facebook_story_mobile = 'FACEBOOK_STORY_MOBILE'
        facebook_story_sticker_mobile = 'FACEBOOK_STORY_STICKER_MOBILE'
        instagram_standard = 'INSTAGRAM_STANDARD'
        instagram_story = 'INSTAGRAM_STORY'
        instant_article_standard = 'INSTANT_ARTICLE_STANDARD'
        instream_banner_desktop = 'INSTREAM_BANNER_DESKTOP'
        instream_banner_mobile = 'INSTREAM_BANNER_MOBILE'
        instream_video_desktop = 'INSTREAM_VIDEO_DESKTOP'
        instream_video_image = 'INSTREAM_VIDEO_IMAGE'
        instream_video_mobile = 'INSTREAM_VIDEO_MOBILE'
        messenger_mobile_inbox_media = 'MESSENGER_MOBILE_INBOX_MEDIA'
        messenger_mobile_story_media = 'MESSENGER_MOBILE_STORY_MEDIA'
        mobile_feed_standard = 'MOBILE_FEED_STANDARD'
        mobile_fullwidth = 'MOBILE_FULLWIDTH'
        mobile_interstitial = 'MOBILE_INTERSTITIAL'
        mobile_medium_rectangle = 'MOBILE_MEDIUM_RECTANGLE'
        mobile_native = 'MOBILE_NATIVE'
        right_column_standard = 'RIGHT_COLUMN_STANDARD'
        suggested_video_mobile = 'SUGGESTED_VIDEO_MOBILE'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'images'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.business import Business
        return Business(api=self._api, fbid=parent_id).create_image(fields, params, batch, success, failure, pending)

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
            target_class=BusinessImage,
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
        'business': 'Business',
        'creation_time': 'datetime',
        'hash': 'string',
        'height': 'int',
        'id': 'string',
        'media_library_url': 'string',
        'name': 'string',
        'url': 'string',
        'url_128': 'string',
        'width': 'int',
        'ad_placements_validation_only': 'bool',
        'bytes': 'string',
        'creative_folder_id': 'string',
        'validation_ad_placements': 'list<ValidationAdPlacements>',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['ValidationAdPlacements'] = BusinessImage.ValidationAdPlacements.__dict__.values()
        return field_enum_info


