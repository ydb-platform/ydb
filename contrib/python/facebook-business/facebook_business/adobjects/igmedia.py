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

class IGMedia(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isIGMedia = True
        super(IGMedia, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        alt_text = 'alt_text'
        boost_eligibility_info = 'boost_eligibility_info'
        caption = 'caption'
        comments_count = 'comments_count'
        copyright_check_information = 'copyright_check_information'
        has_poll = 'has_poll'
        has_slider = 'has_slider'
        id = 'id'
        ig_id = 'ig_id'
        is_comment_enabled = 'is_comment_enabled'
        is_shared_to_feed = 'is_shared_to_feed'
        legacy_instagram_media_id = 'legacy_instagram_media_id'
        like_count = 'like_count'
        media_product_type = 'media_product_type'
        media_type = 'media_type'
        media_url = 'media_url'
        owner = 'owner'
        permalink = 'permalink'
        shortcode = 'shortcode'
        thumbnail_url = 'thumbnail_url'
        timestamp = 'timestamp'
        username = 'username'
        video_title = 'video_title'
        view_count = 'view_count'

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
            'ad_account_id': 'unsigned int',
            'boostable_media_callsite': 'boostable_media_callsite_enum',
            'business_id': 'string',
            'primary_fb_page_id': 'string',
            'primary_ig_user_id': 'string',
            'secondary_fb_page_id': 'string',
            'secondary_ig_user_id': 'string',
        }
        enums = {
            'boostable_media_callsite_enum': [
                'ADS_MANAGER_L1_EDITOR_DYNAMIC_ADS_WITH_EXISTING_POST',
                'PA_HUB_CATALOG_INGESTION_CREATOR_ASSET',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGMedia,
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
            'comment_enabled': 'bool',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGMedia,
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

    def get_boost_ads_list(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.igboostmediaad import IGBoostMediaAd
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/boost_ads_list',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGBoostMediaAd,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGBoostMediaAd, api=self._api),
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

    def get_branded_content_partner_promote(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.brandedcontentshadowiguserid import BrandedContentShadowIGUserID
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/branded_content_partner_promote',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BrandedContentShadowIGUserID,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BrandedContentShadowIGUserID, api=self._api),
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

    def create_branded_content_partner_promote(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.brandedcontentshadowiguserid import BrandedContentShadowIGUserID
        param_types = {
            'permission': 'bool',
            'sponsor_id': 'unsigned int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/branded_content_partner_promote',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BrandedContentShadowIGUserID,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BrandedContentShadowIGUserID, api=self._api),
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

    def get_children(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/children',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGMedia,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGMedia, api=self._api),
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

    def get_collaborators(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.shadowigmediacollaborators import ShadowIGMediaCollaborators
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/collaborators',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ShadowIGMediaCollaborators,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ShadowIGMediaCollaborators, api=self._api),
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
        from facebook_business.adobjects.igcomment import IGComment
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
            target_class=IGComment,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGComment, api=self._api),
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
        from facebook_business.adobjects.igcomment import IGComment
        param_types = {
            'ad_id': 'string',
            'message': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/comments',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGComment,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGComment, api=self._api),
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

    def get_insights(self, fields=None, params=None, is_async=False, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.instagraminsightsresult import InstagramInsightsResult
        if is_async:
          return self.get_insights_async(fields, params, batch, success, failure, pending)
        param_types = {
            'breakdown': 'list<breakdown_enum>',
            'metric': 'list<metric_enum>',
            'period': 'list<period_enum>',
        }
        enums = {
            'breakdown_enum': InstagramInsightsResult.Breakdown.__dict__.values(),
            'metric_enum': InstagramInsightsResult.Metric.__dict__.values(),
            'period_enum': InstagramInsightsResult.Period.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/insights',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=InstagramInsightsResult,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=InstagramInsightsResult, api=self._api),
            include_summary=False,
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

    def delete_partnership_ad_code(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
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
            endpoint='/partnership_ad_code',
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

    def create_partnership_ad_code(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/partnership_ad_code',
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

    def get_product_tags(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.shadowigmediaproducttags import ShadowIGMediaProductTags
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/product_tags',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ShadowIGMediaProductTags,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ShadowIGMediaProductTags, api=self._api),
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

    def create_product_tag(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.shadowigmediaproducttags import ShadowIGMediaProductTags
        param_types = {
            'child_index': 'unsigned int',
            'updated_tags': 'list<map>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/product_tags',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ShadowIGMediaProductTags,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ShadowIGMediaProductTags, api=self._api),
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
        'alt_text': 'string',
        'boost_eligibility_info': 'IGMediaBoostEligibilityInfo',
        'caption': 'string',
        'comments_count': 'int',
        'copyright_check_information': 'IGVideoCopyrightCheckMatchesInformation',
        'has_poll': 'bool',
        'has_slider': 'bool',
        'id': 'string',
        'ig_id': 'string',
        'is_comment_enabled': 'bool',
        'is_shared_to_feed': 'bool',
        'legacy_instagram_media_id': 'string',
        'like_count': 'int',
        'media_product_type': 'string',
        'media_type': 'string',
        'media_url': 'string',
        'owner': 'IGUser',
        'permalink': 'string',
        'shortcode': 'string',
        'thumbnail_url': 'string',
        'timestamp': 'datetime',
        'username': 'string',
        'video_title': 'string',
        'view_count': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


