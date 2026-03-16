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

class IGUser(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isIGUser = True
        super(IGUser, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        biography = 'biography'
        business_discovery = 'business_discovery'
        followers_count = 'followers_count'
        follows_count = 'follows_count'
        has_profile_pic = 'has_profile_pic'
        id = 'id'
        ig_id = 'ig_id'
        is_published = 'is_published'
        legacy_instagram_user_id = 'legacy_instagram_user_id'
        media_count = 'media_count'
        mentioned_comment = 'mentioned_comment'
        mentioned_media = 'mentioned_media'
        name = 'name'
        owner_business = 'owner_business'
        profile_picture_url = 'profile_picture_url'
        shopping_product_tag_eligibility = 'shopping_product_tag_eligibility'
        shopping_review_status = 'shopping_review_status'
        username = 'username'
        website = 'website'

    def api_get(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'adgroup_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGUser,
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

    def get_agencies(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.business import Business
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/agencies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Business,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Business, api=self._api),
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

    def get_authorized_ad_accounts(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.adaccount import AdAccount
        param_types = {
            'business': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/authorized_adaccounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AdAccount,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AdAccount, api=self._api),
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

    def create_authorized_ad_account(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'account_id': 'string',
            'business': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/authorized_adaccounts',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGUser, api=self._api),
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

    def get_available_catalogs(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.useravailablecatalogs import UserAvailableCatalogs
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/available_catalogs',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=UserAvailableCatalogs,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=UserAvailableCatalogs, api=self._api),
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

    def get_branded_content_ad_permissions(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.igbcadspermission import IGBCAdsPermission
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/branded_content_ad_permissions',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGBCAdsPermission,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGBCAdsPermission, api=self._api),
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

    def create_branded_content_ad_permission(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.igbcadspermission import IGBCAdsPermission
        param_types = {
            'creator_instagram_account': 'string',
            'creator_instagram_username': 'string',
            'revoke': 'bool',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/branded_content_ad_permissions',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGBCAdsPermission,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGBCAdsPermission, api=self._api),
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

    def get_branded_content_advertisable_medias(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.brandedcontentshadowigmediaid import BrandedContentShadowIGMediaID
        param_types = {
            'ad_code': 'string',
            'creator_username': 'string',
            'media_relationship': 'list<media_relationship_enum>',
            'only_fetch_allowlisted': 'bool',
            'only_fetch_recommended_content': 'bool',
            'permalinks': 'list<string>',
        }
        enums = {
            'media_relationship_enum': BrandedContentShadowIGMediaID.MediaRelationship.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/branded_content_advertisable_medias',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=BrandedContentShadowIGMediaID,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=BrandedContentShadowIGMediaID, api=self._api),
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

    def delete_branded_content_tag_approval(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'user_ids': 'list<unsigned int>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/branded_content_tag_approval',
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

    def get_branded_content_tag_approval(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.brandedcontentshadowiguserid import BrandedContentShadowIGUserID
        param_types = {
            'user_ids': 'list<unsigned int>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/branded_content_tag_approval',
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

    def create_branded_content_tag_approval(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.brandedcontentshadowiguserid import BrandedContentShadowIGUserID
        param_types = {
            'user_ids': 'list<unsigned int>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/branded_content_tag_approval',
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

    def get_catalog_product_search(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.shadowigusercatalogproductsearch import ShadowIGUserCatalogProductSearch
        param_types = {
            'catalog_id': 'string',
            'q': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/catalog_product_search',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ShadowIGUserCatalogProductSearch,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ShadowIGUserCatalogProductSearch, api=self._api),
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

    def get_collaboration_invites(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.shadowigusercollaborationinvites import ShadowIGUserCollaborationInvites
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/collaboration_invites',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ShadowIGUserCollaborationInvites,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ShadowIGUserCollaborationInvites, api=self._api),
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

    def create_collaboration_invite(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.shadowigusercollaborationinvites import ShadowIGUserCollaborationInvites
        param_types = {
            'accept': 'bool',
            'media_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/collaboration_invites',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ShadowIGUserCollaborationInvites,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ShadowIGUserCollaborationInvites, api=self._api),
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

    def get_connected_threads_user(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.threadsuser import ThreadsUser
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/connected_threads_user',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ThreadsUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ThreadsUser, api=self._api),
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

    def get_content_publishing_limit(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.contentpublishinglimitresponse import ContentPublishingLimitResponse
        param_types = {
            'since': 'datetime',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/content_publishing_limit',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ContentPublishingLimitResponse,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ContentPublishingLimitResponse, api=self._api),
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

    def get_creator_market_place_creators(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.iguserexportforcam import IGUserExportForCAM
        param_types = {
            'creator_age_bucket': 'list<Object>',
            'creator_countries': 'list<creator_countries_enum>',
            'creator_gender': 'list<creator_gender_enum>',
            'creator_interests': 'list<Object>',
            'creator_max_engaged_accounts': 'unsigned int',
            'creator_max_followers': 'unsigned int',
            'creator_min_engaged_accounts': 'unsigned int',
            'creator_min_followers': 'unsigned int',
            'has_public_contact_email': 'bool',
            'major_audience_age_bucket': 'list<Object>',
            'major_audience_countries': 'list<major_audience_countries_enum>',
            'major_audience_gender': 'list<major_audience_gender_enum>',
            'query': 'string',
            'reels_interaction_rate': 'Object',
            'show_onboarded_creators_only': 'bool',
            'similar_to_creators': 'list<string>',
            'username': 'string',
        }
        enums = {
            'creator_countries_enum': IGUserExportForCAM.CreatorCountries.__dict__.values(),
            'creator_gender_enum': IGUserExportForCAM.CreatorGender.__dict__.values(),
            'major_audience_countries_enum': IGUserExportForCAM.MajorAudienceCountries.__dict__.values(),
            'major_audience_gender_enum': IGUserExportForCAM.MajorAudienceGender.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/creator_marketplace_creators',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGUserExportForCAM,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGUserExportForCAM, api=self._api),
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

    def get_dataset(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.dataset import Dataset
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/dataset',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Dataset,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Dataset, api=self._api),
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

    def create_dataset(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.dataset import Dataset
        param_types = {
            'dataset_name': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/dataset',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Dataset,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Dataset, api=self._api),
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
            'metric_type': 'metric_type_enum',
            'period': 'list<period_enum>',
            'since': 'datetime',
            'timeframe': 'timeframe_enum',
            'until': 'datetime',
        }
        enums = {
            'breakdown_enum': InstagramInsightsResult.Breakdown.__dict__.values(),
            'metric_enum': InstagramInsightsResult.Metric.__dict__.values(),
            'metric_type_enum': InstagramInsightsResult.MetricType.__dict__.values(),
            'period_enum': InstagramInsightsResult.Period.__dict__.values(),
            'timeframe_enum': InstagramInsightsResult.Timeframe.__dict__.values(),
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

    def get_instagram_backed_threads_user(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.threadsuser import ThreadsUser
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/instagram_backed_threads_user',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ThreadsUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ThreadsUser, api=self._api),
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

    def create_instagram_backed_threads_user(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.threadsuser import ThreadsUser
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/instagram_backed_threads_user',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ThreadsUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ThreadsUser, api=self._api),
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

    def get_live_media(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.igmedia import IGMedia
        param_types = {
            'since': 'datetime',
            'until': 'datetime',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/live_media',
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

    def get_media(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.igmedia import IGMedia
        param_types = {
            'since': 'datetime',
            'until': 'datetime',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/media',
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

    def create_media(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.igmedia import IGMedia
        param_types = {
            'alt_text': 'string',
            'audio_name': 'string',
            'caption': 'string',
            'children': 'list<string>',
            'collaborators': 'list<string>',
            'cover_url': 'string',
            'image_url': 'string',
            'is_carousel_item': 'bool',
            'location_id': 'string',
            'media_type': 'string',
            'product_tags': 'list<map>',
            'share_to_feed': 'bool',
            'thumb_offset': 'string',
            'trial_params': 'map',
            'upload_type': 'string',
            'user_tags': 'list<map>',
            'video_url': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/media',
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

    def create_media_publish(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.igmedia import IGMedia
        param_types = {
            'creation_id': 'unsigned int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/media_publish',
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

    def create_mention(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'comment_id': 'string',
            'media_id': 'string',
            'message': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/mentions',
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

    def get_notification_message_tokens(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.userpageonetimeoptintokensettings import UserPageOneTimeOptInTokenSettings
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/notification_message_tokens',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=UserPageOneTimeOptInTokenSettings,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=UserPageOneTimeOptInTokenSettings, api=self._api),
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

    def get_product_appeal(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.igshoppingproductappeal import IGShoppingProductAppeal
        param_types = {
            'product_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/product_appeal',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGShoppingProductAppeal,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGShoppingProductAppeal, api=self._api),
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

    def create_product_appeal(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.igshoppingproductappeal import IGShoppingProductAppeal
        param_types = {
            'appeal_reason': 'string',
            'product_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/product_appeal',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGShoppingProductAppeal,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGShoppingProductAppeal, api=self._api),
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

    def get_recently_searched_hashtags(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.shadowighashtag import ShadowIGHashtag
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/recently_searched_hashtags',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ShadowIGHashtag,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ShadowIGHashtag, api=self._api),
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

    def get_scheduled_media(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.shadowigscheduledmedia import ShadowIGScheduledMedia
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/scheduled_media',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ShadowIGScheduledMedia,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ShadowIGScheduledMedia, api=self._api),
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

    def get_stories(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.igmedia import IGMedia
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/stories',
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

    def get_tags(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.igmedia import IGMedia
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/tags',
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

    def get_upcoming_events(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.igupcomingevent import IGUpcomingEvent
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/upcoming_events',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=IGUpcomingEvent,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=IGUpcomingEvent, api=self._api),
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

    def create_upcoming_event(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'end_time': 'datetime',
            'notification_subtypes': 'list<notification_subtypes_enum>',
            'start_time': 'datetime',
            'title': 'string',
        }
        enums = {
            'notification_subtypes_enum': [
                'AFTER_EVENT_1DAY',
                'AFTER_EVENT_2DAY',
                'AFTER_EVENT_3DAY',
                'AFTER_EVENT_4DAY',
                'AFTER_EVENT_5DAY',
                'AFTER_EVENT_6DAY',
                'AFTER_EVENT_7DAY',
                'BEFORE_EVENT_15MIN',
                'BEFORE_EVENT_1DAY',
                'BEFORE_EVENT_1HOUR',
                'BEFORE_EVENT_2DAY',
                'EVENT_START',
                'RESCHEDULED',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/upcoming_events',
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

    def get_welcome_message_flows(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.shadowiguserctxpartnerappwelcomemessageflow import ShadowIGUserCTXPartnerAppWelcomeMessageFlow
        param_types = {
            'app_id': 'string',
            'flow_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/welcome_message_flows',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ShadowIGUserCTXPartnerAppWelcomeMessageFlow,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ShadowIGUserCTXPartnerAppWelcomeMessageFlow, api=self._api),
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
        'biography': 'string',
        'business_discovery': 'IGUser',
        'followers_count': 'int',
        'follows_count': 'int',
        'has_profile_pic': 'bool',
        'id': 'string',
        'ig_id': 'int',
        'is_published': 'bool',
        'legacy_instagram_user_id': 'string',
        'media_count': 'int',
        'mentioned_comment': 'IGComment',
        'mentioned_media': 'IGMedia',
        'name': 'string',
        'owner_business': 'Business',
        'profile_picture_url': 'string',
        'shopping_product_tag_eligibility': 'bool',
        'shopping_review_status': 'string',
        'username': 'string',
        'website': 'string',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


