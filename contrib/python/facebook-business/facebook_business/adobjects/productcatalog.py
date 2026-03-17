# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject
from facebook_business.adobjects.abstractcrudobject import AbstractCrudObject
from facebook_business.adobjects.objectparser import ObjectParser
from facebook_business.api import FacebookRequest
from facebook_business.typechecker import TypeChecker
from facebook_business.adobjects.helpers.productcatalogmixin import ProductCatalogMixin

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class ProductCatalog(
    ProductCatalogMixin,
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isProductCatalog = True
        super(ProductCatalog, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        ad_account_to_collaborative_ads_share_settings = 'ad_account_to_collaborative_ads_share_settings'
        agency_collaborative_ads_share_settings = 'agency_collaborative_ads_share_settings'
        business = 'business'
        catalog_store = 'catalog_store'
        commerce_merchant_settings = 'commerce_merchant_settings'
        creator_user = 'creator_user'
        da_display_settings = 'da_display_settings'
        default_image_url = 'default_image_url'
        fallback_image_url = 'fallback_image_url'
        feed_count = 'feed_count'
        id = 'id'
        is_catalog_segment = 'is_catalog_segment'
        is_local_catalog = 'is_local_catalog'
        name = 'name'
        owner_business = 'owner_business'
        product_count = 'product_count'
        store_catalog_settings = 'store_catalog_settings'
        user_access_expire_time = 'user_access_expire_time'
        vertical = 'vertical'
        additional_vertical_option = 'additional_vertical_option'
        business_metadata = 'business_metadata'
        catalog_segment_filter = 'catalog_segment_filter'
        catalog_segment_product_set_id = 'catalog_segment_product_set_id'
        destination_catalog_settings = 'destination_catalog_settings'
        flight_catalog_settings = 'flight_catalog_settings'
        parent_catalog_id = 'parent_catalog_id'
        partner_integration = 'partner_integration'

    class AdditionalVerticalOption:
        local_da_catalog = 'LOCAL_DA_CATALOG'
        local_products = 'LOCAL_PRODUCTS'

    class Vertical:
        adoptable_pets = 'adoptable_pets'
        commerce = 'commerce'
        destinations = 'destinations'
        flights = 'flights'
        generic = 'generic'
        home_listings = 'home_listings'
        hotels = 'hotels'
        local_service_businesses = 'local_service_businesses'
        offer_items = 'offer_items'
        offline_commerce = 'offline_commerce'
        transactable_items = 'transactable_items'
        vehicles = 'vehicles'

    class EnabledCollabTerms:
        enforce_create_new_ad_account = 'ENFORCE_CREATE_NEW_AD_ACCOUNT'
        enforce_share_ad_performance_access = 'ENFORCE_SHARE_AD_PERFORMANCE_ACCESS'

    class PermittedRoles:
        admin = 'ADMIN'
        advertiser = 'ADVERTISER'

    class PermittedTasks:
        aa_analyze = 'AA_ANALYZE'
        advertise = 'ADVERTISE'
        manage = 'MANAGE'
        manage_ar = 'MANAGE_AR'

    class Tasks:
        aa_analyze = 'AA_ANALYZE'
        advertise = 'ADVERTISE'
        manage = 'MANAGE'
        manage_ar = 'MANAGE_AR'

    class Standard:
        google = 'google'

    class ItemSubType:
        appliances = 'APPLIANCES'
        baby_feeding = 'BABY_FEEDING'
        baby_transport = 'BABY_TRANSPORT'
        beauty = 'BEAUTY'
        bedding = 'BEDDING'
        cameras = 'CAMERAS'
        cell_phones_and_smart_watches = 'CELL_PHONES_AND_SMART_WATCHES'
        cleaning_supplies = 'CLEANING_SUPPLIES'
        clothing = 'CLOTHING'
        clothing_accessories = 'CLOTHING_ACCESSORIES'
        computers_and_tablets = 'COMPUTERS_AND_TABLETS'
        diapering_and_potty_training = 'DIAPERING_AND_POTTY_TRAINING'
        electronics_accessories = 'ELECTRONICS_ACCESSORIES'
        furniture = 'FURNITURE'
        health = 'HEALTH'
        home_goods = 'HOME_GOODS'
        jewelry = 'JEWELRY'
        nursery = 'NURSERY'
        printers_and_scanners = 'PRINTERS_AND_SCANNERS'
        projectors = 'PROJECTORS'
        shoes_and_footwear = 'SHOES_AND_FOOTWEAR'
        software = 'SOFTWARE'
        toys = 'TOYS'
        tvs_and_monitors = 'TVS_AND_MONITORS'
        video_game_consoles_and_video_games = 'VIDEO_GAME_CONSOLES_AND_VIDEO_GAMES'
        watches = 'WATCHES'

    class EventName:
        add_to_cart = 'ADD_TO_CART'
        purchase = 'PURCHASE'
        test = 'TEST'
        view_item = 'VIEW_ITEM'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'owned_product_catalogs'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.business import Business
        return Business(api=self._api, fbid=parent_id).create_owned_product_catalog(fields, params, batch, success, failure, pending)

    def api_delete(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'allow_delete_catalog_with_live_product_set': 'bool',
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
            'segment_use_cases': 'list<segment_use_cases_enum>',
        }
        enums = {
            'segment_use_cases_enum': [
                'AFFILIATE_SELLER_STOREFRONT',
                'AFFILIATE_TAGGED_ONLY_DEPRECATED',
                'COLLAB_ADS',
                'COLLAB_ADS_FOR_MARKETPLACE_PARTNER',
                'COLLAB_ADS_SEGMENT_WITHOUT_SEGMENT_SYNCING',
                'DIGITAL_CIRCULARS',
                'FB_LIVE_SHOPPING',
                'IG_SHOPPING',
                'IG_SHOPPING_SUGGESTED_PRODUCTS',
                'MARKETPLACE_SHOPS',
                'TEST',
            ],
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
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
            'additional_vertical_option': 'additional_vertical_option_enum',
            'da_display_settings': 'Object',
            'default_image_url': 'string',
            'destination_catalog_settings': 'map',
            'fallback_image_url': 'string',
            'flight_catalog_settings': 'map',
            'name': 'string',
            'partner_integration': 'map',
            'store_catalog_settings': 'map',
        }
        enums = {
            'additional_vertical_option_enum': ProductCatalog.AdditionalVerticalOption.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
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

    def delete_agencies(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'business': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/agencies',
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

    def create_agency(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'business': 'string',
            'enabled_collab_terms': 'list<enabled_collab_terms_enum>',
            'permitted_roles': 'list<permitted_roles_enum>',
            'permitted_tasks': 'list<permitted_tasks_enum>',
            'skip_defaults': 'bool',
            'utm_settings': 'map',
        }
        enums = {
            'enabled_collab_terms_enum': ProductCatalog.EnabledCollabTerms.__dict__.values(),
            'permitted_roles_enum': ProductCatalog.PermittedRoles.__dict__.values(),
            'permitted_tasks_enum': ProductCatalog.PermittedTasks.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/agencies',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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

    def delete_assigned_users(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'user': 'int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/assigned_users',
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

    def get_assigned_users(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.assigneduser import AssignedUser
        param_types = {
            'business': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/assigned_users',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AssignedUser,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AssignedUser, api=self._api),
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

    def create_assigned_user(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'tasks': 'list<tasks_enum>',
            'user': 'int',
        }
        enums = {
            'tasks_enum': ProductCatalog.Tasks.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/assigned_users',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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

    def get_automotive_models(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.automotivemodel import AutomotiveModel
        param_types = {
            'bulk_pagination': 'bool',
            'filter': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/automotive_models',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=AutomotiveModel,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=AutomotiveModel, api=self._api),
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

    def create_batch(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'allow_upsert': 'bool',
            'fbe_external_business_id': 'string',
            'requests': 'list<map>',
            'version': 'unsigned int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/batch',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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

    def create_catalog_store(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.storecatalogsettings import StoreCatalogSettings
        param_types = {
            'page': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/catalog_store',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=StoreCatalogSettings,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=StoreCatalogSettings, api=self._api),
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

    def get_categories(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productcatalogcategory import ProductCatalogCategory
        param_types = {
            'categorization_criteria': 'categorization_criteria_enum',
            'filter': 'Object',
        }
        enums = {
            'categorization_criteria_enum': ProductCatalogCategory.CategorizationCriteria.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/categories',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalogCategory,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalogCategory, api=self._api),
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

    def create_category(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productcatalogcategory import ProductCatalogCategory
        param_types = {
            'data': 'list<map>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/categories',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalogCategory,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalogCategory, api=self._api),
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

    def get_check_batch_request_status(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.checkbatchrequeststatus import CheckBatchRequestStatus
        param_types = {
            'error_priority': 'error_priority_enum',
            'handle': 'string',
            'load_ids_of_invalid_requests': 'bool',
        }
        enums = {
            'error_priority_enum': CheckBatchRequestStatus.ErrorPriority.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/check_batch_request_status',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CheckBatchRequestStatus,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CheckBatchRequestStatus, api=self._api),
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

    def get_check_marketplace_partner_deals_status(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productcatalogcheckmarketplacepartnerdealsstatus import ProductCatalogCheckMarketplacePartnerDealsStatus
        param_types = {
            'session_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/check_marketplace_partner_deals_status',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalogCheckMarketplacePartnerDealsStatus,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalogCheckMarketplacePartnerDealsStatus, api=self._api),
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

    def get_check_marketplace_partner_sellers_status(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productcatalogcheckmarketplacepartnersellersstatus import ProductCatalogCheckMarketplacePartnerSellersStatus
        param_types = {
            'session_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/check_marketplace_partner_sellers_status',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalogCheckMarketplacePartnerSellersStatus,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalogCheckMarketplacePartnerSellersStatus, api=self._api),
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

    def get_collaborative_ads_lsb_image_bank(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.cpaslsbimagebank import CPASLsbImageBank
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/collaborative_ads_lsb_image_bank',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CPASLsbImageBank,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CPASLsbImageBank, api=self._api),
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

    def get_collaborative_ads_share_settings(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.collaborativeadssharesettings import CollaborativeAdsShareSettings
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/collaborative_ads_share_settings',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CollaborativeAdsShareSettings,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CollaborativeAdsShareSettings, api=self._api),
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

    def create_cpas_lsb_image_bank(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.cpaslsbimagebank import CPASLsbImageBank
        param_types = {
            'ad_group_id': 'unsigned int',
            'agency_business_id': 'unsigned int',
            'backup_image_urls': 'list<string>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/cpas_lsb_image_bank',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CPASLsbImageBank,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CPASLsbImageBank, api=self._api),
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

    def get_creator_asset_creatives(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.creatorassetcreative import CreatorAssetCreative
        param_types = {
            'moderation_status': 'moderation_status_enum',
        }
        enums = {
            'moderation_status_enum': CreatorAssetCreative.ModerationStatus.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/creator_asset_creatives',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CreatorAssetCreative,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CreatorAssetCreative, api=self._api),
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

    def get_data_sources(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productcatalogdatasource import ProductCatalogDataSource
        param_types = {
            'ingestion_source_type': 'ingestion_source_type_enum',
        }
        enums = {
            'ingestion_source_type_enum': ProductCatalogDataSource.IngestionSourceType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/data_sources',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalogDataSource,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalogDataSource, api=self._api),
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

    def get_destinations(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.destination import Destination
        param_types = {
            'bulk_pagination': 'bool',
            'filter': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/destinations',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Destination,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Destination, api=self._api),
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

    def get_diagnostics(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productcatalogdiagnosticgroup import ProductCatalogDiagnosticGroup
        param_types = {
            'affected_channels': 'list<affected_channels_enum>',
            'affected_entities': 'list<affected_entities_enum>',
            'affected_features': 'list<affected_features_enum>',
            'severities': 'list<severities_enum>',
            'types': 'list<types_enum>',
        }
        enums = {
            'affected_channels_enum': ProductCatalogDiagnosticGroup.AffectedChannels.__dict__.values(),
            'affected_entities_enum': ProductCatalogDiagnosticGroup.AffectedEntities.__dict__.values(),
            'affected_features_enum': ProductCatalogDiagnosticGroup.AffectedFeatures.__dict__.values(),
            'severities_enum': ProductCatalogDiagnosticGroup.Severities.__dict__.values(),
            'types_enum': ProductCatalogDiagnosticGroup.Types.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/diagnostics',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalogDiagnosticGroup,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalogDiagnosticGroup, api=self._api),
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

    def get_event_stats(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.producteventstat import ProductEventStat
        param_types = {
            'breakdowns': 'list<breakdowns_enum>',
        }
        enums = {
            'breakdowns_enum': ProductEventStat.Breakdowns.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/event_stats',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductEventStat,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductEventStat, api=self._api),
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

    def delete_external_event_sources(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'external_event_sources': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='DELETE',
            endpoint='/external_event_sources',
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

    def get_external_event_sources(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.externaleventsource import ExternalEventSource
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/external_event_sources',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ExternalEventSource,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ExternalEventSource, api=self._api),
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

    def create_external_event_source(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'external_event_sources': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/external_event_sources',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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

    def get_flights(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.flight import Flight
        param_types = {
            'bulk_pagination': 'bool',
            'filter': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/flights',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Flight,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Flight, api=self._api),
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

    def create_geolocated_items_batch(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'allow_upsert': 'bool',
            'item_type': 'string',
            'requests': 'map',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/geolocated_items_batch',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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

    def get_home_listings(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.homelisting import HomeListing
        param_types = {
            'bulk_pagination': 'bool',
            'filter': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/home_listings',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=HomeListing,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=HomeListing, api=self._api),
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

    def create_home_listing(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.homelisting import HomeListing
        param_types = {
            'address': 'Object',
            'availability': 'string',
            'currency': 'string',
            'description': 'string',
            'home_listing_id': 'string',
            'images': 'list<Object>',
            'listing_type': 'string',
            'name': 'string',
            'num_baths': 'float',
            'num_beds': 'float',
            'num_units': 'float',
            'price': 'float',
            'property_type': 'string',
            'url': 'string',
            'year_built': 'unsigned int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/home_listings',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=HomeListing,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=HomeListing, api=self._api),
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

    def get_hotel_rooms_batch(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productcataloghotelroomsbatch import ProductCatalogHotelRoomsBatch
        param_types = {
            'handle': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/hotel_rooms_batch',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalogHotelRoomsBatch,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalogHotelRoomsBatch, api=self._api),
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

    def create_hotel_rooms_batch(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'file': 'file',
            'password': 'string',
            'standard': 'standard_enum',
            'update_only': 'bool',
            'url': 'string',
            'username': 'string',
        }
        enums = {
            'standard_enum': ProductCatalog.Standard.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/hotel_rooms_batch',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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

    def get_hotels(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.hotel import Hotel
        param_types = {
            'bulk_pagination': 'bool',
            'filter': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/hotels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Hotel,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Hotel, api=self._api),
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

    def create_hotel(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.hotel import Hotel
        param_types = {
            'address': 'Object',
            'applinks': 'Object',
            'base_price': 'unsigned int',
            'brand': 'string',
            'currency': 'string',
            'description': 'string',
            'guest_ratings': 'list<Object>',
            'hotel_id': 'string',
            'images': 'list<Object>',
            'name': 'string',
            'phone': 'string',
            'star_rating': 'float',
            'url': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/hotels',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Hotel,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Hotel, api=self._api),
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

    def create_items_batch(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'allow_upsert': 'bool',
            'item_sub_type': 'item_sub_type_enum',
            'item_type': 'string',
            'requests': 'map',
            'version': 'unsigned int',
        }
        enums = {
            'item_sub_type_enum': ProductCatalog.ItemSubType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/items_batch',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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

    def create_localized_items_batch(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'allow_upsert': 'bool',
            'item_type': 'string',
            'requests': 'map',
            'version': 'unsigned int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/localized_items_batch',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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

    def create_market_place_partner_deals_detail(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'requests': 'map',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/marketplace_partner_deals_details',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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

    def create_market_place_partner_sellers_detail(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'requests': 'map',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/marketplace_partner_sellers_details',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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

    def create_market_place_partner_signal(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'event_name': 'event_name_enum',
            'event_source_url': 'string',
            'event_time': 'datetime',
            'order_data': 'map',
            'user_data': 'map',
        }
        enums = {
            'event_name_enum': ProductCatalog.EventName.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/marketplace_partner_signals',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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

    def get_pricing_variables_batch(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productcatalogpricingvariablesbatch import ProductCatalogPricingVariablesBatch
        param_types = {
            'handle': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/pricing_variables_batch',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalogPricingVariablesBatch,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalogPricingVariablesBatch, api=self._api),
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

    def create_pricing_variables_batch(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'file': 'file',
            'password': 'string',
            'standard': 'standard_enum',
            'update_only': 'bool',
            'url': 'string',
            'username': 'string',
        }
        enums = {
            'standard_enum': ProductCatalog.Standard.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/pricing_variables_batch',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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

    def get_product_feeds(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productfeed import ProductFeed
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/product_feeds',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductFeed,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductFeed, api=self._api),
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

    def create_product_feed(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productfeed import ProductFeed
        param_types = {
            'country': 'string',
            'default_currency': 'string',
            'deletion_enabled': 'bool',
            'delimiter': 'delimiter_enum',
            'encoding': 'encoding_enum',
            'feed_type': 'feed_type_enum',
            'file_name': 'string',
            'ingestion_source_type': 'ingestion_source_type_enum',
            'item_sub_type': 'item_sub_type_enum',
            'migrated_from_feed_id': 'string',
            'name': 'string',
            'override_type': 'override_type_enum',
            'override_value': 'string',
            'primary_feed_ids': 'list<string>',
            'quoted_fields_mode': 'quoted_fields_mode_enum',
            'rules': 'list<string>',
            'schedule': 'string',
            'selected_override_fields': 'list<string>',
            'update_schedule': 'string',
            'use_case': 'use_case_enum',
        }
        enums = {
            'delimiter_enum': ProductFeed.Delimiter.__dict__.values(),
            'encoding_enum': ProductFeed.Encoding.__dict__.values(),
            'feed_type_enum': ProductFeed.FeedType.__dict__.values(),
            'ingestion_source_type_enum': ProductFeed.IngestionSourceType.__dict__.values(),
            'item_sub_type_enum': ProductFeed.ItemSubType.__dict__.values(),
            'override_type_enum': ProductFeed.OverrideType.__dict__.values(),
            'quoted_fields_mode_enum': ProductFeed.QuotedFieldsMode.__dict__.values(),
            'use_case_enum': ProductFeed.UseCase.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/product_feeds',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductFeed,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductFeed, api=self._api),
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

    def get_product_groups(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productgroup import ProductGroup
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/product_groups',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductGroup,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductGroup, api=self._api),
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

    def create_product_group(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productgroup import ProductGroup
        param_types = {
            'retailer_id': 'string',
            'variants': 'list<Object>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/product_groups',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductGroup,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductGroup, api=self._api),
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

    def get_product_sets(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productset import ProductSet
        param_types = {
            'ancestor_id': 'string',
            'has_children': 'bool',
            'parent_id': 'string',
            'retailer_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/product_sets',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductSet, api=self._api),
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

    def create_product_set(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productset import ProductSet
        param_types = {
            'filter': 'Object',
            'metadata': 'map',
            'name': 'string',
            'ordering_info': 'list<unsigned int>',
            'publish_to_shops': 'list<map>',
            'retailer_id': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/product_sets',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductSet,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductSet, api=self._api),
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

    def get_product_sets_batch(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productcatalogproductsetsbatch import ProductCatalogProductSetsBatch
        param_types = {
            'handle': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/product_sets_batch',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalogProductSetsBatch,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalogProductSetsBatch, api=self._api),
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

    def get_products(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productitem import ProductItem
        param_types = {
            'bulk_pagination': 'bool',
            'error_priority': 'error_priority_enum',
            'error_type': 'error_type_enum',
            'filter': 'Object',
            'return_only_approved_products': 'bool',
        }
        enums = {
            'error_priority_enum': ProductItem.ErrorPriority.__dict__.values(),
            'error_type_enum': ProductItem.ErrorType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/products',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductItem,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductItem, api=self._api),
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

    def create_product(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productitem import ProductItem
        param_types = {
            'additional_image_urls': 'list<string>',
            'additional_variant_attributes': 'map',
            'age_group': 'age_group_enum',
            'allow_upsert': 'bool',
            'android_app_name': 'string',
            'android_class': 'string',
            'android_package': 'string',
            'android_url': 'string',
            'availability': 'availability_enum',
            'brand': 'string',
            'category': 'string',
            'category_specific_fields': 'map',
            'checkout_url': 'string',
            'color': 'string',
            'commerce_tax_category': 'commerce_tax_category_enum',
            'condition': 'condition_enum',
            'currency': 'string',
            'custom_data': 'map',
            'custom_label_0': 'string',
            'custom_label_1': 'string',
            'custom_label_2': 'string',
            'custom_label_3': 'string',
            'custom_label_4': 'string',
            'custom_number_0': 'unsigned int',
            'custom_number_1': 'unsigned int',
            'custom_number_2': 'unsigned int',
            'custom_number_3': 'unsigned int',
            'custom_number_4': 'unsigned int',
            'description': 'string',
            'expiration_date': 'string',
            'fb_product_category': 'string',
            'gender': 'gender_enum',
            'gtin': 'string',
            'image_url': 'string',
            'importer_address': 'map',
            'importer_name': 'string',
            'inventory': 'unsigned int',
            'ios_app_name': 'string',
            'ios_app_store_id': 'unsigned int',
            'ios_url': 'string',
            'ipad_app_name': 'string',
            'ipad_app_store_id': 'unsigned int',
            'ipad_url': 'string',
            'iphone_app_name': 'string',
            'iphone_app_store_id': 'unsigned int',
            'iphone_url': 'string',
            'launch_date': 'string',
            'live_special_price': 'string',
            'manufacturer_info': 'string',
            'manufacturer_part_number': 'string',
            'marked_for_product_launch': 'marked_for_product_launch_enum',
            'material': 'string',
            'mobile_link': 'string',
            'name': 'string',
            'ordering_index': 'unsigned int',
            'origin_country': 'origin_country_enum',
            'pattern': 'string',
            'price': 'unsigned int',
            'product_priority_0': 'float',
            'product_priority_1': 'float',
            'product_priority_2': 'float',
            'product_priority_3': 'float',
            'product_priority_4': 'float',
            'product_type': 'string',
            'quantity_to_sell_on_facebook': 'unsigned int',
            'retailer_id': 'string',
            'retailer_product_group_id': 'string',
            'return_policy_days': 'unsigned int',
            'rich_text_description': 'string',
            'sale_price': 'unsigned int',
            'sale_price_end_date': 'datetime',
            'sale_price_start_date': 'datetime',
            'short_description': 'string',
            'size': 'string',
            'start_date': 'string',
            'url': 'string',
            'visibility': 'visibility_enum',
            'wa_compliance_category': 'wa_compliance_category_enum',
            'windows_phone_app_id': 'string',
            'windows_phone_app_name': 'string',
            'windows_phone_url': 'string',
        }
        enums = {
            'age_group_enum': ProductItem.AgeGroup.__dict__.values(),
            'availability_enum': ProductItem.Availability.__dict__.values(),
            'commerce_tax_category_enum': ProductItem.CommerceTaxCategory.__dict__.values(),
            'condition_enum': ProductItem.Condition.__dict__.values(),
            'gender_enum': ProductItem.Gender.__dict__.values(),
            'marked_for_product_launch_enum': ProductItem.MarkedForProductLaunch.__dict__.values(),
            'origin_country_enum': ProductItem.OriginCountry.__dict__.values(),
            'visibility_enum': ProductItem.Visibility.__dict__.values(),
            'wa_compliance_category_enum': ProductItem.WaComplianceCategory.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/products',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductItem,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductItem, api=self._api),
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

    def create_update_generated_image_config(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'data': 'list<Object>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/update_generated_image_config',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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

    def get_vehicle_offers(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.vehicleoffer import VehicleOffer
        param_types = {
            'bulk_pagination': 'bool',
            'filter': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/vehicle_offers',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=VehicleOffer,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=VehicleOffer, api=self._api),
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

    def get_vehicles(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.vehicle import Vehicle
        param_types = {
            'bulk_pagination': 'bool',
            'filter': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/vehicles',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Vehicle,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Vehicle, api=self._api),
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

    def create_vehicle(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.vehicle import Vehicle
        param_types = {
            'address': 'map',
            'applinks': 'Object',
            'availability': 'availability_enum',
            'body_style': 'body_style_enum',
            'condition': 'condition_enum',
            'currency': 'string',
            'date_first_on_lot': 'string',
            'dealer_id': 'string',
            'dealer_name': 'string',
            'dealer_phone': 'string',
            'description': 'string',
            'drivetrain': 'drivetrain_enum',
            'exterior_color': 'string',
            'fb_page_id': 'string',
            'fuel_type': 'fuel_type_enum',
            'images': 'list<Object>',
            'interior_color': 'string',
            'make': 'string',
            'mileage': 'map',
            'model': 'string',
            'price': 'unsigned int',
            'state_of_vehicle': 'state_of_vehicle_enum',
            'title': 'string',
            'transmission': 'transmission_enum',
            'trim': 'string',
            'url': 'string',
            'vehicle_id': 'string',
            'vehicle_type': 'vehicle_type_enum',
            'vin': 'string',
            'year': 'unsigned int',
        }
        enums = {
            'availability_enum': Vehicle.Availability.__dict__.values(),
            'body_style_enum': Vehicle.BodyStyle.__dict__.values(),
            'condition_enum': Vehicle.Condition.__dict__.values(),
            'drivetrain_enum': Vehicle.Drivetrain.__dict__.values(),
            'fuel_type_enum': Vehicle.FuelType.__dict__.values(),
            'state_of_vehicle_enum': Vehicle.StateOfVehicle.__dict__.values(),
            'transmission_enum': Vehicle.Transmission.__dict__.values(),
            'vehicle_type_enum': Vehicle.VehicleType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/vehicles',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Vehicle,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=Vehicle, api=self._api),
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

    def get_version_configs(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.catalogcontentversionconfig import CatalogContentVersionConfig
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/version_configs',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CatalogContentVersionConfig,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CatalogContentVersionConfig, api=self._api),
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

    def create_version_items_batch(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'allow_upsert': 'bool',
            'item_type': 'string',
            'item_version': 'string',
            'requests': 'map',
            'version': 'unsigned int',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/version_items_batch',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductCatalog,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductCatalog, api=self._api),
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
        'ad_account_to_collaborative_ads_share_settings': 'CollaborativeAdsShareSettings',
        'agency_collaborative_ads_share_settings': 'CollaborativeAdsShareSettings',
        'business': 'Business',
        'catalog_store': 'StoreCatalogSettings',
        'commerce_merchant_settings': 'CommerceMerchantSettings',
        'creator_user': 'User',
        'da_display_settings': 'ProductCatalogImageSettings',
        'default_image_url': 'string',
        'fallback_image_url': 'list<string>',
        'feed_count': 'int',
        'id': 'string',
        'is_catalog_segment': 'bool',
        'is_local_catalog': 'bool',
        'name': 'string',
        'owner_business': 'Business',
        'product_count': 'int',
        'store_catalog_settings': 'StoreCatalogSettings',
        'user_access_expire_time': 'datetime',
        'vertical': 'string',
        'additional_vertical_option': 'AdditionalVerticalOption',
        'business_metadata': 'map',
        'catalog_segment_filter': 'Object',
        'catalog_segment_product_set_id': 'string',
        'destination_catalog_settings': 'map',
        'flight_catalog_settings': 'map',
        'parent_catalog_id': 'string',
        'partner_integration': 'map',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['AdditionalVerticalOption'] = ProductCatalog.AdditionalVerticalOption.__dict__.values()
        field_enum_info['Vertical'] = ProductCatalog.Vertical.__dict__.values()
        field_enum_info['EnabledCollabTerms'] = ProductCatalog.EnabledCollabTerms.__dict__.values()
        field_enum_info['PermittedRoles'] = ProductCatalog.PermittedRoles.__dict__.values()
        field_enum_info['PermittedTasks'] = ProductCatalog.PermittedTasks.__dict__.values()
        field_enum_info['Tasks'] = ProductCatalog.Tasks.__dict__.values()
        field_enum_info['Standard'] = ProductCatalog.Standard.__dict__.values()
        field_enum_info['ItemSubType'] = ProductCatalog.ItemSubType.__dict__.values()
        field_enum_info['EventName'] = ProductCatalog.EventName.__dict__.values()
        return field_enum_info


