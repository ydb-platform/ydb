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

class ProductFeed(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isProductFeed = True
        super(ProductFeed, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        country = 'country'
        created_time = 'created_time'
        default_currency = 'default_currency'
        deletion_enabled = 'deletion_enabled'
        delimiter = 'delimiter'
        encoding = 'encoding'
        file_name = 'file_name'
        id = 'id'
        ingestion_source_type = 'ingestion_source_type'
        item_sub_type = 'item_sub_type'
        latest_upload = 'latest_upload'
        migrated_from_feed_id = 'migrated_from_feed_id'
        name = 'name'
        override_type = 'override_type'
        primary_feeds = 'primary_feeds'
        product_count = 'product_count'
        quoted_fields_mode = 'quoted_fields_mode'
        schedule = 'schedule'
        supplementary_feeds = 'supplementary_feeds'
        update_schedule = 'update_schedule'
        feed_type = 'feed_type'
        override_value = 'override_value'
        primary_feed_ids = 'primary_feed_ids'
        rules = 'rules'
        selected_override_fields = 'selected_override_fields'
        use_case = 'use_case'

    class Delimiter:
        autodetect = 'AUTODETECT'
        bar = 'BAR'
        comma = 'COMMA'
        semicolon = 'SEMICOLON'
        tab = 'TAB'
        tilde = 'TILDE'

    class IngestionSourceType:
        primary_feed = 'primary_feed'
        supplementary_feed = 'supplementary_feed'

    class QuotedFieldsMode:
        autodetect = 'AUTODETECT'
        off = 'OFF'
        on = 'ON'

    class Encoding:
        autodetect = 'AUTODETECT'
        latin1 = 'LATIN1'
        utf16be = 'UTF16BE'
        utf16le = 'UTF16LE'
        utf32be = 'UTF32BE'
        utf32le = 'UTF32LE'
        utf8 = 'UTF8'

    class FeedType:
        automotive_model = 'AUTOMOTIVE_MODEL'
        collection = 'COLLECTION'
        destination = 'DESTINATION'
        flight = 'FLIGHT'
        home_listing = 'HOME_LISTING'
        hotel = 'HOTEL'
        hotel_room = 'HOTEL_ROOM'
        local_inventory = 'LOCAL_INVENTORY'
        media_title = 'MEDIA_TITLE'
        offer = 'OFFER'
        products = 'PRODUCTS'
        product_ratings_and_reviews = 'PRODUCT_RATINGS_AND_REVIEWS'
        transactable_items = 'TRANSACTABLE_ITEMS'
        vehicles = 'VEHICLES'
        vehicle_offer = 'VEHICLE_OFFER'

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

    class OverrideType:
        batch_api_language_or_country = 'BATCH_API_LANGUAGE_OR_COUNTRY'
        catalog_segment_customize_default = 'CATALOG_SEGMENT_CUSTOMIZE_DEFAULT'
        country = 'COUNTRY'
        language = 'LANGUAGE'
        language_and_country = 'LANGUAGE_AND_COUNTRY'
        local = 'LOCAL'
        smart_pixel_language_or_country = 'SMART_PIXEL_LANGUAGE_OR_COUNTRY'
        version = 'VERSION'

    class UseCase:
        creator_asset = 'CREATOR_ASSET'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'product_feeds'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.productcatalog import ProductCatalog
        return ProductCatalog(api=self._api, fbid=parent_id).create_product_feed(fields, params, batch, success, failure, pending)

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
            target_class=ProductFeed,
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
            'default_currency': 'string',
            'deletion_enabled': 'bool',
            'delimiter': 'delimiter_enum',
            'encoding': 'encoding_enum',
            'migrated_from_feed_id': 'string',
            'name': 'string',
            'quoted_fields_mode': 'quoted_fields_mode_enum',
            'schedule': 'string',
            'update_schedule': 'string',
        }
        enums = {
            'delimiter_enum': ProductFeed.Delimiter.__dict__.values(),
            'encoding_enum': ProductFeed.Encoding.__dict__.values(),
            'quoted_fields_mode_enum': ProductFeed.QuotedFieldsMode.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductFeed,
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

    def get_media_titles(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.mediatitle import MediaTitle
        param_types = {
            'bulk_pagination': 'bool',
            'filter': 'Object',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/media_titles',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=MediaTitle,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=MediaTitle, api=self._api),
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

    def get_rules(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productfeedrule import ProductFeedRule
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/rules',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductFeedRule,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductFeedRule, api=self._api),
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

    def create_rule(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productfeedrule import ProductFeedRule
        param_types = {
            'attribute': 'string',
            'params': 'map',
            'rule_type': 'rule_type_enum',
        }
        enums = {
            'rule_type_enum': ProductFeedRule.RuleType.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/rules',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductFeedRule,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductFeedRule, api=self._api),
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

    def create_supplementary_feed_assoc(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'assoc_data': 'list<map>',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/supplementary_feed_assocs',
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

    def get_upload_schedules(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productfeedschedule import ProductFeedSchedule
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/upload_schedules',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductFeedSchedule,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductFeedSchedule, api=self._api),
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

    def create_upload_schedule(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        param_types = {
            'upload_schedule': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/upload_schedules',
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

    def get_uploads(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productfeedupload import ProductFeedUpload
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/uploads',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductFeedUpload,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductFeedUpload, api=self._api),
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

    def create_upload(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.productfeedupload import ProductFeedUpload
        param_types = {
            'fbe_external_business_id': 'string',
            'file': 'file',
            'password': 'string',
            'update_only': 'bool',
            'url': 'string',
            'username': 'string',
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='POST',
            endpoint='/uploads',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=ProductFeedUpload,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=ProductFeedUpload, api=self._api),
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

    _field_types = {
        'country': 'string',
        'created_time': 'datetime',
        'default_currency': 'string',
        'deletion_enabled': 'bool',
        'delimiter': 'Delimiter',
        'encoding': 'string',
        'file_name': 'string',
        'id': 'string',
        'ingestion_source_type': 'IngestionSourceType',
        'item_sub_type': 'string',
        'latest_upload': 'ProductFeedUpload',
        'migrated_from_feed_id': 'string',
        'name': 'string',
        'override_type': 'string',
        'primary_feeds': 'list<string>',
        'product_count': 'int',
        'quoted_fields_mode': 'QuotedFieldsMode',
        'schedule': 'ProductFeedSchedule',
        'supplementary_feeds': 'list<string>',
        'update_schedule': 'ProductFeedSchedule',
        'feed_type': 'FeedType',
        'override_value': 'string',
        'primary_feed_ids': 'list<string>',
        'rules': 'list<string>',
        'selected_override_fields': 'list<string>',
        'use_case': 'UseCase',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['Delimiter'] = ProductFeed.Delimiter.__dict__.values()
        field_enum_info['IngestionSourceType'] = ProductFeed.IngestionSourceType.__dict__.values()
        field_enum_info['QuotedFieldsMode'] = ProductFeed.QuotedFieldsMode.__dict__.values()
        field_enum_info['Encoding'] = ProductFeed.Encoding.__dict__.values()
        field_enum_info['FeedType'] = ProductFeed.FeedType.__dict__.values()
        field_enum_info['ItemSubType'] = ProductFeed.ItemSubType.__dict__.values()
        field_enum_info['OverrideType'] = ProductFeed.OverrideType.__dict__.values()
        field_enum_info['UseCase'] = ProductFeed.UseCase.__dict__.values()
        return field_enum_info


