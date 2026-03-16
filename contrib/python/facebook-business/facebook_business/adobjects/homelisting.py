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

class HomeListing(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isHomeListing = True
        super(HomeListing, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        ac_type = 'ac_type'
        additional_fees_description = 'additional_fees_description'
        address = 'address'
        agent_company = 'agent_company'
        agent_email = 'agent_email'
        agent_fb_page_id = 'agent_fb_page_id'
        agent_name = 'agent_name'
        agent_phone = 'agent_phone'
        applinks = 'applinks'
        area_size = 'area_size'
        area_unit = 'area_unit'
        availability = 'availability'
        category_specific_fields = 'category_specific_fields'
        co_2_emission_rating_eu = 'co_2_emission_rating_eu'
        currency = 'currency'
        custom_label_0 = 'custom_label_0'
        custom_label_1 = 'custom_label_1'
        custom_label_2 = 'custom_label_2'
        custom_label_3 = 'custom_label_3'
        custom_label_4 = 'custom_label_4'
        custom_number_0 = 'custom_number_0'
        custom_number_1 = 'custom_number_1'
        custom_number_2 = 'custom_number_2'
        custom_number_3 = 'custom_number_3'
        custom_number_4 = 'custom_number_4'
        days_on_market = 'days_on_market'
        description = 'description'
        energy_rating_eu = 'energy_rating_eu'
        furnish_type = 'furnish_type'
        group_id = 'group_id'
        heating_type = 'heating_type'
        home_listing_id = 'home_listing_id'
        id = 'id'
        image_fetch_status = 'image_fetch_status'
        images = 'images'
        laundry_type = 'laundry_type'
        listing_type = 'listing_type'
        max_currency = 'max_currency'
        max_price = 'max_price'
        min_currency = 'min_currency'
        min_price = 'min_price'
        name = 'name'
        num_baths = 'num_baths'
        num_beds = 'num_beds'
        num_rooms = 'num_rooms'
        num_units = 'num_units'
        parking_type = 'parking_type'
        partner_verification = 'partner_verification'
        pet_policy = 'pet_policy'
        price = 'price'
        property_type = 'property_type'
        sanitized_images = 'sanitized_images'
        securitydeposit_currency = 'securitydeposit_currency'
        securitydeposit_price = 'securitydeposit_price'
        tags = 'tags'
        unit_price = 'unit_price'
        url = 'url'
        visibility = 'visibility'
        year_built = 'year_built'

    class ImageFetchStatus:
        direct_upload = 'DIRECT_UPLOAD'
        fetched = 'FETCHED'
        fetch_failed = 'FETCH_FAILED'
        no_status = 'NO_STATUS'
        outdated = 'OUTDATED'
        partial_fetch = 'PARTIAL_FETCH'

    class Visibility:
        published = 'PUBLISHED'
        staging = 'STAGING'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'home_listings'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.productcatalog import ProductCatalog
        return ProductCatalog(api=self._api, fbid=parent_id).create_home_listing(fields, params, batch, success, failure, pending)

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
            target_class=HomeListing,
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
            'address': 'Object',
            'availability': 'string',
            'currency': 'string',
            'description': 'string',
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
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=HomeListing,
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

    def get_channels_to_integrity_status(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.catalogitemchannelstointegritystatus import CatalogItemChannelsToIntegrityStatus
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/channels_to_integrity_status',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=CatalogItemChannelsToIntegrityStatus,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=CatalogItemChannelsToIntegrityStatus, api=self._api),
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

    def get_override_details(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.overridedetails import OverrideDetails
        param_types = {
            'keys': 'list<string>',
            'type': 'type_enum',
        }
        enums = {
            'type_enum': OverrideDetails.Type.__dict__.values(),
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/override_details',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=OverrideDetails,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=OverrideDetails, api=self._api),
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

    def get_videos_metadata(self, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.utils import api_utils
        if batch is None and (success is not None or failure is not None):
          api_utils.warning('`success` and `failure` callback only work for batch call.')
        from facebook_business.adobjects.dynamicvideometadata import DynamicVideoMetadata
        param_types = {
        }
        enums = {
        }
        request = FacebookRequest(
            node_id=self['id'],
            method='GET',
            endpoint='/videos_metadata',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=DynamicVideoMetadata,
            api_type='EDGE',
            response_parser=ObjectParser(target_class=DynamicVideoMetadata, api=self._api),
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
        'ac_type': 'string',
        'additional_fees_description': 'string',
        'address': 'Object',
        'agent_company': 'string',
        'agent_email': 'string',
        'agent_fb_page_id': 'Page',
        'agent_name': 'string',
        'agent_phone': 'string',
        'applinks': 'CatalogItemAppLinks',
        'area_size': 'unsigned int',
        'area_unit': 'string',
        'availability': 'string',
        'category_specific_fields': 'CatalogSubVerticalList',
        'co_2_emission_rating_eu': 'Object',
        'currency': 'string',
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
        'days_on_market': 'unsigned int',
        'description': 'string',
        'energy_rating_eu': 'Object',
        'furnish_type': 'string',
        'group_id': 'string',
        'heating_type': 'string',
        'home_listing_id': 'string',
        'id': 'string',
        'image_fetch_status': 'ImageFetchStatus',
        'images': 'list<string>',
        'laundry_type': 'string',
        'listing_type': 'string',
        'max_currency': 'string',
        'max_price': 'string',
        'min_currency': 'string',
        'min_price': 'string',
        'name': 'string',
        'num_baths': 'float',
        'num_beds': 'float',
        'num_rooms': 'float',
        'num_units': 'unsigned int',
        'parking_type': 'string',
        'partner_verification': 'string',
        'pet_policy': 'string',
        'price': 'string',
        'property_type': 'string',
        'sanitized_images': 'list<string>',
        'securitydeposit_currency': 'string',
        'securitydeposit_price': 'string',
        'tags': 'list<string>',
        'unit_price': 'Object',
        'url': 'string',
        'visibility': 'Visibility',
        'year_built': 'int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['ImageFetchStatus'] = HomeListing.ImageFetchStatus.__dict__.values()
        field_enum_info['Visibility'] = HomeListing.Visibility.__dict__.values()
        return field_enum_info


