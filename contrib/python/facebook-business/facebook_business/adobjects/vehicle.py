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

class Vehicle(
    AbstractCrudObject,
):

    def __init__(self, fbid=None, parent_id=None, api=None):
        self._isVehicle = True
        super(Vehicle, self).__init__(fbid, parent_id, api)

    class Field(AbstractObject.Field):
        address = 'address'
        applinks = 'applinks'
        availability = 'availability'
        availability_circle_radius = 'availability_circle_radius'
        availability_circle_radius_unit = 'availability_circle_radius_unit'
        body_style = 'body_style'
        category_specific_fields = 'category_specific_fields'
        condition = 'condition'
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
        date_first_on_lot = 'date_first_on_lot'
        dealer_communication_channel = 'dealer_communication_channel'
        dealer_email = 'dealer_email'
        dealer_id = 'dealer_id'
        dealer_name = 'dealer_name'
        dealer_phone = 'dealer_phone'
        dealer_privacy_policy_url = 'dealer_privacy_policy_url'
        description = 'description'
        drivetrain = 'drivetrain'
        exterior_color = 'exterior_color'
        fb_page_id = 'fb_page_id'
        features = 'features'
        fuel_type = 'fuel_type'
        id = 'id'
        image_fetch_status = 'image_fetch_status'
        images = 'images'
        interior_color = 'interior_color'
        legal_disclosure_impressum_url = 'legal_disclosure_impressum_url'
        make = 'make'
        mileage = 'mileage'
        model = 'model'
        previous_currency = 'previous_currency'
        previous_price = 'previous_price'
        price = 'price'
        product_priority_0 = 'product_priority_0'
        product_priority_1 = 'product_priority_1'
        product_priority_2 = 'product_priority_2'
        product_priority_3 = 'product_priority_3'
        product_priority_4 = 'product_priority_4'
        sale_currency = 'sale_currency'
        sale_price = 'sale_price'
        sanitized_images = 'sanitized_images'
        state_of_vehicle = 'state_of_vehicle'
        tags = 'tags'
        title = 'title'
        transmission = 'transmission'
        trim = 'trim'
        unit_price = 'unit_price'
        url = 'url'
        vehicle_id = 'vehicle_id'
        vehicle_registration_plate = 'vehicle_registration_plate'
        vehicle_specifications = 'vehicle_specifications'
        vehicle_type = 'vehicle_type'
        vin = 'vin'
        visibility = 'visibility'
        year = 'year'

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

    class Availability:
        available = 'AVAILABLE'
        not_available = 'NOT_AVAILABLE'
        pending = 'PENDING'
        unknown = 'UNKNOWN'

    class BodyStyle:
        convertible = 'CONVERTIBLE'
        coupe = 'COUPE'
        crossover = 'CROSSOVER'
        estate = 'ESTATE'
        grandtourer = 'GRANDTOURER'
        hatchback = 'HATCHBACK'
        minibus = 'MINIBUS'
        minivan = 'MINIVAN'
        mpv = 'MPV'
        none = 'NONE'
        other = 'OTHER'
        pickup = 'PICKUP'
        roadster = 'ROADSTER'
        saloon = 'SALOON'
        sedan = 'SEDAN'
        small_car = 'SMALL_CAR'
        sportscar = 'SPORTSCAR'
        supercar = 'SUPERCAR'
        supermini = 'SUPERMINI'
        suv = 'SUV'
        truck = 'TRUCK'
        van = 'VAN'
        wagon = 'WAGON'

    class Condition:
        excellent = 'EXCELLENT'
        fair = 'FAIR'
        good = 'GOOD'
        none = 'NONE'
        other = 'OTHER'
        poor = 'POOR'
        very_good = 'VERY_GOOD'

    class Drivetrain:
        awd = 'AWD'
        four_wd = 'FOUR_WD'
        fwd = 'FWD'
        none = 'NONE'
        other = 'OTHER'
        rwd = 'RWD'
        two_wd = 'TWO_WD'

    class FuelType:
        diesel = 'DIESEL'
        electric = 'ELECTRIC'
        flex = 'FLEX'
        gasoline = 'GASOLINE'
        hybrid = 'HYBRID'
        none = 'NONE'
        other = 'OTHER'
        petrol = 'PETROL'
        plugin_hybrid = 'PLUGIN_HYBRID'

    class StateOfVehicle:
        cpo = 'CPO'
        new = 'NEW'
        used = 'USED'

    class Transmission:
        automatic = 'AUTOMATIC'
        manual = 'MANUAL'
        none = 'NONE'
        other = 'OTHER'

    class VehicleType:
        boat = 'BOAT'
        car_truck = 'CAR_TRUCK'
        commercial = 'COMMERCIAL'
        motorcycle = 'MOTORCYCLE'
        other = 'OTHER'
        powersport = 'POWERSPORT'
        rv_camper = 'RV_CAMPER'
        trailer = 'TRAILER'

    # @deprecated get_endpoint function is deprecated
    @classmethod
    def get_endpoint(cls):
        return 'vehicles'

    # @deprecated api_create is being deprecated
    def api_create(self, parent_id, fields=None, params=None, batch=None, success=None, failure=None, pending=False):
        from facebook_business.adobjects.productcatalog import ProductCatalog
        return ProductCatalog(api=self._api, fbid=parent_id).create_vehicle(fields, params, batch, success, failure, pending)

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
            target_class=Vehicle,
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
            endpoint='/',
            api=self._api,
            param_checker=TypeChecker(param_types, enums),
            target_class=Vehicle,
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
        'address': 'Object',
        'applinks': 'CatalogItemAppLinks',
        'availability': 'string',
        'availability_circle_radius': 'float',
        'availability_circle_radius_unit': 'string',
        'body_style': 'string',
        'category_specific_fields': 'CatalogSubVerticalList',
        'condition': 'string',
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
        'date_first_on_lot': 'string',
        'dealer_communication_channel': 'string',
        'dealer_email': 'string',
        'dealer_id': 'string',
        'dealer_name': 'string',
        'dealer_phone': 'string',
        'dealer_privacy_policy_url': 'string',
        'description': 'string',
        'drivetrain': 'string',
        'exterior_color': 'string',
        'fb_page_id': 'Page',
        'features': 'list<Object>',
        'fuel_type': 'string',
        'id': 'string',
        'image_fetch_status': 'ImageFetchStatus',
        'images': 'list<string>',
        'interior_color': 'string',
        'legal_disclosure_impressum_url': 'string',
        'make': 'string',
        'mileage': 'Object',
        'model': 'string',
        'previous_currency': 'string',
        'previous_price': 'string',
        'price': 'string',
        'product_priority_0': 'float',
        'product_priority_1': 'float',
        'product_priority_2': 'float',
        'product_priority_3': 'float',
        'product_priority_4': 'float',
        'sale_currency': 'string',
        'sale_price': 'string',
        'sanitized_images': 'list<string>',
        'state_of_vehicle': 'string',
        'tags': 'list<string>',
        'title': 'string',
        'transmission': 'string',
        'trim': 'string',
        'unit_price': 'Object',
        'url': 'string',
        'vehicle_id': 'string',
        'vehicle_registration_plate': 'string',
        'vehicle_specifications': 'list<Object>',
        'vehicle_type': 'string',
        'vin': 'string',
        'visibility': 'Visibility',
        'year': 'unsigned int',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        field_enum_info['ImageFetchStatus'] = Vehicle.ImageFetchStatus.__dict__.values()
        field_enum_info['Visibility'] = Vehicle.Visibility.__dict__.values()
        field_enum_info['Availability'] = Vehicle.Availability.__dict__.values()
        field_enum_info['BodyStyle'] = Vehicle.BodyStyle.__dict__.values()
        field_enum_info['Condition'] = Vehicle.Condition.__dict__.values()
        field_enum_info['Drivetrain'] = Vehicle.Drivetrain.__dict__.values()
        field_enum_info['FuelType'] = Vehicle.FuelType.__dict__.values()
        field_enum_info['StateOfVehicle'] = Vehicle.StateOfVehicle.__dict__.values()
        field_enum_info['Transmission'] = Vehicle.Transmission.__dict__.values()
        field_enum_info['VehicleType'] = Vehicle.VehicleType.__dict__.values()
        return field_enum_info


