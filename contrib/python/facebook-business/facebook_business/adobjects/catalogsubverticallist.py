# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from facebook_business.adobjects.abstractobject import AbstractObject

"""
This class is auto-generated.

For any issues or feature requests related to this class, please let us know on
github and we'll fix in our codegen framework. We'll not be able to accept
pull request for this class.
"""

class CatalogSubVerticalList(
    AbstractObject,
):

    def __init__(self, api=None):
        super(CatalogSubVerticalList, self).__init__()
        self._isCatalogSubVerticalList = True
        self._api = api

    class Field(AbstractObject.Field):
        appliances = 'appliances'
        baby_feeding = 'baby_feeding'
        baby_transport = 'baby_transport'
        beauty = 'beauty'
        bedding = 'bedding'
        cameras = 'cameras'
        cameras_and_photos = 'cameras_and_photos'
        cell_phones_and_smart_watches = 'cell_phones_and_smart_watches'
        cleaning_supplies = 'cleaning_supplies'
        clo_offer = 'clo_offer'
        clothing = 'clothing'
        clothing_accessories = 'clothing_accessories'
        computer_components = 'computer_components'
        computers_and_tablets = 'computers_and_tablets'
        computers_laptops_and_tablets = 'computers_laptops_and_tablets'
        diapering_and_potty_training = 'diapering_and_potty_training'
        electronic_accessories_and_cables = 'electronic_accessories_and_cables'
        electronics_accessories = 'electronics_accessories'
        furniture = 'furniture'
        health = 'health'
        home = 'home'
        home_goods = 'home_goods'
        household_and_cleaning_supplies = 'household_and_cleaning_supplies'
        jewelry = 'jewelry'
        large_appliances = 'large_appliances'
        local_service_business_item = 'local_service_business_item'
        local_service_business_restaurant = 'local_service_business_restaurant'
        loyalty_offer = 'loyalty_offer'
        nursery = 'nursery'
        printers_and_scanners = 'printers_and_scanners'
        printers_scanners_and_fax_machines = 'printers_scanners_and_fax_machines'
        product_discount = 'product_discount'
        projectors = 'projectors'
        shoes = 'shoes'
        shoes_and_footwear = 'shoes_and_footwear'
        software = 'software'
        televisions_and_monitors = 'televisions_and_monitors'
        test_child_sub_vertical = 'test_child_sub_vertical'
        test_grand_child_sub_vertical = 'test_grand_child_sub_vertical'
        test_sub_vertical = 'test_sub_vertical'
        test_sub_vertical_alias = 'test_sub_vertical_alias'
        test_sub_vertical_data_object = 'test_sub_vertical_data_object'
        third_party_electronics = 'third_party_electronics'
        third_party_toys_and_games = 'third_party_toys_and_games'
        toys = 'toys'
        toys_and_games = 'toys_and_games'
        tvs_and_monitors = 'tvs_and_monitors'
        vehicle_manufacturer = 'vehicle_manufacturer'
        video_game_consoles_and_video_games = 'video_game_consoles_and_video_games'
        video_games_and_consoles = 'video_games_and_consoles'
        video_projectors = 'video_projectors'
        watches = 'watches'

    _field_types = {
        'appliances': 'Object',
        'baby_feeding': 'Object',
        'baby_transport': 'Object',
        'beauty': 'Object',
        'bedding': 'Object',
        'cameras': 'Object',
        'cameras_and_photos': 'Object',
        'cell_phones_and_smart_watches': 'Object',
        'cleaning_supplies': 'Object',
        'clo_offer': 'Object',
        'clothing': 'Object',
        'clothing_accessories': 'Object',
        'computer_components': 'Object',
        'computers_and_tablets': 'Object',
        'computers_laptops_and_tablets': 'Object',
        'diapering_and_potty_training': 'Object',
        'electronic_accessories_and_cables': 'Object',
        'electronics_accessories': 'Object',
        'furniture': 'Object',
        'health': 'Object',
        'home': 'Object',
        'home_goods': 'Object',
        'household_and_cleaning_supplies': 'Object',
        'jewelry': 'Object',
        'large_appliances': 'Object',
        'local_service_business_item': 'Object',
        'local_service_business_restaurant': 'Object',
        'loyalty_offer': 'Object',
        'nursery': 'Object',
        'printers_and_scanners': 'Object',
        'printers_scanners_and_fax_machines': 'Object',
        'product_discount': 'Object',
        'projectors': 'Object',
        'shoes': 'Object',
        'shoes_and_footwear': 'Object',
        'software': 'Object',
        'televisions_and_monitors': 'Object',
        'test_child_sub_vertical': 'Object',
        'test_grand_child_sub_vertical': 'Object',
        'test_sub_vertical': 'Object',
        'test_sub_vertical_alias': 'Object',
        'test_sub_vertical_data_object': 'Object',
        'third_party_electronics': 'Object',
        'third_party_toys_and_games': 'Object',
        'toys': 'Object',
        'toys_and_games': 'Object',
        'tvs_and_monitors': 'Object',
        'vehicle_manufacturer': 'Object',
        'video_game_consoles_and_video_games': 'Object',
        'video_games_and_consoles': 'Object',
        'video_projectors': 'Object',
        'watches': 'Object',
    }
    @classmethod
    def _get_field_enum_info(cls):
        field_enum_info = {}
        return field_enum_info


