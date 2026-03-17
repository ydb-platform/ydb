# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._test_helpers import APIResourceTestHelpers
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, cast, overload
from typing_extensions import Literal, Type, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.issuing._cardholder import Cardholder
    from stripe.issuing._personalization_design import PersonalizationDesign
    from stripe.params.issuing._card_create_params import CardCreateParams
    from stripe.params.issuing._card_deliver_card_params import (
        CardDeliverCardParams,
    )
    from stripe.params.issuing._card_fail_card_params import CardFailCardParams
    from stripe.params.issuing._card_list_params import CardListParams
    from stripe.params.issuing._card_modify_params import CardModifyParams
    from stripe.params.issuing._card_retrieve_params import CardRetrieveParams
    from stripe.params.issuing._card_return_card_params import (
        CardReturnCardParams,
    )
    from stripe.params.issuing._card_ship_card_params import CardShipCardParams
    from stripe.params.issuing._card_submit_card_params import (
        CardSubmitCardParams,
    )


class Card(
    CreateableAPIResource["Card"],
    ListableAPIResource["Card"],
    UpdateableAPIResource["Card"],
):
    """
    You can [create physical or virtual cards](https://docs.stripe.com/issuing) that are issued to cardholders.
    """

    OBJECT_NAME: ClassVar[Literal["issuing.card"]] = "issuing.card"

    class LatestFraudWarning(StripeObject):
        started_at: Optional[int]
        """
        Timestamp of the most recent fraud warning.
        """
        type: Optional[
            Literal[
                "card_testing_exposure",
                "fraud_dispute_filed",
                "third_party_reported",
                "user_indicated_fraud",
            ]
        ]
        """
        The type of fraud warning that most recently took place on this card. This field updates with every new fraud warning, so the value changes over time. If populated, cancel and reissue the card.
        """

    class Shipping(StripeObject):
        class Address(StripeObject):
            city: Optional[str]
            """
            City, district, suburb, town, or village.
            """
            country: Optional[str]
            """
            Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
            """
            line1: Optional[str]
            """
            Address line 1, such as the street, PO Box, or company name.
            """
            line2: Optional[str]
            """
            Address line 2, such as the apartment, suite, unit, or building.
            """
            postal_code: Optional[str]
            """
            ZIP or postal code.
            """
            state: Optional[str]
            """
            State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
            """

        class AddressValidation(StripeObject):
            class NormalizedAddress(StripeObject):
                city: Optional[str]
                """
                City, district, suburb, town, or village.
                """
                country: Optional[str]
                """
                Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
                """
                line1: Optional[str]
                """
                Address line 1, such as the street, PO Box, or company name.
                """
                line2: Optional[str]
                """
                Address line 2, such as the apartment, suite, unit, or building.
                """
                postal_code: Optional[str]
                """
                ZIP or postal code.
                """
                state: Optional[str]
                """
                State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
                """

            mode: Literal[
                "disabled",
                "normalization_only",
                "validation_and_normalization",
            ]
            """
            The address validation capabilities to use.
            """
            normalized_address: Optional[NormalizedAddress]
            """
            The normalized shipping address.
            """
            result: Optional[
                Literal[
                    "indeterminate",
                    "likely_deliverable",
                    "likely_undeliverable",
                ]
            ]
            """
            The validation result for the shipping address.
            """
            _inner_class_types = {"normalized_address": NormalizedAddress}

        class Customs(StripeObject):
            eori_number: Optional[str]
            """
            A registration number used for customs in Europe. See [https://www.gov.uk/eori](https://www.gov.uk/eori) for the UK and [https://ec.europa.eu/taxation_customs/business/customs-procedures-import-and-export/customs-procedures/economic-operators-registration-and-identification-number-eori_en](https://ec.europa.eu/taxation_customs/business/customs-procedures-import-and-export/customs-procedures/economic-operators-registration-and-identification-number-eori_en) for the EU.
            """

        address: Address
        address_validation: Optional[AddressValidation]
        """
        Address validation details for the shipment.
        """
        carrier: Optional[Literal["dhl", "fedex", "royal_mail", "usps"]]
        """
        The delivery company that shipped a card.
        """
        customs: Optional[Customs]
        """
        Additional information that may be required for clearing customs.
        """
        eta: Optional[int]
        """
        A unix timestamp representing a best estimate of when the card will be delivered.
        """
        name: str
        """
        Recipient name.
        """
        phone_number: Optional[str]
        """
        The phone number of the receiver of the shipment. Our courier partners will use this number to contact you in the event of card delivery issues. For individual shipments to the EU/UK, if this field is empty, we will provide them with the phone number provided when the cardholder was initially created.
        """
        require_signature: Optional[bool]
        """
        Whether a signature is required for card delivery. This feature is only supported for US users. Standard shipping service does not support signature on delivery. The default value for standard shipping service is false and for express and priority services is true.
        """
        service: Literal["express", "priority", "standard"]
        """
        Shipment service, such as `standard` or `express`.
        """
        status: Optional[
            Literal[
                "canceled",
                "delivered",
                "failure",
                "pending",
                "returned",
                "shipped",
                "submitted",
            ]
        ]
        """
        The delivery status of the card.
        """
        tracking_number: Optional[str]
        """
        A tracking number for a card shipment.
        """
        tracking_url: Optional[str]
        """
        A link to the shipping carrier's site where you can view detailed information about a card shipment.
        """
        type: Literal["bulk", "individual"]
        """
        Packaging options.
        """
        _inner_class_types = {
            "address": Address,
            "address_validation": AddressValidation,
            "customs": Customs,
        }

    class SpendingControls(StripeObject):
        class SpendingLimit(StripeObject):
            amount: int
            """
            Maximum amount allowed to spend per interval. This amount is in the card's currency and in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
            """
            categories: Optional[
                List[
                    Literal[
                        "ac_refrigeration_repair",
                        "accounting_bookkeeping_services",
                        "advertising_services",
                        "agricultural_cooperative",
                        "airlines_air_carriers",
                        "airports_flying_fields",
                        "ambulance_services",
                        "amusement_parks_carnivals",
                        "antique_reproductions",
                        "antique_shops",
                        "aquariums",
                        "architectural_surveying_services",
                        "art_dealers_and_galleries",
                        "artists_supply_and_craft_shops",
                        "auto_and_home_supply_stores",
                        "auto_body_repair_shops",
                        "auto_paint_shops",
                        "auto_service_shops",
                        "automated_cash_disburse",
                        "automated_fuel_dispensers",
                        "automobile_associations",
                        "automotive_parts_and_accessories_stores",
                        "automotive_tire_stores",
                        "bail_and_bond_payments",
                        "bakeries",
                        "bands_orchestras",
                        "barber_and_beauty_shops",
                        "betting_casino_gambling",
                        "bicycle_shops",
                        "billiard_pool_establishments",
                        "boat_dealers",
                        "boat_rentals_and_leases",
                        "book_stores",
                        "books_periodicals_and_newspapers",
                        "bowling_alleys",
                        "bus_lines",
                        "business_secretarial_schools",
                        "buying_shopping_services",
                        "cable_satellite_and_other_pay_television_and_radio",
                        "camera_and_photographic_supply_stores",
                        "candy_nut_and_confectionery_stores",
                        "car_and_truck_dealers_new_used",
                        "car_and_truck_dealers_used_only",
                        "car_rental_agencies",
                        "car_washes",
                        "carpentry_services",
                        "carpet_upholstery_cleaning",
                        "caterers",
                        "charitable_and_social_service_organizations_fundraising",
                        "chemicals_and_allied_products",
                        "child_care_services",
                        "childrens_and_infants_wear_stores",
                        "chiropodists_podiatrists",
                        "chiropractors",
                        "cigar_stores_and_stands",
                        "civic_social_fraternal_associations",
                        "cleaning_and_maintenance",
                        "clothing_rental",
                        "colleges_universities",
                        "commercial_equipment",
                        "commercial_footwear",
                        "commercial_photography_art_and_graphics",
                        "commuter_transport_and_ferries",
                        "computer_network_services",
                        "computer_programming",
                        "computer_repair",
                        "computer_software_stores",
                        "computers_peripherals_and_software",
                        "concrete_work_services",
                        "construction_materials",
                        "consulting_public_relations",
                        "correspondence_schools",
                        "cosmetic_stores",
                        "counseling_services",
                        "country_clubs",
                        "courier_services",
                        "court_costs",
                        "credit_reporting_agencies",
                        "cruise_lines",
                        "dairy_products_stores",
                        "dance_hall_studios_schools",
                        "dating_escort_services",
                        "dentists_orthodontists",
                        "department_stores",
                        "detective_agencies",
                        "digital_goods_applications",
                        "digital_goods_games",
                        "digital_goods_large_volume",
                        "digital_goods_media",
                        "direct_marketing_catalog_merchant",
                        "direct_marketing_combination_catalog_and_retail_merchant",
                        "direct_marketing_inbound_telemarketing",
                        "direct_marketing_insurance_services",
                        "direct_marketing_other",
                        "direct_marketing_outbound_telemarketing",
                        "direct_marketing_subscription",
                        "direct_marketing_travel",
                        "discount_stores",
                        "doctors",
                        "door_to_door_sales",
                        "drapery_window_covering_and_upholstery_stores",
                        "drinking_places",
                        "drug_stores_and_pharmacies",
                        "drugs_drug_proprietaries_and_druggist_sundries",
                        "dry_cleaners",
                        "durable_goods",
                        "duty_free_stores",
                        "eating_places_restaurants",
                        "educational_services",
                        "electric_razor_stores",
                        "electric_vehicle_charging",
                        "electrical_parts_and_equipment",
                        "electrical_services",
                        "electronics_repair_shops",
                        "electronics_stores",
                        "elementary_secondary_schools",
                        "emergency_services_gcas_visa_use_only",
                        "employment_temp_agencies",
                        "equipment_rental",
                        "exterminating_services",
                        "family_clothing_stores",
                        "fast_food_restaurants",
                        "financial_institutions",
                        "fines_government_administrative_entities",
                        "fireplace_fireplace_screens_and_accessories_stores",
                        "floor_covering_stores",
                        "florists",
                        "florists_supplies_nursery_stock_and_flowers",
                        "freezer_and_locker_meat_provisioners",
                        "fuel_dealers_non_automotive",
                        "funeral_services_crematories",
                        "furniture_home_furnishings_and_equipment_stores_except_appliances",
                        "furniture_repair_refinishing",
                        "furriers_and_fur_shops",
                        "general_services",
                        "gift_card_novelty_and_souvenir_shops",
                        "glass_paint_and_wallpaper_stores",
                        "glassware_crystal_stores",
                        "golf_courses_public",
                        "government_licensed_horse_dog_racing_us_region_only",
                        "government_licensed_online_casions_online_gambling_us_region_only",
                        "government_owned_lotteries_non_us_region",
                        "government_owned_lotteries_us_region_only",
                        "government_services",
                        "grocery_stores_supermarkets",
                        "hardware_equipment_and_supplies",
                        "hardware_stores",
                        "health_and_beauty_spas",
                        "hearing_aids_sales_and_supplies",
                        "heating_plumbing_a_c",
                        "hobby_toy_and_game_shops",
                        "home_supply_warehouse_stores",
                        "hospitals",
                        "hotels_motels_and_resorts",
                        "household_appliance_stores",
                        "industrial_supplies",
                        "information_retrieval_services",
                        "insurance_default",
                        "insurance_underwriting_premiums",
                        "intra_company_purchases",
                        "jewelry_stores_watches_clocks_and_silverware_stores",
                        "landscaping_services",
                        "laundries",
                        "laundry_cleaning_services",
                        "legal_services_attorneys",
                        "luggage_and_leather_goods_stores",
                        "lumber_building_materials_stores",
                        "manual_cash_disburse",
                        "marinas_service_and_supplies",
                        "marketplaces",
                        "masonry_stonework_and_plaster",
                        "massage_parlors",
                        "medical_and_dental_labs",
                        "medical_dental_ophthalmic_and_hospital_equipment_and_supplies",
                        "medical_services",
                        "membership_organizations",
                        "mens_and_boys_clothing_and_accessories_stores",
                        "mens_womens_clothing_stores",
                        "metal_service_centers",
                        "miscellaneous",
                        "miscellaneous_apparel_and_accessory_shops",
                        "miscellaneous_auto_dealers",
                        "miscellaneous_business_services",
                        "miscellaneous_food_stores",
                        "miscellaneous_general_merchandise",
                        "miscellaneous_general_services",
                        "miscellaneous_home_furnishing_specialty_stores",
                        "miscellaneous_publishing_and_printing",
                        "miscellaneous_recreation_services",
                        "miscellaneous_repair_shops",
                        "miscellaneous_specialty_retail",
                        "mobile_home_dealers",
                        "motion_picture_theaters",
                        "motor_freight_carriers_and_trucking",
                        "motor_homes_dealers",
                        "motor_vehicle_supplies_and_new_parts",
                        "motorcycle_shops_and_dealers",
                        "motorcycle_shops_dealers",
                        "music_stores_musical_instruments_pianos_and_sheet_music",
                        "news_dealers_and_newsstands",
                        "non_fi_money_orders",
                        "non_fi_stored_value_card_purchase_load",
                        "nondurable_goods",
                        "nurseries_lawn_and_garden_supply_stores",
                        "nursing_personal_care",
                        "office_and_commercial_furniture",
                        "opticians_eyeglasses",
                        "optometrists_ophthalmologist",
                        "orthopedic_goods_prosthetic_devices",
                        "osteopaths",
                        "package_stores_beer_wine_and_liquor",
                        "paints_varnishes_and_supplies",
                        "parking_lots_garages",
                        "passenger_railways",
                        "pawn_shops",
                        "pet_shops_pet_food_and_supplies",
                        "petroleum_and_petroleum_products",
                        "photo_developing",
                        "photographic_photocopy_microfilm_equipment_and_supplies",
                        "photographic_studios",
                        "picture_video_production",
                        "piece_goods_notions_and_other_dry_goods",
                        "plumbing_heating_equipment_and_supplies",
                        "political_organizations",
                        "postal_services_government_only",
                        "precious_stones_and_metals_watches_and_jewelry",
                        "professional_services",
                        "public_warehousing_and_storage",
                        "quick_copy_repro_and_blueprint",
                        "railroads",
                        "real_estate_agents_and_managers_rentals",
                        "record_stores",
                        "recreational_vehicle_rentals",
                        "religious_goods_stores",
                        "religious_organizations",
                        "roofing_siding_sheet_metal",
                        "secretarial_support_services",
                        "security_brokers_dealers",
                        "service_stations",
                        "sewing_needlework_fabric_and_piece_goods_stores",
                        "shoe_repair_hat_cleaning",
                        "shoe_stores",
                        "small_appliance_repair",
                        "snowmobile_dealers",
                        "special_trade_services",
                        "specialty_cleaning",
                        "sporting_goods_stores",
                        "sporting_recreation_camps",
                        "sports_and_riding_apparel_stores",
                        "sports_clubs_fields",
                        "stamp_and_coin_stores",
                        "stationary_office_supplies_printing_and_writing_paper",
                        "stationery_stores_office_and_school_supply_stores",
                        "swimming_pools_sales",
                        "t_ui_travel_germany",
                        "tailors_alterations",
                        "tax_payments_government_agencies",
                        "tax_preparation_services",
                        "taxicabs_limousines",
                        "telecommunication_equipment_and_telephone_sales",
                        "telecommunication_services",
                        "telegraph_services",
                        "tent_and_awning_shops",
                        "testing_laboratories",
                        "theatrical_ticket_agencies",
                        "timeshares",
                        "tire_retreading_and_repair",
                        "tolls_bridge_fees",
                        "tourist_attractions_and_exhibits",
                        "towing_services",
                        "trailer_parks_campgrounds",
                        "transportation_services",
                        "travel_agencies_tour_operators",
                        "truck_stop_iteration",
                        "truck_utility_trailer_rentals",
                        "typesetting_plate_making_and_related_services",
                        "typewriter_stores",
                        "u_s_federal_government_agencies_or_departments",
                        "uniforms_commercial_clothing",
                        "used_merchandise_and_secondhand_stores",
                        "utilities",
                        "variety_stores",
                        "veterinary_services",
                        "video_amusement_game_supplies",
                        "video_game_arcades",
                        "video_tape_rental_stores",
                        "vocational_trade_schools",
                        "watch_jewelry_repair",
                        "welding_repair",
                        "wholesale_clubs",
                        "wig_and_toupee_stores",
                        "wires_money_orders",
                        "womens_accessory_and_specialty_shops",
                        "womens_ready_to_wear_stores",
                        "wrecking_and_salvage_yards",
                    ]
                ]
            ]
            """
            Array of strings containing [categories](https://docs.stripe.com/api#issuing_authorization_object-merchant_data-category) this limit applies to. Omitting this field will apply the limit to all categories.
            """
            interval: Literal[
                "all_time",
                "daily",
                "monthly",
                "per_authorization",
                "weekly",
                "yearly",
            ]
            """
            Interval (or event) to which the amount applies.
            """

        allowed_categories: Optional[
            List[
                Literal[
                    "ac_refrigeration_repair",
                    "accounting_bookkeeping_services",
                    "advertising_services",
                    "agricultural_cooperative",
                    "airlines_air_carriers",
                    "airports_flying_fields",
                    "ambulance_services",
                    "amusement_parks_carnivals",
                    "antique_reproductions",
                    "antique_shops",
                    "aquariums",
                    "architectural_surveying_services",
                    "art_dealers_and_galleries",
                    "artists_supply_and_craft_shops",
                    "auto_and_home_supply_stores",
                    "auto_body_repair_shops",
                    "auto_paint_shops",
                    "auto_service_shops",
                    "automated_cash_disburse",
                    "automated_fuel_dispensers",
                    "automobile_associations",
                    "automotive_parts_and_accessories_stores",
                    "automotive_tire_stores",
                    "bail_and_bond_payments",
                    "bakeries",
                    "bands_orchestras",
                    "barber_and_beauty_shops",
                    "betting_casino_gambling",
                    "bicycle_shops",
                    "billiard_pool_establishments",
                    "boat_dealers",
                    "boat_rentals_and_leases",
                    "book_stores",
                    "books_periodicals_and_newspapers",
                    "bowling_alleys",
                    "bus_lines",
                    "business_secretarial_schools",
                    "buying_shopping_services",
                    "cable_satellite_and_other_pay_television_and_radio",
                    "camera_and_photographic_supply_stores",
                    "candy_nut_and_confectionery_stores",
                    "car_and_truck_dealers_new_used",
                    "car_and_truck_dealers_used_only",
                    "car_rental_agencies",
                    "car_washes",
                    "carpentry_services",
                    "carpet_upholstery_cleaning",
                    "caterers",
                    "charitable_and_social_service_organizations_fundraising",
                    "chemicals_and_allied_products",
                    "child_care_services",
                    "childrens_and_infants_wear_stores",
                    "chiropodists_podiatrists",
                    "chiropractors",
                    "cigar_stores_and_stands",
                    "civic_social_fraternal_associations",
                    "cleaning_and_maintenance",
                    "clothing_rental",
                    "colleges_universities",
                    "commercial_equipment",
                    "commercial_footwear",
                    "commercial_photography_art_and_graphics",
                    "commuter_transport_and_ferries",
                    "computer_network_services",
                    "computer_programming",
                    "computer_repair",
                    "computer_software_stores",
                    "computers_peripherals_and_software",
                    "concrete_work_services",
                    "construction_materials",
                    "consulting_public_relations",
                    "correspondence_schools",
                    "cosmetic_stores",
                    "counseling_services",
                    "country_clubs",
                    "courier_services",
                    "court_costs",
                    "credit_reporting_agencies",
                    "cruise_lines",
                    "dairy_products_stores",
                    "dance_hall_studios_schools",
                    "dating_escort_services",
                    "dentists_orthodontists",
                    "department_stores",
                    "detective_agencies",
                    "digital_goods_applications",
                    "digital_goods_games",
                    "digital_goods_large_volume",
                    "digital_goods_media",
                    "direct_marketing_catalog_merchant",
                    "direct_marketing_combination_catalog_and_retail_merchant",
                    "direct_marketing_inbound_telemarketing",
                    "direct_marketing_insurance_services",
                    "direct_marketing_other",
                    "direct_marketing_outbound_telemarketing",
                    "direct_marketing_subscription",
                    "direct_marketing_travel",
                    "discount_stores",
                    "doctors",
                    "door_to_door_sales",
                    "drapery_window_covering_and_upholstery_stores",
                    "drinking_places",
                    "drug_stores_and_pharmacies",
                    "drugs_drug_proprietaries_and_druggist_sundries",
                    "dry_cleaners",
                    "durable_goods",
                    "duty_free_stores",
                    "eating_places_restaurants",
                    "educational_services",
                    "electric_razor_stores",
                    "electric_vehicle_charging",
                    "electrical_parts_and_equipment",
                    "electrical_services",
                    "electronics_repair_shops",
                    "electronics_stores",
                    "elementary_secondary_schools",
                    "emergency_services_gcas_visa_use_only",
                    "employment_temp_agencies",
                    "equipment_rental",
                    "exterminating_services",
                    "family_clothing_stores",
                    "fast_food_restaurants",
                    "financial_institutions",
                    "fines_government_administrative_entities",
                    "fireplace_fireplace_screens_and_accessories_stores",
                    "floor_covering_stores",
                    "florists",
                    "florists_supplies_nursery_stock_and_flowers",
                    "freezer_and_locker_meat_provisioners",
                    "fuel_dealers_non_automotive",
                    "funeral_services_crematories",
                    "furniture_home_furnishings_and_equipment_stores_except_appliances",
                    "furniture_repair_refinishing",
                    "furriers_and_fur_shops",
                    "general_services",
                    "gift_card_novelty_and_souvenir_shops",
                    "glass_paint_and_wallpaper_stores",
                    "glassware_crystal_stores",
                    "golf_courses_public",
                    "government_licensed_horse_dog_racing_us_region_only",
                    "government_licensed_online_casions_online_gambling_us_region_only",
                    "government_owned_lotteries_non_us_region",
                    "government_owned_lotteries_us_region_only",
                    "government_services",
                    "grocery_stores_supermarkets",
                    "hardware_equipment_and_supplies",
                    "hardware_stores",
                    "health_and_beauty_spas",
                    "hearing_aids_sales_and_supplies",
                    "heating_plumbing_a_c",
                    "hobby_toy_and_game_shops",
                    "home_supply_warehouse_stores",
                    "hospitals",
                    "hotels_motels_and_resorts",
                    "household_appliance_stores",
                    "industrial_supplies",
                    "information_retrieval_services",
                    "insurance_default",
                    "insurance_underwriting_premiums",
                    "intra_company_purchases",
                    "jewelry_stores_watches_clocks_and_silverware_stores",
                    "landscaping_services",
                    "laundries",
                    "laundry_cleaning_services",
                    "legal_services_attorneys",
                    "luggage_and_leather_goods_stores",
                    "lumber_building_materials_stores",
                    "manual_cash_disburse",
                    "marinas_service_and_supplies",
                    "marketplaces",
                    "masonry_stonework_and_plaster",
                    "massage_parlors",
                    "medical_and_dental_labs",
                    "medical_dental_ophthalmic_and_hospital_equipment_and_supplies",
                    "medical_services",
                    "membership_organizations",
                    "mens_and_boys_clothing_and_accessories_stores",
                    "mens_womens_clothing_stores",
                    "metal_service_centers",
                    "miscellaneous",
                    "miscellaneous_apparel_and_accessory_shops",
                    "miscellaneous_auto_dealers",
                    "miscellaneous_business_services",
                    "miscellaneous_food_stores",
                    "miscellaneous_general_merchandise",
                    "miscellaneous_general_services",
                    "miscellaneous_home_furnishing_specialty_stores",
                    "miscellaneous_publishing_and_printing",
                    "miscellaneous_recreation_services",
                    "miscellaneous_repair_shops",
                    "miscellaneous_specialty_retail",
                    "mobile_home_dealers",
                    "motion_picture_theaters",
                    "motor_freight_carriers_and_trucking",
                    "motor_homes_dealers",
                    "motor_vehicle_supplies_and_new_parts",
                    "motorcycle_shops_and_dealers",
                    "motorcycle_shops_dealers",
                    "music_stores_musical_instruments_pianos_and_sheet_music",
                    "news_dealers_and_newsstands",
                    "non_fi_money_orders",
                    "non_fi_stored_value_card_purchase_load",
                    "nondurable_goods",
                    "nurseries_lawn_and_garden_supply_stores",
                    "nursing_personal_care",
                    "office_and_commercial_furniture",
                    "opticians_eyeglasses",
                    "optometrists_ophthalmologist",
                    "orthopedic_goods_prosthetic_devices",
                    "osteopaths",
                    "package_stores_beer_wine_and_liquor",
                    "paints_varnishes_and_supplies",
                    "parking_lots_garages",
                    "passenger_railways",
                    "pawn_shops",
                    "pet_shops_pet_food_and_supplies",
                    "petroleum_and_petroleum_products",
                    "photo_developing",
                    "photographic_photocopy_microfilm_equipment_and_supplies",
                    "photographic_studios",
                    "picture_video_production",
                    "piece_goods_notions_and_other_dry_goods",
                    "plumbing_heating_equipment_and_supplies",
                    "political_organizations",
                    "postal_services_government_only",
                    "precious_stones_and_metals_watches_and_jewelry",
                    "professional_services",
                    "public_warehousing_and_storage",
                    "quick_copy_repro_and_blueprint",
                    "railroads",
                    "real_estate_agents_and_managers_rentals",
                    "record_stores",
                    "recreational_vehicle_rentals",
                    "religious_goods_stores",
                    "religious_organizations",
                    "roofing_siding_sheet_metal",
                    "secretarial_support_services",
                    "security_brokers_dealers",
                    "service_stations",
                    "sewing_needlework_fabric_and_piece_goods_stores",
                    "shoe_repair_hat_cleaning",
                    "shoe_stores",
                    "small_appliance_repair",
                    "snowmobile_dealers",
                    "special_trade_services",
                    "specialty_cleaning",
                    "sporting_goods_stores",
                    "sporting_recreation_camps",
                    "sports_and_riding_apparel_stores",
                    "sports_clubs_fields",
                    "stamp_and_coin_stores",
                    "stationary_office_supplies_printing_and_writing_paper",
                    "stationery_stores_office_and_school_supply_stores",
                    "swimming_pools_sales",
                    "t_ui_travel_germany",
                    "tailors_alterations",
                    "tax_payments_government_agencies",
                    "tax_preparation_services",
                    "taxicabs_limousines",
                    "telecommunication_equipment_and_telephone_sales",
                    "telecommunication_services",
                    "telegraph_services",
                    "tent_and_awning_shops",
                    "testing_laboratories",
                    "theatrical_ticket_agencies",
                    "timeshares",
                    "tire_retreading_and_repair",
                    "tolls_bridge_fees",
                    "tourist_attractions_and_exhibits",
                    "towing_services",
                    "trailer_parks_campgrounds",
                    "transportation_services",
                    "travel_agencies_tour_operators",
                    "truck_stop_iteration",
                    "truck_utility_trailer_rentals",
                    "typesetting_plate_making_and_related_services",
                    "typewriter_stores",
                    "u_s_federal_government_agencies_or_departments",
                    "uniforms_commercial_clothing",
                    "used_merchandise_and_secondhand_stores",
                    "utilities",
                    "variety_stores",
                    "veterinary_services",
                    "video_amusement_game_supplies",
                    "video_game_arcades",
                    "video_tape_rental_stores",
                    "vocational_trade_schools",
                    "watch_jewelry_repair",
                    "welding_repair",
                    "wholesale_clubs",
                    "wig_and_toupee_stores",
                    "wires_money_orders",
                    "womens_accessory_and_specialty_shops",
                    "womens_ready_to_wear_stores",
                    "wrecking_and_salvage_yards",
                ]
            ]
        ]
        """
        Array of strings containing [categories](https://docs.stripe.com/api#issuing_authorization_object-merchant_data-category) of authorizations to allow. All other categories will be blocked. Cannot be set with `blocked_categories`.
        """
        allowed_merchant_countries: Optional[List[str]]
        """
        Array of strings containing representing countries from which authorizations will be allowed. Authorizations from merchants in all other countries will be declined. Country codes should be ISO 3166 alpha-2 country codes (e.g. `US`). Cannot be set with `blocked_merchant_countries`. Provide an empty value to unset this control.
        """
        blocked_categories: Optional[
            List[
                Literal[
                    "ac_refrigeration_repair",
                    "accounting_bookkeeping_services",
                    "advertising_services",
                    "agricultural_cooperative",
                    "airlines_air_carriers",
                    "airports_flying_fields",
                    "ambulance_services",
                    "amusement_parks_carnivals",
                    "antique_reproductions",
                    "antique_shops",
                    "aquariums",
                    "architectural_surveying_services",
                    "art_dealers_and_galleries",
                    "artists_supply_and_craft_shops",
                    "auto_and_home_supply_stores",
                    "auto_body_repair_shops",
                    "auto_paint_shops",
                    "auto_service_shops",
                    "automated_cash_disburse",
                    "automated_fuel_dispensers",
                    "automobile_associations",
                    "automotive_parts_and_accessories_stores",
                    "automotive_tire_stores",
                    "bail_and_bond_payments",
                    "bakeries",
                    "bands_orchestras",
                    "barber_and_beauty_shops",
                    "betting_casino_gambling",
                    "bicycle_shops",
                    "billiard_pool_establishments",
                    "boat_dealers",
                    "boat_rentals_and_leases",
                    "book_stores",
                    "books_periodicals_and_newspapers",
                    "bowling_alleys",
                    "bus_lines",
                    "business_secretarial_schools",
                    "buying_shopping_services",
                    "cable_satellite_and_other_pay_television_and_radio",
                    "camera_and_photographic_supply_stores",
                    "candy_nut_and_confectionery_stores",
                    "car_and_truck_dealers_new_used",
                    "car_and_truck_dealers_used_only",
                    "car_rental_agencies",
                    "car_washes",
                    "carpentry_services",
                    "carpet_upholstery_cleaning",
                    "caterers",
                    "charitable_and_social_service_organizations_fundraising",
                    "chemicals_and_allied_products",
                    "child_care_services",
                    "childrens_and_infants_wear_stores",
                    "chiropodists_podiatrists",
                    "chiropractors",
                    "cigar_stores_and_stands",
                    "civic_social_fraternal_associations",
                    "cleaning_and_maintenance",
                    "clothing_rental",
                    "colleges_universities",
                    "commercial_equipment",
                    "commercial_footwear",
                    "commercial_photography_art_and_graphics",
                    "commuter_transport_and_ferries",
                    "computer_network_services",
                    "computer_programming",
                    "computer_repair",
                    "computer_software_stores",
                    "computers_peripherals_and_software",
                    "concrete_work_services",
                    "construction_materials",
                    "consulting_public_relations",
                    "correspondence_schools",
                    "cosmetic_stores",
                    "counseling_services",
                    "country_clubs",
                    "courier_services",
                    "court_costs",
                    "credit_reporting_agencies",
                    "cruise_lines",
                    "dairy_products_stores",
                    "dance_hall_studios_schools",
                    "dating_escort_services",
                    "dentists_orthodontists",
                    "department_stores",
                    "detective_agencies",
                    "digital_goods_applications",
                    "digital_goods_games",
                    "digital_goods_large_volume",
                    "digital_goods_media",
                    "direct_marketing_catalog_merchant",
                    "direct_marketing_combination_catalog_and_retail_merchant",
                    "direct_marketing_inbound_telemarketing",
                    "direct_marketing_insurance_services",
                    "direct_marketing_other",
                    "direct_marketing_outbound_telemarketing",
                    "direct_marketing_subscription",
                    "direct_marketing_travel",
                    "discount_stores",
                    "doctors",
                    "door_to_door_sales",
                    "drapery_window_covering_and_upholstery_stores",
                    "drinking_places",
                    "drug_stores_and_pharmacies",
                    "drugs_drug_proprietaries_and_druggist_sundries",
                    "dry_cleaners",
                    "durable_goods",
                    "duty_free_stores",
                    "eating_places_restaurants",
                    "educational_services",
                    "electric_razor_stores",
                    "electric_vehicle_charging",
                    "electrical_parts_and_equipment",
                    "electrical_services",
                    "electronics_repair_shops",
                    "electronics_stores",
                    "elementary_secondary_schools",
                    "emergency_services_gcas_visa_use_only",
                    "employment_temp_agencies",
                    "equipment_rental",
                    "exterminating_services",
                    "family_clothing_stores",
                    "fast_food_restaurants",
                    "financial_institutions",
                    "fines_government_administrative_entities",
                    "fireplace_fireplace_screens_and_accessories_stores",
                    "floor_covering_stores",
                    "florists",
                    "florists_supplies_nursery_stock_and_flowers",
                    "freezer_and_locker_meat_provisioners",
                    "fuel_dealers_non_automotive",
                    "funeral_services_crematories",
                    "furniture_home_furnishings_and_equipment_stores_except_appliances",
                    "furniture_repair_refinishing",
                    "furriers_and_fur_shops",
                    "general_services",
                    "gift_card_novelty_and_souvenir_shops",
                    "glass_paint_and_wallpaper_stores",
                    "glassware_crystal_stores",
                    "golf_courses_public",
                    "government_licensed_horse_dog_racing_us_region_only",
                    "government_licensed_online_casions_online_gambling_us_region_only",
                    "government_owned_lotteries_non_us_region",
                    "government_owned_lotteries_us_region_only",
                    "government_services",
                    "grocery_stores_supermarkets",
                    "hardware_equipment_and_supplies",
                    "hardware_stores",
                    "health_and_beauty_spas",
                    "hearing_aids_sales_and_supplies",
                    "heating_plumbing_a_c",
                    "hobby_toy_and_game_shops",
                    "home_supply_warehouse_stores",
                    "hospitals",
                    "hotels_motels_and_resorts",
                    "household_appliance_stores",
                    "industrial_supplies",
                    "information_retrieval_services",
                    "insurance_default",
                    "insurance_underwriting_premiums",
                    "intra_company_purchases",
                    "jewelry_stores_watches_clocks_and_silverware_stores",
                    "landscaping_services",
                    "laundries",
                    "laundry_cleaning_services",
                    "legal_services_attorneys",
                    "luggage_and_leather_goods_stores",
                    "lumber_building_materials_stores",
                    "manual_cash_disburse",
                    "marinas_service_and_supplies",
                    "marketplaces",
                    "masonry_stonework_and_plaster",
                    "massage_parlors",
                    "medical_and_dental_labs",
                    "medical_dental_ophthalmic_and_hospital_equipment_and_supplies",
                    "medical_services",
                    "membership_organizations",
                    "mens_and_boys_clothing_and_accessories_stores",
                    "mens_womens_clothing_stores",
                    "metal_service_centers",
                    "miscellaneous",
                    "miscellaneous_apparel_and_accessory_shops",
                    "miscellaneous_auto_dealers",
                    "miscellaneous_business_services",
                    "miscellaneous_food_stores",
                    "miscellaneous_general_merchandise",
                    "miscellaneous_general_services",
                    "miscellaneous_home_furnishing_specialty_stores",
                    "miscellaneous_publishing_and_printing",
                    "miscellaneous_recreation_services",
                    "miscellaneous_repair_shops",
                    "miscellaneous_specialty_retail",
                    "mobile_home_dealers",
                    "motion_picture_theaters",
                    "motor_freight_carriers_and_trucking",
                    "motor_homes_dealers",
                    "motor_vehicle_supplies_and_new_parts",
                    "motorcycle_shops_and_dealers",
                    "motorcycle_shops_dealers",
                    "music_stores_musical_instruments_pianos_and_sheet_music",
                    "news_dealers_and_newsstands",
                    "non_fi_money_orders",
                    "non_fi_stored_value_card_purchase_load",
                    "nondurable_goods",
                    "nurseries_lawn_and_garden_supply_stores",
                    "nursing_personal_care",
                    "office_and_commercial_furniture",
                    "opticians_eyeglasses",
                    "optometrists_ophthalmologist",
                    "orthopedic_goods_prosthetic_devices",
                    "osteopaths",
                    "package_stores_beer_wine_and_liquor",
                    "paints_varnishes_and_supplies",
                    "parking_lots_garages",
                    "passenger_railways",
                    "pawn_shops",
                    "pet_shops_pet_food_and_supplies",
                    "petroleum_and_petroleum_products",
                    "photo_developing",
                    "photographic_photocopy_microfilm_equipment_and_supplies",
                    "photographic_studios",
                    "picture_video_production",
                    "piece_goods_notions_and_other_dry_goods",
                    "plumbing_heating_equipment_and_supplies",
                    "political_organizations",
                    "postal_services_government_only",
                    "precious_stones_and_metals_watches_and_jewelry",
                    "professional_services",
                    "public_warehousing_and_storage",
                    "quick_copy_repro_and_blueprint",
                    "railroads",
                    "real_estate_agents_and_managers_rentals",
                    "record_stores",
                    "recreational_vehicle_rentals",
                    "religious_goods_stores",
                    "religious_organizations",
                    "roofing_siding_sheet_metal",
                    "secretarial_support_services",
                    "security_brokers_dealers",
                    "service_stations",
                    "sewing_needlework_fabric_and_piece_goods_stores",
                    "shoe_repair_hat_cleaning",
                    "shoe_stores",
                    "small_appliance_repair",
                    "snowmobile_dealers",
                    "special_trade_services",
                    "specialty_cleaning",
                    "sporting_goods_stores",
                    "sporting_recreation_camps",
                    "sports_and_riding_apparel_stores",
                    "sports_clubs_fields",
                    "stamp_and_coin_stores",
                    "stationary_office_supplies_printing_and_writing_paper",
                    "stationery_stores_office_and_school_supply_stores",
                    "swimming_pools_sales",
                    "t_ui_travel_germany",
                    "tailors_alterations",
                    "tax_payments_government_agencies",
                    "tax_preparation_services",
                    "taxicabs_limousines",
                    "telecommunication_equipment_and_telephone_sales",
                    "telecommunication_services",
                    "telegraph_services",
                    "tent_and_awning_shops",
                    "testing_laboratories",
                    "theatrical_ticket_agencies",
                    "timeshares",
                    "tire_retreading_and_repair",
                    "tolls_bridge_fees",
                    "tourist_attractions_and_exhibits",
                    "towing_services",
                    "trailer_parks_campgrounds",
                    "transportation_services",
                    "travel_agencies_tour_operators",
                    "truck_stop_iteration",
                    "truck_utility_trailer_rentals",
                    "typesetting_plate_making_and_related_services",
                    "typewriter_stores",
                    "u_s_federal_government_agencies_or_departments",
                    "uniforms_commercial_clothing",
                    "used_merchandise_and_secondhand_stores",
                    "utilities",
                    "variety_stores",
                    "veterinary_services",
                    "video_amusement_game_supplies",
                    "video_game_arcades",
                    "video_tape_rental_stores",
                    "vocational_trade_schools",
                    "watch_jewelry_repair",
                    "welding_repair",
                    "wholesale_clubs",
                    "wig_and_toupee_stores",
                    "wires_money_orders",
                    "womens_accessory_and_specialty_shops",
                    "womens_ready_to_wear_stores",
                    "wrecking_and_salvage_yards",
                ]
            ]
        ]
        """
        Array of strings containing [categories](https://docs.stripe.com/api#issuing_authorization_object-merchant_data-category) of authorizations to decline. All other categories will be allowed. Cannot be set with `allowed_categories`.
        """
        blocked_merchant_countries: Optional[List[str]]
        """
        Array of strings containing representing countries from which authorizations will be declined. Country codes should be ISO 3166 alpha-2 country codes (e.g. `US`). Cannot be set with `allowed_merchant_countries`. Provide an empty value to unset this control.
        """
        spending_limits: Optional[List[SpendingLimit]]
        """
        Limit spending with amount-based rules that apply across any cards this card replaced (i.e., its `replacement_for` card and _that_ card's `replacement_for` card, up the chain).
        """
        spending_limits_currency: Optional[str]
        """
        Currency of the amounts within `spending_limits`. Always the same as the currency of the card.
        """
        _inner_class_types = {"spending_limits": SpendingLimit}

    class Wallets(StripeObject):
        class ApplePay(StripeObject):
            eligible: bool
            """
            Apple Pay Eligibility
            """
            ineligible_reason: Optional[
                Literal[
                    "missing_agreement",
                    "missing_cardholder_contact",
                    "unsupported_region",
                ]
            ]
            """
            Reason the card is ineligible for Apple Pay
            """

        class GooglePay(StripeObject):
            eligible: bool
            """
            Google Pay Eligibility
            """
            ineligible_reason: Optional[
                Literal[
                    "missing_agreement",
                    "missing_cardholder_contact",
                    "unsupported_region",
                ]
            ]
            """
            Reason the card is ineligible for Google Pay
            """

        apple_pay: ApplePay
        google_pay: GooglePay
        primary_account_identifier: Optional[str]
        """
        Unique identifier for a card used with digital wallets
        """
        _inner_class_types = {"apple_pay": ApplePay, "google_pay": GooglePay}

    brand: str
    """
    The brand of the card.
    """
    cancellation_reason: Optional[Literal["design_rejected", "lost", "stolen"]]
    """
    The reason why the card was canceled.
    """
    cardholder: "Cardholder"
    """
    An Issuing `Cardholder` object represents an individual or business entity who is [issued](https://docs.stripe.com/issuing) cards.

    Related guide: [How to create a cardholder](https://docs.stripe.com/issuing/cards/virtual/issue-cards#create-cardholder)
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Supported currencies are `usd` in the US, `eur` in the EU, and `gbp` in the UK.
    """
    cvc: Optional[str]
    """
    The card's CVC. For security reasons, this is only available for virtual cards, and will be omitted unless you explicitly request it with [the `expand` parameter](https://docs.stripe.com/api/expanding_objects). Additionally, it's only available via the ["Retrieve a card" endpoint](https://docs.stripe.com/api/issuing/cards/retrieve), not via "List all cards" or any other endpoint.
    """
    exp_month: int
    """
    The expiration month of the card.
    """
    exp_year: int
    """
    The expiration year of the card.
    """
    financial_account: Optional[str]
    """
    The financial account this card is attached to.
    """
    id: str
    """
    Unique identifier for the object.
    """
    last4: str
    """
    The last 4 digits of the card number.
    """
    latest_fraud_warning: Optional[LatestFraudWarning]
    """
    Stripe's assessment of whether this card's details have been compromised. If this property isn't null, cancel and reissue the card to prevent fraudulent activity risk.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    number: Optional[str]
    """
    The full unredacted card number. For security reasons, this is only available for virtual cards, and will be omitted unless you explicitly request it with [the `expand` parameter](https://docs.stripe.com/api/expanding_objects). Additionally, it's only available via the ["Retrieve a card" endpoint](https://docs.stripe.com/api/issuing/cards/retrieve), not via "List all cards" or any other endpoint.
    """
    object: Literal["issuing.card"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    personalization_design: Optional[ExpandableField["PersonalizationDesign"]]
    """
    The personalization design object belonging to this card.
    """
    replaced_by: Optional[ExpandableField["Card"]]
    """
    The latest card that replaces this card, if any.
    """
    replacement_for: Optional[ExpandableField["Card"]]
    """
    The card this card replaces, if any.
    """
    replacement_reason: Optional[
        Literal["damaged", "expired", "lost", "stolen"]
    ]
    """
    The reason why the previous card needed to be replaced.
    """
    second_line: Optional[str]
    """
    Text separate from cardholder name, printed on the card.
    """
    shipping: Optional[Shipping]
    """
    Where and how the card will be shipped.
    """
    spending_controls: SpendingControls
    status: Literal["active", "canceled", "inactive"]
    """
    Whether authorizations can be approved on this card. May be blocked from activating cards depending on past-due Cardholder requirements. Defaults to `inactive`.
    """
    type: Literal["physical", "virtual"]
    """
    The type of the card.
    """
    wallets: Optional[Wallets]
    """
    Information relating to digital wallets (like Apple Pay and Google Pay).
    """

    @classmethod
    def create(cls, **params: Unpack["CardCreateParams"]) -> "Card":
        """
        Creates an Issuing Card object.
        """
        return cast(
            "Card",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["CardCreateParams"]
    ) -> "Card":
        """
        Creates an Issuing Card object.
        """
        return cast(
            "Card",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(cls, **params: Unpack["CardListParams"]) -> ListObject["Card"]:
        """
        Returns a list of Issuing Card objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        result = cls._static_request(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    async def list_async(
        cls, **params: Unpack["CardListParams"]
    ) -> ListObject["Card"]:
        """
        Returns a list of Issuing Card objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        result = await cls._static_request_async(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    def modify(cls, id: str, **params: Unpack["CardModifyParams"]) -> "Card":
        """
        Updates the specified Issuing Card object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Card",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["CardModifyParams"]
    ) -> "Card":
        """
        Updates the specified Issuing Card object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Card",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["CardRetrieveParams"]
    ) -> "Card":
        """
        Retrieves an Issuing Card object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["CardRetrieveParams"]
    ) -> "Card":
        """
        Retrieves an Issuing Card object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    class TestHelpers(APIResourceTestHelpers["Card"]):
        _resource_cls: Type["Card"]

        @classmethod
        def _cls_deliver_card(
            cls, card: str, **params: Unpack["CardDeliverCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to delivered.
            """
            return cast(
                "Card",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/deliver".format(
                        card=sanitize_id(card)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def deliver_card(
            card: str, **params: Unpack["CardDeliverCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to delivered.
            """
            ...

        @overload
        def deliver_card(
            self, **params: Unpack["CardDeliverCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to delivered.
            """
            ...

        @class_method_variant("_cls_deliver_card")
        def deliver_card(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["CardDeliverCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to delivered.
            """
            return cast(
                "Card",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/deliver".format(
                        card=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_deliver_card_async(
            cls, card: str, **params: Unpack["CardDeliverCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to delivered.
            """
            return cast(
                "Card",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/deliver".format(
                        card=sanitize_id(card)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def deliver_card_async(
            card: str, **params: Unpack["CardDeliverCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to delivered.
            """
            ...

        @overload
        async def deliver_card_async(
            self, **params: Unpack["CardDeliverCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to delivered.
            """
            ...

        @class_method_variant("_cls_deliver_card_async")
        async def deliver_card_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["CardDeliverCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to delivered.
            """
            return cast(
                "Card",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/deliver".format(
                        card=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_fail_card(
            cls, card: str, **params: Unpack["CardFailCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to failure.
            """
            return cast(
                "Card",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/fail".format(
                        card=sanitize_id(card)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def fail_card(
            card: str, **params: Unpack["CardFailCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to failure.
            """
            ...

        @overload
        def fail_card(self, **params: Unpack["CardFailCardParams"]) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to failure.
            """
            ...

        @class_method_variant("_cls_fail_card")
        def fail_card(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["CardFailCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to failure.
            """
            return cast(
                "Card",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/fail".format(
                        card=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_fail_card_async(
            cls, card: str, **params: Unpack["CardFailCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to failure.
            """
            return cast(
                "Card",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/fail".format(
                        card=sanitize_id(card)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def fail_card_async(
            card: str, **params: Unpack["CardFailCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to failure.
            """
            ...

        @overload
        async def fail_card_async(
            self, **params: Unpack["CardFailCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to failure.
            """
            ...

        @class_method_variant("_cls_fail_card_async")
        async def fail_card_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["CardFailCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to failure.
            """
            return cast(
                "Card",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/fail".format(
                        card=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_return_card(
            cls, card: str, **params: Unpack["CardReturnCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to returned.
            """
            return cast(
                "Card",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/return".format(
                        card=sanitize_id(card)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def return_card(
            card: str, **params: Unpack["CardReturnCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to returned.
            """
            ...

        @overload
        def return_card(
            self, **params: Unpack["CardReturnCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to returned.
            """
            ...

        @class_method_variant("_cls_return_card")
        def return_card(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["CardReturnCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to returned.
            """
            return cast(
                "Card",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/return".format(
                        card=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_return_card_async(
            cls, card: str, **params: Unpack["CardReturnCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to returned.
            """
            return cast(
                "Card",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/return".format(
                        card=sanitize_id(card)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def return_card_async(
            card: str, **params: Unpack["CardReturnCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to returned.
            """
            ...

        @overload
        async def return_card_async(
            self, **params: Unpack["CardReturnCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to returned.
            """
            ...

        @class_method_variant("_cls_return_card_async")
        async def return_card_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["CardReturnCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to returned.
            """
            return cast(
                "Card",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/return".format(
                        card=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_ship_card(
            cls, card: str, **params: Unpack["CardShipCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to shipped.
            """
            return cast(
                "Card",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/ship".format(
                        card=sanitize_id(card)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def ship_card(
            card: str, **params: Unpack["CardShipCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to shipped.
            """
            ...

        @overload
        def ship_card(self, **params: Unpack["CardShipCardParams"]) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to shipped.
            """
            ...

        @class_method_variant("_cls_ship_card")
        def ship_card(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["CardShipCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to shipped.
            """
            return cast(
                "Card",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/ship".format(
                        card=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_ship_card_async(
            cls, card: str, **params: Unpack["CardShipCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to shipped.
            """
            return cast(
                "Card",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/ship".format(
                        card=sanitize_id(card)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def ship_card_async(
            card: str, **params: Unpack["CardShipCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to shipped.
            """
            ...

        @overload
        async def ship_card_async(
            self, **params: Unpack["CardShipCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to shipped.
            """
            ...

        @class_method_variant("_cls_ship_card_async")
        async def ship_card_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["CardShipCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to shipped.
            """
            return cast(
                "Card",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/ship".format(
                        card=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_submit_card(
            cls, card: str, **params: Unpack["CardSubmitCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to submitted. This method requires Stripe Version ‘2024-09-30.acacia' or later.
            """
            return cast(
                "Card",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/submit".format(
                        card=sanitize_id(card)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def submit_card(
            card: str, **params: Unpack["CardSubmitCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to submitted. This method requires Stripe Version ‘2024-09-30.acacia' or later.
            """
            ...

        @overload
        def submit_card(
            self, **params: Unpack["CardSubmitCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to submitted. This method requires Stripe Version ‘2024-09-30.acacia' or later.
            """
            ...

        @class_method_variant("_cls_submit_card")
        def submit_card(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["CardSubmitCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to submitted. This method requires Stripe Version ‘2024-09-30.acacia' or later.
            """
            return cast(
                "Card",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/submit".format(
                        card=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_submit_card_async(
            cls, card: str, **params: Unpack["CardSubmitCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to submitted. This method requires Stripe Version ‘2024-09-30.acacia' or later.
            """
            return cast(
                "Card",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/submit".format(
                        card=sanitize_id(card)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def submit_card_async(
            card: str, **params: Unpack["CardSubmitCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to submitted. This method requires Stripe Version ‘2024-09-30.acacia' or later.
            """
            ...

        @overload
        async def submit_card_async(
            self, **params: Unpack["CardSubmitCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to submitted. This method requires Stripe Version ‘2024-09-30.acacia' or later.
            """
            ...

        @class_method_variant("_cls_submit_card_async")
        async def submit_card_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["CardSubmitCardParams"]
        ) -> "Card":
            """
            Updates the shipping status of the specified Issuing Card object to submitted. This method requires Stripe Version ‘2024-09-30.acacia' or later.
            """
            return cast(
                "Card",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/issuing/cards/{card}/shipping/submit".format(
                        card=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

    @property
    def test_helpers(self):
        return self.TestHelpers(self)

    _inner_class_types = {
        "latest_fraud_warning": LatestFraudWarning,
        "shipping": Shipping,
        "spending_controls": SpendingControls,
        "wallets": Wallets,
    }


Card.TestHelpers._resource_cls = Card
