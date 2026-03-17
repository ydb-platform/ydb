# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class TransactionCreateUnlinkedRefundParams(TypedDict):
    amount: int
    """
    The total amount to attempt to refund. This amount is in the provided currency, or defaults to the cards currency, and in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    card: str
    """
    Card associated with this unlinked refund transaction.
    """
    currency: NotRequired[str]
    """
    The currency of the unlinked refund. If not provided, defaults to the currency of the card. Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    merchant_data: NotRequired[
        "TransactionCreateUnlinkedRefundParamsMerchantData"
    ]
    """
    Details about the seller (grocery store, e-commerce website, etc.) where the card authorization happened.
    """
    purchase_details: NotRequired[
        "TransactionCreateUnlinkedRefundParamsPurchaseDetails"
    ]
    """
    Additional purchase information that is optionally provided by the merchant.
    """


class TransactionCreateUnlinkedRefundParamsMerchantData(TypedDict):
    category: NotRequired[
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
    """
    A categorization of the seller's type of business. See our [merchant categories guide](https://docs.stripe.com/issuing/merchant-categories) for a list of possible values.
    """
    city: NotRequired[str]
    """
    City where the seller is located
    """
    country: NotRequired[str]
    """
    Country where the seller is located
    """
    name: NotRequired[str]
    """
    Name of the seller
    """
    network_id: NotRequired[str]
    """
    Identifier assigned to the seller by the card network. Different card networks may assign different network_id fields to the same merchant.
    """
    postal_code: NotRequired[str]
    """
    Postal code where the seller is located
    """
    state: NotRequired[str]
    """
    State where the seller is located
    """
    terminal_id: NotRequired[str]
    """
    An ID assigned by the seller to the location of the sale.
    """
    url: NotRequired[str]
    """
    URL provided by the merchant on a 3DS request
    """


class TransactionCreateUnlinkedRefundParamsPurchaseDetails(TypedDict):
    fleet: NotRequired[
        "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleet"
    ]
    """
    Fleet-specific information for transactions using Fleet cards.
    """
    flight: NotRequired[
        "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFlight"
    ]
    """
    Information about the flight that was purchased with this transaction.
    """
    fuel: NotRequired[
        "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFuel"
    ]
    """
    Information about fuel that was purchased with this transaction.
    """
    lodging: NotRequired[
        "TransactionCreateUnlinkedRefundParamsPurchaseDetailsLodging"
    ]
    """
    Information about lodging that was purchased with this transaction.
    """
    receipt: NotRequired[
        List["TransactionCreateUnlinkedRefundParamsPurchaseDetailsReceipt"]
    ]
    """
    The line items in the purchase.
    """
    reference: NotRequired[str]
    """
    A merchant-specific order number.
    """


class TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleet(TypedDict):
    cardholder_prompt_data: NotRequired[
        "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetCardholderPromptData"
    ]
    """
    Answers to prompts presented to the cardholder at the point of sale. Prompted fields vary depending on the configuration of your physical fleet cards. Typical points of sale support only numeric entry.
    """
    purchase_type: NotRequired[
        Literal[
            "fuel_and_non_fuel_purchase", "fuel_purchase", "non_fuel_purchase"
        ]
    ]
    """
    The type of purchase. One of `fuel_purchase`, `non_fuel_purchase`, or `fuel_and_non_fuel_purchase`.
    """
    reported_breakdown: NotRequired[
        "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdown"
    ]
    """
    More information about the total amount. This information is not guaranteed to be accurate as some merchants may provide unreliable data.
    """
    service_type: NotRequired[
        Literal["full_service", "non_fuel_transaction", "self_service"]
    ]
    """
    The type of fuel service. One of `non_fuel_transaction`, `full_service`, or `self_service`.
    """


class TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetCardholderPromptData(
    TypedDict,
):
    driver_id: NotRequired[str]
    """
    Driver ID.
    """
    odometer: NotRequired[int]
    """
    Odometer reading.
    """
    unspecified_id: NotRequired[str]
    """
    An alphanumeric ID. This field is used when a vehicle ID, driver ID, or generic ID is entered by the cardholder, but the merchant or card network did not specify the prompt type.
    """
    user_id: NotRequired[str]
    """
    User ID.
    """
    vehicle_number: NotRequired[str]
    """
    Vehicle number.
    """


class TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdown(
    TypedDict,
):
    fuel: NotRequired[
        "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownFuel"
    ]
    """
    Breakdown of fuel portion of the purchase.
    """
    non_fuel: NotRequired[
        "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownNonFuel"
    ]
    """
    Breakdown of non-fuel portion of the purchase.
    """
    tax: NotRequired[
        "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownTax"
    ]
    """
    Information about tax included in this transaction.
    """


class TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownFuel(
    TypedDict,
):
    gross_amount_decimal: NotRequired[str]
    """
    Gross fuel amount that should equal Fuel Volume multipled by Fuel Unit Cost, inclusive of taxes.
    """


class TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownNonFuel(
    TypedDict,
):
    gross_amount_decimal: NotRequired[str]
    """
    Gross non-fuel amount that should equal the sum of the line items, inclusive of taxes.
    """


class TransactionCreateUnlinkedRefundParamsPurchaseDetailsFleetReportedBreakdownTax(
    TypedDict,
):
    local_amount_decimal: NotRequired[str]
    """
    Amount of state or provincial Sales Tax included in the transaction amount. Null if not reported by merchant or not subject to tax.
    """
    national_amount_decimal: NotRequired[str]
    """
    Amount of national Sales Tax or VAT included in the transaction amount. Null if not reported by merchant or not subject to tax.
    """


class TransactionCreateUnlinkedRefundParamsPurchaseDetailsFlight(TypedDict):
    departure_at: NotRequired[int]
    """
    The time that the flight departed.
    """
    passenger_name: NotRequired[str]
    """
    The name of the passenger.
    """
    refundable: NotRequired[bool]
    """
    Whether the ticket is refundable.
    """
    segments: NotRequired[
        List[
            "TransactionCreateUnlinkedRefundParamsPurchaseDetailsFlightSegment"
        ]
    ]
    """
    The legs of the trip.
    """
    travel_agency: NotRequired[str]
    """
    The travel agency that issued the ticket.
    """


class TransactionCreateUnlinkedRefundParamsPurchaseDetailsFlightSegment(
    TypedDict,
):
    arrival_airport_code: NotRequired[str]
    """
    The three-letter IATA airport code of the flight's destination.
    """
    carrier: NotRequired[str]
    """
    The airline carrier code.
    """
    departure_airport_code: NotRequired[str]
    """
    The three-letter IATA airport code that the flight departed from.
    """
    flight_number: NotRequired[str]
    """
    The flight number.
    """
    service_class: NotRequired[str]
    """
    The flight's service class.
    """
    stopover_allowed: NotRequired[bool]
    """
    Whether a stopover is allowed on this flight.
    """


class TransactionCreateUnlinkedRefundParamsPurchaseDetailsFuel(TypedDict):
    industry_product_code: NotRequired[str]
    """
    [Conexxus Payment System Product Code](https://www.conexxus.org/conexxus-payment-system-product-codes) identifying the primary fuel product purchased.
    """
    quantity_decimal: NotRequired[str]
    """
    The quantity of `unit`s of fuel that was dispensed, represented as a decimal string with at most 12 decimal places.
    """
    type: NotRequired[
        Literal[
            "diesel",
            "other",
            "unleaded_plus",
            "unleaded_regular",
            "unleaded_super",
        ]
    ]
    """
    The type of fuel that was purchased. One of `diesel`, `unleaded_plus`, `unleaded_regular`, `unleaded_super`, or `other`.
    """
    unit: NotRequired[
        Literal[
            "charging_minute",
            "imperial_gallon",
            "kilogram",
            "kilowatt_hour",
            "liter",
            "other",
            "pound",
            "us_gallon",
        ]
    ]
    """
    The units for `quantity_decimal`. One of `charging_minute`, `imperial_gallon`, `kilogram`, `kilowatt_hour`, `liter`, `pound`, `us_gallon`, or `other`.
    """
    unit_cost_decimal: NotRequired[str]
    """
    The cost in cents per each unit of fuel, represented as a decimal string with at most 12 decimal places.
    """


class TransactionCreateUnlinkedRefundParamsPurchaseDetailsLodging(TypedDict):
    check_in_at: NotRequired[int]
    """
    The time of checking into the lodging.
    """
    nights: NotRequired[int]
    """
    The number of nights stayed at the lodging.
    """


class TransactionCreateUnlinkedRefundParamsPurchaseDetailsReceipt(TypedDict):
    description: NotRequired[str]
    quantity: NotRequired[str]
    total: NotRequired[int]
    unit_cost: NotRequired[int]
