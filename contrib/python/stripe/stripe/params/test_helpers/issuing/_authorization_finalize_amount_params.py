# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class AuthorizationFinalizeAmountParams(TypedDict):
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    final_amount: int
    """
    The final authorization amount that will be captured by the merchant. This amount is in the authorization currency and in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    fleet: NotRequired["AuthorizationFinalizeAmountParamsFleet"]
    """
    Fleet-specific information for authorizations using Fleet cards.
    """
    fuel: NotRequired["AuthorizationFinalizeAmountParamsFuel"]
    """
    Information about fuel that was purchased with this transaction.
    """


class AuthorizationFinalizeAmountParamsFleet(TypedDict):
    cardholder_prompt_data: NotRequired[
        "AuthorizationFinalizeAmountParamsFleetCardholderPromptData"
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
        "AuthorizationFinalizeAmountParamsFleetReportedBreakdown"
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


class AuthorizationFinalizeAmountParamsFleetCardholderPromptData(TypedDict):
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


class AuthorizationFinalizeAmountParamsFleetReportedBreakdown(TypedDict):
    fuel: NotRequired[
        "AuthorizationFinalizeAmountParamsFleetReportedBreakdownFuel"
    ]
    """
    Breakdown of fuel portion of the purchase.
    """
    non_fuel: NotRequired[
        "AuthorizationFinalizeAmountParamsFleetReportedBreakdownNonFuel"
    ]
    """
    Breakdown of non-fuel portion of the purchase.
    """
    tax: NotRequired[
        "AuthorizationFinalizeAmountParamsFleetReportedBreakdownTax"
    ]
    """
    Information about tax included in this transaction.
    """


class AuthorizationFinalizeAmountParamsFleetReportedBreakdownFuel(TypedDict):
    gross_amount_decimal: NotRequired[str]
    """
    Gross fuel amount that should equal Fuel Volume multipled by Fuel Unit Cost, inclusive of taxes.
    """


class AuthorizationFinalizeAmountParamsFleetReportedBreakdownNonFuel(
    TypedDict
):
    gross_amount_decimal: NotRequired[str]
    """
    Gross non-fuel amount that should equal the sum of the line items, inclusive of taxes.
    """


class AuthorizationFinalizeAmountParamsFleetReportedBreakdownTax(TypedDict):
    local_amount_decimal: NotRequired[str]
    """
    Amount of state or provincial Sales Tax included in the transaction amount. Null if not reported by merchant or not subject to tax.
    """
    national_amount_decimal: NotRequired[str]
    """
    Amount of national Sales Tax or VAT included in the transaction amount. Null if not reported by merchant or not subject to tax.
    """


class AuthorizationFinalizeAmountParamsFuel(TypedDict):
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
