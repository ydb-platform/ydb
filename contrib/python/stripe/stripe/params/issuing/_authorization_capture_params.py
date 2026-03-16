# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class AuthorizationCaptureParams(RequestOptions):
    capture_amount: NotRequired[int]
    """
    The amount to capture from the authorization. If not provided, the full amount of the authorization will be captured. This amount is in the authorization currency and in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    close_authorization: NotRequired[bool]
    """
    Whether to close the authorization after capture. Defaults to true. Set to false to enable multi-capture flows.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    purchase_details: NotRequired["AuthorizationCaptureParamsPurchaseDetails"]
    """
    Additional purchase information that is optionally provided by the merchant.
    """


class AuthorizationCaptureParamsPurchaseDetails(TypedDict):
    fleet: NotRequired["AuthorizationCaptureParamsPurchaseDetailsFleet"]
    """
    Fleet-specific information for transactions using Fleet cards.
    """
    flight: NotRequired["AuthorizationCaptureParamsPurchaseDetailsFlight"]
    """
    Information about the flight that was purchased with this transaction.
    """
    fuel: NotRequired["AuthorizationCaptureParamsPurchaseDetailsFuel"]
    """
    Information about fuel that was purchased with this transaction.
    """
    lodging: NotRequired["AuthorizationCaptureParamsPurchaseDetailsLodging"]
    """
    Information about lodging that was purchased with this transaction.
    """
    receipt: NotRequired[
        List["AuthorizationCaptureParamsPurchaseDetailsReceipt"]
    ]
    """
    The line items in the purchase.
    """
    reference: NotRequired[str]
    """
    A merchant-specific order number.
    """


class AuthorizationCaptureParamsPurchaseDetailsFleet(TypedDict):
    cardholder_prompt_data: NotRequired[
        "AuthorizationCaptureParamsPurchaseDetailsFleetCardholderPromptData"
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
        "AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdown"
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


class AuthorizationCaptureParamsPurchaseDetailsFleetCardholderPromptData(
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


class AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdown(
    TypedDict,
):
    fuel: NotRequired[
        "AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownFuel"
    ]
    """
    Breakdown of fuel portion of the purchase.
    """
    non_fuel: NotRequired[
        "AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownNonFuel"
    ]
    """
    Breakdown of non-fuel portion of the purchase.
    """
    tax: NotRequired[
        "AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownTax"
    ]
    """
    Information about tax included in this transaction.
    """


class AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownFuel(
    TypedDict,
):
    gross_amount_decimal: NotRequired[str]
    """
    Gross fuel amount that should equal Fuel Volume multipled by Fuel Unit Cost, inclusive of taxes.
    """


class AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownNonFuel(
    TypedDict,
):
    gross_amount_decimal: NotRequired[str]
    """
    Gross non-fuel amount that should equal the sum of the line items, inclusive of taxes.
    """


class AuthorizationCaptureParamsPurchaseDetailsFleetReportedBreakdownTax(
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


class AuthorizationCaptureParamsPurchaseDetailsFlight(TypedDict):
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
        List["AuthorizationCaptureParamsPurchaseDetailsFlightSegment"]
    ]
    """
    The legs of the trip.
    """
    travel_agency: NotRequired[str]
    """
    The travel agency that issued the ticket.
    """


class AuthorizationCaptureParamsPurchaseDetailsFlightSegment(TypedDict):
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


class AuthorizationCaptureParamsPurchaseDetailsFuel(TypedDict):
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


class AuthorizationCaptureParamsPurchaseDetailsLodging(TypedDict):
    check_in_at: NotRequired[int]
    """
    The time of checking into the lodging.
    """
    nights: NotRequired[int]
    """
    The number of nights stayed at the lodging.
    """


class AuthorizationCaptureParamsPurchaseDetailsReceipt(TypedDict):
    description: NotRequired[str]
    quantity: NotRequired[str]
    total: NotRequired[int]
    unit_cost: NotRequired[int]
