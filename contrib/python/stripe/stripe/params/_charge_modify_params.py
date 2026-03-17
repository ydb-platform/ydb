# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List, Union
from typing_extensions import Literal, NotRequired, TypedDict


class ChargeModifyParams(RequestOptions):
    customer: NotRequired[str]
    """
    The ID of an existing customer that will be associated with this request. This field may only be updated if there is no existing associated customer with this charge.
    """
    description: NotRequired[str]
    """
    An arbitrary string which you can attach to a charge object. It is displayed when in the web interface alongside the charge. Note that if you use Stripe to send automatic email receipts to your customers, your receipt emails will include the `description` of the charge(s) that they are describing.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    fraud_details: NotRequired["ChargeModifyParamsFraudDetails"]
    """
    A set of key-value pairs you can attach to a charge giving information about its riskiness. If you believe a charge is fraudulent, include a `user_report` key with a value of `fraudulent`. If you believe a charge is safe, include a `user_report` key with a value of `safe`. Stripe will use the information you send to improve our fraud detection algorithms.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    receipt_email: NotRequired[str]
    """
    This is the email address that the receipt for this charge will be sent to. If this field is updated, then a new email receipt will be sent to the updated address.
    """
    shipping: NotRequired["ChargeModifyParamsShipping"]
    """
    Shipping information for the charge. Helps prevent fraud on charges for physical goods.
    """
    transfer_group: NotRequired[str]
    """
    A string that identifies this transaction as part of a group. `transfer_group` may only be provided if it has not been set. See the [Connect documentation](https://docs.stripe.com/connect/separate-charges-and-transfers#transfer-options) for details.
    """


class ChargeModifyParamsFraudDetails(TypedDict):
    user_report: Union[Literal[""], Literal["fraudulent", "safe"]]
    """
    Either `safe` or `fraudulent`.
    """


class ChargeModifyParamsShipping(TypedDict):
    address: "ChargeModifyParamsShippingAddress"
    """
    Shipping address.
    """
    carrier: NotRequired[str]
    """
    The delivery service that shipped a physical product, such as Fedex, UPS, USPS, etc.
    """
    name: str
    """
    Recipient name.
    """
    phone: NotRequired[str]
    """
    Recipient phone (including extension).
    """
    tracking_number: NotRequired[str]
    """
    The tracking number for a physical product, obtained from the delivery service. If multiple tracking numbers were generated for this purchase, please separate them with commas.
    """


class ChargeModifyParamsShippingAddress(TypedDict):
    city: NotRequired[str]
    """
    City, district, suburb, town, or village.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
    """
    Address line 1, such as the street, PO Box, or company name.
    """
    line2: NotRequired[str]
    """
    Address line 2, such as the apartment, suite, unit, or building.
    """
    postal_code: NotRequired[str]
    """
    ZIP or postal code.
    """
    state: NotRequired[str]
    """
    State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
    """
