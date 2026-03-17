# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class SourceUpdateParams(TypedDict):
    amount: NotRequired[int]
    """
    Amount associated with the source.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    mandate: NotRequired["SourceUpdateParamsMandate"]
    """
    Information about a mandate possibility attached to a source object (generally for bank debits) as well as its acceptance status.
    """
    metadata: NotRequired["Literal['']|Dict[str, str]"]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    owner: NotRequired["SourceUpdateParamsOwner"]
    """
    Information about the owner of the payment instrument that may be used or required by particular source types.
    """
    source_order: NotRequired["SourceUpdateParamsSourceOrder"]
    """
    Information about the items and shipping associated with the source. Required for transactional credit (for example Klarna) sources before you can charge it.
    """


class SourceUpdateParamsMandate(TypedDict):
    acceptance: NotRequired["SourceUpdateParamsMandateAcceptance"]
    """
    The parameters required to notify Stripe of a mandate acceptance or refusal by the customer.
    """
    amount: NotRequired["Literal['']|int"]
    """
    The amount specified by the mandate. (Leave null for a mandate covering all amounts)
    """
    currency: NotRequired[str]
    """
    The currency specified by the mandate. (Must match `currency` of the source)
    """
    interval: NotRequired[Literal["one_time", "scheduled", "variable"]]
    """
    The interval of debits permitted by the mandate. Either `one_time` (just permitting a single debit), `scheduled` (with debits on an agreed schedule or for clearly-defined events), or `variable`(for debits with any frequency)
    """
    notification_method: NotRequired[
        Literal["deprecated_none", "email", "manual", "none", "stripe_email"]
    ]
    """
    The method Stripe should use to notify the customer of upcoming debit instructions and/or mandate confirmation as required by the underlying debit network. Either `email` (an email is sent directly to the customer), `manual` (a `source.mandate_notification` event is sent to your webhooks endpoint and you should handle the notification) or `none` (the underlying debit network does not require any notification).
    """


class SourceUpdateParamsMandateAcceptance(TypedDict):
    date: NotRequired[int]
    """
    The Unix timestamp (in seconds) when the mandate was accepted or refused by the customer.
    """
    ip: NotRequired[str]
    """
    The IP address from which the mandate was accepted or refused by the customer.
    """
    offline: NotRequired["SourceUpdateParamsMandateAcceptanceOffline"]
    """
    The parameters required to store a mandate accepted offline. Should only be set if `mandate[type]` is `offline`
    """
    online: NotRequired["SourceUpdateParamsMandateAcceptanceOnline"]
    """
    The parameters required to store a mandate accepted online. Should only be set if `mandate[type]` is `online`
    """
    status: Literal["accepted", "pending", "refused", "revoked"]
    """
    The status of the mandate acceptance. Either `accepted` (the mandate was accepted) or `refused` (the mandate was refused).
    """
    type: NotRequired[Literal["offline", "online"]]
    """
    The type of acceptance information included with the mandate. Either `online` or `offline`
    """
    user_agent: NotRequired[str]
    """
    The user agent of the browser from which the mandate was accepted or refused by the customer.
    """


class SourceUpdateParamsMandateAcceptanceOffline(TypedDict):
    contact_email: str
    """
    An email to contact you with if a copy of the mandate is requested, required if `type` is `offline`.
    """


class SourceUpdateParamsMandateAcceptanceOnline(TypedDict):
    date: NotRequired[int]
    """
    The Unix timestamp (in seconds) when the mandate was accepted or refused by the customer.
    """
    ip: NotRequired[str]
    """
    The IP address from which the mandate was accepted or refused by the customer.
    """
    user_agent: NotRequired[str]
    """
    The user agent of the browser from which the mandate was accepted or refused by the customer.
    """


class SourceUpdateParamsOwner(TypedDict):
    address: NotRequired["SourceUpdateParamsOwnerAddress"]
    """
    Owner's address.
    """
    email: NotRequired[str]
    """
    Owner's email address.
    """
    name: NotRequired[str]
    """
    Owner's full name.
    """
    phone: NotRequired[str]
    """
    Owner's phone number.
    """


class SourceUpdateParamsOwnerAddress(TypedDict):
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


class SourceUpdateParamsSourceOrder(TypedDict):
    items: NotRequired[List["SourceUpdateParamsSourceOrderItem"]]
    """
    List of items constituting the order.
    """
    shipping: NotRequired["SourceUpdateParamsSourceOrderShipping"]
    """
    Shipping address for the order. Required if any of the SKUs are for products that have `shippable` set to true.
    """


class SourceUpdateParamsSourceOrderItem(TypedDict):
    amount: NotRequired[int]
    currency: NotRequired[str]
    description: NotRequired[str]
    parent: NotRequired[str]
    """
    The ID of the SKU being ordered.
    """
    quantity: NotRequired[int]
    """
    The quantity of this order item. When type is `sku`, this is the number of instances of the SKU to be ordered.
    """
    type: NotRequired[Literal["discount", "shipping", "sku", "tax"]]


class SourceUpdateParamsSourceOrderShipping(TypedDict):
    address: "SourceUpdateParamsSourceOrderShippingAddress"
    """
    Shipping address.
    """
    carrier: NotRequired[str]
    """
    The delivery service that shipped a physical product, such as Fedex, UPS, USPS, etc.
    """
    name: NotRequired[str]
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


class SourceUpdateParamsSourceOrderShippingAddress(TypedDict):
    city: NotRequired[str]
    """
    City, district, suburb, town, or village.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: str
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
