# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class SourceCreateParams(RequestOptions):
    amount: NotRequired[int]
    """
    Amount associated with the source. This is the amount for which the source will be chargeable once ready. Required for `single_use` sources. Not supported for `receiver` type sources, where charge amount may not be specified until funds land.
    """
    currency: NotRequired[str]
    """
    Three-letter [ISO code for the currency](https://stripe.com/docs/currencies) associated with the source. This is the currency for which the source will be chargeable once ready.
    """
    customer: NotRequired[str]
    """
    The `Customer` to whom the original source is attached to. Must be set when the original source is not a `Source` (e.g., `Card`).
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    flow: NotRequired[
        Literal["code_verification", "none", "receiver", "redirect"]
    ]
    """
    The authentication `flow` of the source to create. `flow` is one of `redirect`, `receiver`, `code_verification`, `none`. It is generally inferred unless a type supports multiple flows.
    """
    mandate: NotRequired["SourceCreateParamsMandate"]
    """
    Information about a mandate possibility attached to a source object (generally for bank debits) as well as its acceptance status.
    """
    metadata: NotRequired[Dict[str, str]]
    original_source: NotRequired[str]
    """
    The source to share.
    """
    owner: NotRequired["SourceCreateParamsOwner"]
    """
    Information about the owner of the payment instrument that may be used or required by particular source types.
    """
    receiver: NotRequired["SourceCreateParamsReceiver"]
    """
    Optional parameters for the receiver flow. Can be set only if the source is a receiver (`flow` is `receiver`).
    """
    redirect: NotRequired["SourceCreateParamsRedirect"]
    """
    Parameters required for the redirect flow. Required if the source is authenticated by a redirect (`flow` is `redirect`).
    """
    source_order: NotRequired["SourceCreateParamsSourceOrder"]
    """
    Information about the items and shipping associated with the source. Required for transactional credit (for example Klarna) sources before you can charge it.
    """
    statement_descriptor: NotRequired[str]
    """
    An arbitrary string to be displayed on your customer's statement. As an example, if your website is `RunClub` and the item you're charging for is a race ticket, you may want to specify a `statement_descriptor` of `RunClub 5K race ticket.` While many payment types will display this information, some may not display it at all.
    """
    token: NotRequired[str]
    """
    An optional token used to create the source. When passed, token properties will override source parameters.
    """
    type: NotRequired[str]
    """
    The `type` of the source to create. Required unless `customer` and `original_source` are specified (see the [Cloning card Sources](https://docs.stripe.com/sources/connect#cloning-card-sources) guide)
    """
    usage: NotRequired[Literal["reusable", "single_use"]]


class SourceCreateParamsMandate(TypedDict):
    acceptance: NotRequired["SourceCreateParamsMandateAcceptance"]
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


class SourceCreateParamsMandateAcceptance(TypedDict):
    date: NotRequired[int]
    """
    The Unix timestamp (in seconds) when the mandate was accepted or refused by the customer.
    """
    ip: NotRequired[str]
    """
    The IP address from which the mandate was accepted or refused by the customer.
    """
    offline: NotRequired["SourceCreateParamsMandateAcceptanceOffline"]
    """
    The parameters required to store a mandate accepted offline. Should only be set if `mandate[type]` is `offline`
    """
    online: NotRequired["SourceCreateParamsMandateAcceptanceOnline"]
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


class SourceCreateParamsMandateAcceptanceOffline(TypedDict):
    contact_email: str
    """
    An email to contact you with if a copy of the mandate is requested, required if `type` is `offline`.
    """


class SourceCreateParamsMandateAcceptanceOnline(TypedDict):
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


class SourceCreateParamsOwner(TypedDict):
    address: NotRequired["SourceCreateParamsOwnerAddress"]
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


class SourceCreateParamsOwnerAddress(TypedDict):
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


class SourceCreateParamsReceiver(TypedDict):
    refund_attributes_method: NotRequired[Literal["email", "manual", "none"]]
    """
    The method Stripe should use to request information needed to process a refund or mispayment. Either `email` (an email is sent directly to the customer) or `manual` (a `source.refund_attributes_required` event is sent to your webhooks endpoint). Refer to each payment method's documentation to learn which refund attributes may be required.
    """


class SourceCreateParamsRedirect(TypedDict):
    return_url: str
    """
    The URL you provide to redirect the customer back to you after they authenticated their payment. It can use your application URI scheme in the context of a mobile application.
    """


class SourceCreateParamsSourceOrder(TypedDict):
    items: NotRequired[List["SourceCreateParamsSourceOrderItem"]]
    """
    List of items constituting the order.
    """
    shipping: NotRequired["SourceCreateParamsSourceOrderShipping"]
    """
    Shipping address for the order. Required if any of the SKUs are for products that have `shippable` set to true.
    """


class SourceCreateParamsSourceOrderItem(TypedDict):
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


class SourceCreateParamsSourceOrderShipping(TypedDict):
    address: "SourceCreateParamsSourceOrderShippingAddress"
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


class SourceCreateParamsSourceOrderShippingAddress(TypedDict):
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
