# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar, Optional
from typing_extensions import Literal


class PaymentIntentAmountDetailsLineItem(StripeObject):
    OBJECT_NAME: ClassVar[
        Literal["payment_intent_amount_details_line_item"]
    ] = "payment_intent_amount_details_line_item"

    class PaymentMethodOptions(StripeObject):
        class Card(StripeObject):
            commodity_code: Optional[str]

        class CardPresent(StripeObject):
            commodity_code: Optional[str]

        class Klarna(StripeObject):
            image_url: Optional[str]
            product_url: Optional[str]
            reference: Optional[str]
            subscription_reference: Optional[str]

        class Paypal(StripeObject):
            category: Optional[
                Literal["digital_goods", "donation", "physical_goods"]
            ]
            """
            Type of the line item.
            """
            description: Optional[str]
            """
            Description of the line item.
            """
            sold_by: Optional[str]
            """
            The Stripe account ID of the connected account that sells the item. This is only needed when using [Separate Charges and Transfers](https://docs.stripe.com/connect/separate-charges-and-transfers).
            """

        card: Optional[Card]
        card_present: Optional[CardPresent]
        klarna: Optional[Klarna]
        paypal: Optional[Paypal]
        _inner_class_types = {
            "card": Card,
            "card_present": CardPresent,
            "klarna": Klarna,
            "paypal": Paypal,
        }

    class Tax(StripeObject):
        total_tax_amount: int
        """
        The total amount of tax on the transaction represented in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal). Required for L2 rates. An integer greater than or equal to 0.

        This field is mutually exclusive with the `amount_details[line_items][#][tax][total_tax_amount]` field.
        """

    discount_amount: Optional[int]
    """
    The discount applied on this line item represented in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal). An integer greater than 0.

    This field is mutually exclusive with the `amount_details[discount_amount]` field.
    """
    id: str
    """
    Unique identifier for the object.
    """
    object: Literal["payment_intent_amount_details_line_item"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    payment_method_options: Optional[PaymentMethodOptions]
    """
    Payment method-specific information for line items.
    """
    product_code: Optional[str]
    """
    The product code of the line item, such as an SKU. Required for L3 rates. At most 12 characters long.
    """
    product_name: str
    """
    The product name of the line item. Required for L3 rates. At most 1024 characters long.

    For Cards, this field is truncated to 26 alphanumeric characters before being sent to the card networks. For PayPal, this field is truncated to 127 characters.
    """
    quantity: int
    """
    The quantity of items. Required for L3 rates. An integer greater than 0.
    """
    tax: Optional[Tax]
    """
    Contains information about the tax on the item.
    """
    unit_cost: int
    """
    The unit cost of the line item represented in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal). Required for L3 rates. An integer greater than or equal to 0.
    """
    unit_of_measure: Optional[str]
    """
    A unit of measure for the line item, such as gallons, feet, meters, etc. Required for L3 rates. At most 12 alphanumeric characters long.
    """
    _inner_class_types = {
        "payment_method_options": PaymentMethodOptions,
        "tax": Tax,
    }
