# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar, Dict, Optional
from typing_extensions import Literal


class TransactionLineItem(StripeObject):
    OBJECT_NAME: ClassVar[Literal["tax.transaction_line_item"]] = (
        "tax.transaction_line_item"
    )

    class Reversal(StripeObject):
        original_line_item: str
        """
        The `id` of the line item to reverse in the original transaction.
        """

    amount: int
    """
    The line item amount in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal). If `tax_behavior=inclusive`, then this amount includes taxes. Otherwise, taxes were calculated on top of this amount.
    """
    amount_tax: int
    """
    The amount of tax calculated for this line item, in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["tax.transaction_line_item"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    product: Optional[str]
    """
    The ID of an existing [Product](https://docs.stripe.com/api/products/object).
    """
    quantity: int
    """
    The number of units of the item being purchased. For reversals, this is the quantity reversed.
    """
    reference: str
    """
    A custom identifier for this line item in the transaction.
    """
    reversal: Optional[Reversal]
    """
    If `type=reversal`, contains information about what was reversed.
    """
    tax_behavior: Literal["exclusive", "inclusive"]
    """
    Specifies whether the `amount` includes taxes. If `tax_behavior=inclusive`, then the amount includes taxes.
    """
    tax_code: str
    """
    The [tax code](https://docs.stripe.com/tax/tax-categories) ID used for this resource.
    """
    type: Literal["reversal", "transaction"]
    """
    If `reversal`, this line item reverses an earlier transaction.
    """
    _inner_class_types = {"reversal": Reversal}
