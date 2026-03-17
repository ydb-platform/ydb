# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._expandable_field import ExpandableField
from stripe._transfer import Transfer
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import sanitize_id
from typing import ClassVar, Dict, Optional
from typing_extensions import Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._balance_transaction import BalanceTransaction
    from stripe._refund import Refund


class Reversal(UpdateableAPIResource["Reversal"]):
    """
    [Stripe Connect](https://docs.stripe.com/connect) platforms can reverse transfers made to a
    connected account, either entirely or partially, and can also specify whether
    to refund any related application fees. Transfer reversals add to the
    platform's balance and subtract from the destination account's balance.

    Reversing a transfer that was made for a [destination
    charge](https://docs.stripe.com/docs/connect/destination-charges) is allowed only up to the amount of
    the charge. It is possible to reverse a
    [transfer_group](https://docs.stripe.com/connect/separate-charges-and-transfers#transfer-options)
    transfer only if the destination account has enough balance to cover the
    reversal.

    Related guide: [Reverse transfers](https://docs.stripe.com/connect/separate-charges-and-transfers#reverse-transfers)
    """

    OBJECT_NAME: ClassVar[Literal["transfer_reversal"]] = "transfer_reversal"
    amount: int
    """
    Amount, in cents (or local equivalent).
    """
    balance_transaction: Optional[ExpandableField["BalanceTransaction"]]
    """
    Balance transaction that describes the impact on your account balance.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    destination_payment_refund: Optional[ExpandableField["Refund"]]
    """
    Linked payment refund for the transfer reversal.
    """
    id: str
    """
    Unique identifier for the object.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["transfer_reversal"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    source_refund: Optional[ExpandableField["Refund"]]
    """
    ID of the refund responsible for the transfer reversal.
    """
    transfer: ExpandableField["Transfer"]
    """
    ID of the transfer that was reversed.
    """

    def instance_url(self):
        token = self.id
        transfer = self.transfer
        if isinstance(transfer, Transfer):
            transfer = transfer.id
        base = Transfer.class_url()
        cust_extn = sanitize_id(transfer)
        extn = sanitize_id(token)
        return "%s/%s/reversals/%s" % (base, cust_extn, extn)

    @classmethod
    def modify(cls, sid, **params):
        raise NotImplementedError(
            "Can't modify a reversal without a transfer ID. "
            "Use stripe.Transfer.modify_reversal('transfer_id', 'reversal_id', ...) "
            "(see https://stripe.com/docs/api/transfer_reversals/update)."
        )

    @classmethod
    def retrieve(cls, id, **params):
        raise NotImplementedError(
            "Can't retrieve a reversal without a transfer ID. "
            "Use stripe.Transfer.retrieve_reversal('transfer_id', 'reversal_id') "
            "(see https://stripe.com/docs/api/transfer_reversals/retrieve)."
        )
