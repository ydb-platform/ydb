# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._application_fee import ApplicationFee
from stripe._expandable_field import ExpandableField
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import sanitize_id
from typing import ClassVar, Dict, Optional, cast
from typing_extensions import Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._balance_transaction import BalanceTransaction


class ApplicationFeeRefund(UpdateableAPIResource["ApplicationFeeRefund"]):
    """
    `Application Fee Refund` objects allow you to refund an application fee that
    has previously been created but not yet refunded. Funds will be refunded to
    the Stripe account from which the fee was originally collected.

    Related guide: [Refunding application fees](https://docs.stripe.com/connect/destination-charges#refunding-app-fee)
    """

    OBJECT_NAME: ClassVar[Literal["fee_refund"]] = "fee_refund"
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
    fee: ExpandableField["ApplicationFee"]
    """
    ID of the application fee that was refunded.
    """
    id: str
    """
    Unique identifier for the object.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["fee_refund"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """

    @classmethod
    def _build_instance_url(cls, fee, sid):
        base = ApplicationFee.class_url()
        cust_extn = sanitize_id(fee)
        extn = sanitize_id(sid)
        return "%s/%s/refunds/%s" % (base, cust_extn, extn)

    @classmethod
    def modify(cls, fee, sid, **params) -> "ApplicationFeeRefund":
        url = cls._build_instance_url(fee, sid)
        return cast(
            "ApplicationFeeRefund",
            cls._static_request("post", url, params=params),
        )

    def instance_url(self):
        return self._build_instance_url(self.fee, self.id)

    @classmethod
    def retrieve(cls, id, **params) -> "ApplicationFeeRefund":
        raise NotImplementedError(
            "Can't retrieve a refund without an application fee ID. "
            "Use application_fee.refunds.retrieve('refund_id') instead."
        )
