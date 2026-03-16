# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar, Optional
from typing_extensions import Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._source import Source


class SourceMandateNotification(StripeObject):
    """
    Source mandate notifications should be created when a notification related to
    a source mandate must be sent to the payer. They will trigger a webhook or
    deliver an email to the customer.
    """

    OBJECT_NAME: ClassVar[Literal["source_mandate_notification"]] = (
        "source_mandate_notification"
    )

    class AcssDebit(StripeObject):
        statement_descriptor: Optional[str]
        """
        The statement descriptor associate with the debit.
        """

    class BacsDebit(StripeObject):
        last4: Optional[str]
        """
        Last 4 digits of the account number associated with the debit.
        """

    class SepaDebit(StripeObject):
        creditor_identifier: Optional[str]
        """
        SEPA creditor ID.
        """
        last4: Optional[str]
        """
        Last 4 digits of the account number associated with the debit.
        """
        mandate_reference: Optional[str]
        """
        Mandate reference associated with the debit.
        """

    acss_debit: Optional[AcssDebit]
    amount: Optional[int]
    """
    A positive integer in the smallest currency unit (that is, 100 cents for $1.00, or 1 for ¥1, Japanese Yen being a zero-decimal currency) representing the amount associated with the mandate notification. The amount is expressed in the currency of the underlying source. Required if the notification type is `debit_initiated`.
    """
    bacs_debit: Optional[BacsDebit]
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["source_mandate_notification"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    reason: str
    """
    The reason of the mandate notification. Valid reasons are `mandate_confirmed` or `debit_initiated`.
    """
    sepa_debit: Optional[SepaDebit]
    source: "Source"
    """
    `Source` objects allow you to accept a variety of payment methods. They
    represent a customer's payment instrument, and can be used with the Stripe API
    just like a `Card` object: once chargeable, they can be charged, or can be
    attached to customers.

    Stripe doesn't recommend using the deprecated [Sources API](https://docs.stripe.com/api/sources).
    We recommend that you adopt the [PaymentMethods API](https://docs.stripe.com/api/payment_methods).
    This newer API provides access to our latest features and payment method types.

    Related guides: [Sources API](https://docs.stripe.com/sources) and [Sources & Customers](https://docs.stripe.com/sources/customers).
    """
    status: str
    """
    The status of the mandate notification. Valid statuses are `pending` or `submitted`.
    """
    type: str
    """
    The type of source this mandate notification is attached to. Should be the source type identifier code for the payment method, such as `three_d_secure`.
    """
    _inner_class_types = {
        "acss_debit": AcssDebit,
        "bacs_debit": BacsDebit,
        "sepa_debit": SepaDebit,
    }
