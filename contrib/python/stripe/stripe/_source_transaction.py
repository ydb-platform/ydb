# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_object import StripeObject
from typing import ClassVar, Optional
from typing_extensions import Literal


class SourceTransaction(StripeObject):
    """
    Some payment methods have no required amount that a customer must send.
    Customers can be instructed to send any amount, and it can be made up of
    multiple transactions. As such, sources can have multiple associated
    transactions.
    """

    OBJECT_NAME: ClassVar[Literal["source_transaction"]] = "source_transaction"

    class AchCreditTransfer(StripeObject):
        customer_data: Optional[str]
        """
        Customer data associated with the transfer.
        """
        fingerprint: Optional[str]
        """
        Bank account fingerprint associated with the transfer.
        """
        last4: Optional[str]
        """
        Last 4 digits of the account number associated with the transfer.
        """
        routing_number: Optional[str]
        """
        Routing number associated with the transfer.
        """

    class ChfCreditTransfer(StripeObject):
        reference: Optional[str]
        """
        Reference associated with the transfer.
        """
        sender_address_country: Optional[str]
        """
        Sender's country address.
        """
        sender_address_line1: Optional[str]
        """
        Sender's line 1 address.
        """
        sender_iban: Optional[str]
        """
        Sender's bank account IBAN.
        """
        sender_name: Optional[str]
        """
        Sender's name.
        """

    class GbpCreditTransfer(StripeObject):
        fingerprint: Optional[str]
        """
        Bank account fingerprint associated with the Stripe owned bank account receiving the transfer.
        """
        funding_method: Optional[str]
        """
        The credit transfer rails the sender used to push this transfer. The possible rails are: Faster Payments, BACS, CHAPS, and wire transfers. Currently only Faster Payments is supported.
        """
        last4: Optional[str]
        """
        Last 4 digits of sender account number associated with the transfer.
        """
        reference: Optional[str]
        """
        Sender entered arbitrary information about the transfer.
        """
        sender_account_number: Optional[str]
        """
        Sender account number associated with the transfer.
        """
        sender_name: Optional[str]
        """
        Sender name associated with the transfer.
        """
        sender_sort_code: Optional[str]
        """
        Sender sort code associated with the transfer.
        """

    class PaperCheck(StripeObject):
        available_at: Optional[str]
        """
        Time at which the deposited funds will be available for use. Measured in seconds since the Unix epoch.
        """
        invoices: Optional[str]
        """
        Comma-separated list of invoice IDs associated with the paper check.
        """

    class SepaCreditTransfer(StripeObject):
        reference: Optional[str]
        """
        Reference associated with the transfer.
        """
        sender_iban: Optional[str]
        """
        Sender's bank account IBAN.
        """
        sender_name: Optional[str]
        """
        Sender's name.
        """

    ach_credit_transfer: Optional[AchCreditTransfer]
    amount: int
    """
    A positive integer in the smallest currency unit (that is, 100 cents for $1.00, or 1 for ¥1, Japanese Yen being a zero-decimal currency) representing the amount your customer has pushed to the receiver.
    """
    chf_credit_transfer: Optional[ChfCreditTransfer]
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    gbp_credit_transfer: Optional[GbpCreditTransfer]
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["source_transaction"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    paper_check: Optional[PaperCheck]
    sepa_credit_transfer: Optional[SepaCreditTransfer]
    source: str
    """
    The ID of the source this transaction is attached to.
    """
    status: str
    """
    The status of the transaction, one of `succeeded`, `pending`, or `failed`.
    """
    type: Literal[
        "ach_credit_transfer",
        "ach_debit",
        "alipay",
        "bancontact",
        "card",
        "card_present",
        "eps",
        "giropay",
        "ideal",
        "klarna",
        "multibanco",
        "p24",
        "sepa_debit",
        "sofort",
        "three_d_secure",
        "wechat",
    ]
    """
    The type of source this transaction is attached to.
    """
    _inner_class_types = {
        "ach_credit_transfer": AchCreditTransfer,
        "chf_credit_transfer": ChfCreditTransfer,
        "gbp_credit_transfer": GbpCreditTransfer,
        "paper_check": PaperCheck,
        "sepa_credit_transfer": SepaCreditTransfer,
    }
