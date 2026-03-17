# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._api_resource import APIResource
from stripe._customer import Customer
from stripe._expandable_field import ExpandableField
from stripe._util import sanitize_id
from typing import ClassVar, Dict, Optional
from typing_extensions import Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._credit_note import CreditNote
    from stripe._invoice import Invoice
    from stripe.checkout._session import Session


class CustomerBalanceTransaction(APIResource["CustomerBalanceTransaction"]):
    """
    Each customer has a [Balance](https://docs.stripe.com/api/customers/object#customer_object-balance) value,
    which denotes a debit or credit that's automatically applied to their next invoice upon finalization.
    You may modify the value directly by using the [update customer API](https://docs.stripe.com/api/customers/update),
    or by creating a Customer Balance Transaction, which increments or decrements the customer's `balance` by the specified `amount`.

    Related guide: [Customer balance](https://docs.stripe.com/billing/customer/balance)
    """

    OBJECT_NAME: ClassVar[Literal["customer_balance_transaction"]] = (
        "customer_balance_transaction"
    )
    amount: int
    """
    The amount of the transaction. A negative value is a credit for the customer's balance, and a positive value is a debit to the customer's `balance`.
    """
    checkout_session: Optional[ExpandableField["Session"]]
    """
    The ID of the checkout session (if any) that created the transaction.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    credit_note: Optional[ExpandableField["CreditNote"]]
    """
    The ID of the credit note (if any) related to the transaction.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    customer: ExpandableField["Customer"]
    """
    The ID of the customer the transaction belongs to.
    """
    customer_account: Optional[str]
    """
    The ID of an Account representing a customer that the transaction belongs to.
    """
    description: Optional[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    ending_balance: int
    """
    The customer's `balance` after the transaction was applied. A negative value decreases the amount due on the customer's next invoice. A positive value increases the amount due on the customer's next invoice.
    """
    id: str
    """
    Unique identifier for the object.
    """
    invoice: Optional[ExpandableField["Invoice"]]
    """
    The ID of the invoice (if any) related to the transaction.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["customer_balance_transaction"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    type: Literal[
        "adjustment",
        "applied_to_invoice",
        "checkout_session_subscription_payment",
        "checkout_session_subscription_payment_canceled",
        "credit_note",
        "initial",
        "invoice_overpaid",
        "invoice_too_large",
        "invoice_too_small",
        "migration",
        "unapplied_from_invoice",
        "unspent_receiver_credit",
    ]
    """
    Transaction type: `adjustment`, `applied_to_invoice`, `credit_note`, `initial`, `invoice_overpaid`, `invoice_too_large`, `invoice_too_small`, `unspent_receiver_credit`, `unapplied_from_invoice`, `checkout_session_subscription_payment`, or `checkout_session_subscription_payment_canceled`. See the [Customer Balance page](https://docs.stripe.com/billing/customer/balance#types) to learn more about transaction types.
    """

    def instance_url(self):
        token = self.id
        customer = self.customer
        if isinstance(customer, Customer):
            customer = customer.id
        base = Customer.class_url()
        cust_extn = sanitize_id(customer)
        extn = sanitize_id(token)
        return "%s/%s/balance_transactions/%s" % (base, cust_extn, extn)

    @classmethod
    def retrieve(cls, id, **params) -> "CustomerBalanceTransaction":
        raise NotImplementedError(
            "Can't retrieve a Customer Balance Transaction without a Customer ID. "
            "Use Customer.retrieve_customer_balance_transaction('cus_123', 'cbtxn_123')"
        )
