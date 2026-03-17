# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._customer import Customer
from stripe._stripe_object import StripeObject
from stripe._util import sanitize_id
from typing import ClassVar, Dict, Optional
from typing_extensions import Literal


class CashBalance(StripeObject):
    """
    A customer's `Cash balance` represents real funds. Customers can add funds to their cash balance by sending a bank transfer. These funds can be used for payment and can eventually be paid out to your bank account.
    """

    OBJECT_NAME: ClassVar[Literal["cash_balance"]] = "cash_balance"

    class Settings(StripeObject):
        reconciliation_mode: Literal["automatic", "manual"]
        """
        The configuration for how funds that land in the customer cash balance are reconciled.
        """
        using_merchant_default: bool
        """
        A flag to indicate if reconciliation mode returned is the user's default or is specific to this customer cash balance
        """

    available: Optional[Dict[str, int]]
    """
    A hash of all cash balances available to this customer. You cannot delete a customer with any cash balances, even if the balance is 0. Amounts are represented in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    customer: str
    """
    The ID of the customer whose cash balance this object represents.
    """
    customer_account: Optional[str]
    """
    The ID of an Account representing a customer whose cash balance this object represents.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["cash_balance"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    settings: Settings

    def instance_url(self):
        customer = self.customer
        base = Customer.class_url()
        cust_extn = sanitize_id(customer)
        return "%s/%s/cash_balance" % (base, cust_extn)

    @classmethod
    def retrieve(cls, id, **params):
        raise NotImplementedError(
            "Can't retrieve a Customer Cash Balance without a Customer ID. "
            "Use Customer.retrieve_cash_balance('cus_123')"
        )

    _inner_class_types = {"settings": Settings}
