# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from typing import ClassVar, Optional
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._invoice import Invoice
    from stripe.billing._credit_grant import CreditGrant
    from stripe.params.billing._credit_balance_transaction_list_params import (
        CreditBalanceTransactionListParams,
    )
    from stripe.params.billing._credit_balance_transaction_retrieve_params import (
        CreditBalanceTransactionRetrieveParams,
    )
    from stripe.test_helpers._test_clock import TestClock


class CreditBalanceTransaction(
    ListableAPIResource["CreditBalanceTransaction"]
):
    """
    A credit balance transaction is a resource representing a transaction (either a credit or a debit) against an existing credit grant.
    """

    OBJECT_NAME: ClassVar[Literal["billing.credit_balance_transaction"]] = (
        "billing.credit_balance_transaction"
    )

    class Credit(StripeObject):
        class Amount(StripeObject):
            class Monetary(StripeObject):
                currency: str
                """
                Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
                """
                value: int
                """
                A positive integer representing the amount.
                """

            monetary: Optional[Monetary]
            """
            The monetary amount.
            """
            type: Literal["monetary"]
            """
            The type of this amount. We currently only support `monetary` billing credits.
            """
            _inner_class_types = {"monetary": Monetary}

        class CreditsApplicationInvoiceVoided(StripeObject):
            invoice: ExpandableField["Invoice"]
            """
            The invoice to which the reinstated billing credits were originally applied.
            """
            invoice_line_item: str
            """
            The invoice line item to which the reinstated billing credits were originally applied.
            """

        amount: Amount
        credits_application_invoice_voided: Optional[
            CreditsApplicationInvoiceVoided
        ]
        """
        Details of the invoice to which the reinstated credits were originally applied. Only present if `type` is `credits_application_invoice_voided`.
        """
        type: Literal["credits_application_invoice_voided", "credits_granted"]
        """
        The type of credit transaction.
        """
        _inner_class_types = {
            "amount": Amount,
            "credits_application_invoice_voided": CreditsApplicationInvoiceVoided,
        }

    class Debit(StripeObject):
        class Amount(StripeObject):
            class Monetary(StripeObject):
                currency: str
                """
                Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
                """
                value: int
                """
                A positive integer representing the amount.
                """

            monetary: Optional[Monetary]
            """
            The monetary amount.
            """
            type: Literal["monetary"]
            """
            The type of this amount. We currently only support `monetary` billing credits.
            """
            _inner_class_types = {"monetary": Monetary}

        class CreditsApplied(StripeObject):
            invoice: ExpandableField["Invoice"]
            """
            The invoice to which the billing credits were applied.
            """
            invoice_line_item: str
            """
            The invoice line item to which the billing credits were applied.
            """

        amount: Amount
        credits_applied: Optional[CreditsApplied]
        """
        Details of how the billing credits were applied to an invoice. Only present if `type` is `credits_applied`.
        """
        type: Literal["credits_applied", "credits_expired", "credits_voided"]
        """
        The type of debit transaction.
        """
        _inner_class_types = {
            "amount": Amount,
            "credits_applied": CreditsApplied,
        }

    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    credit: Optional[Credit]
    """
    Credit details for this credit balance transaction. Only present if type is `credit`.
    """
    credit_grant: ExpandableField["CreditGrant"]
    """
    The credit grant associated with this credit balance transaction.
    """
    debit: Optional[Debit]
    """
    Debit details for this credit balance transaction. Only present if type is `debit`.
    """
    effective_at: int
    """
    The effective time of this credit balance transaction.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["billing.credit_balance_transaction"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    test_clock: Optional[ExpandableField["TestClock"]]
    """
    ID of the test clock this credit balance transaction belongs to.
    """
    type: Optional[Literal["credit", "debit"]]
    """
    The type of credit balance transaction (credit or debit).
    """

    @classmethod
    def list(
        cls, **params: Unpack["CreditBalanceTransactionListParams"]
    ) -> ListObject["CreditBalanceTransaction"]:
        """
        Retrieve a list of credit balance transactions.
        """
        result = cls._static_request(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    async def list_async(
        cls, **params: Unpack["CreditBalanceTransactionListParams"]
    ) -> ListObject["CreditBalanceTransaction"]:
        """
        Retrieve a list of credit balance transactions.
        """
        result = await cls._static_request_async(
            "get",
            cls.class_url(),
            params=params,
        )
        if not isinstance(result, ListObject):
            raise TypeError(
                "Expected list object from API, got %s"
                % (type(result).__name__)
            )

        return result

    @classmethod
    def retrieve(
        cls,
        id: str,
        **params: Unpack["CreditBalanceTransactionRetrieveParams"],
    ) -> "CreditBalanceTransaction":
        """
        Retrieves a credit balance transaction.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls,
        id: str,
        **params: Unpack["CreditBalanceTransactionRetrieveParams"],
    ) -> "CreditBalanceTransaction":
        """
        Retrieves a credit balance transaction.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {"credit": Credit, "debit": Debit}
