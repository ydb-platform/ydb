# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from typing import ClassVar, Optional
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.issuing._authorization import Authorization
    from stripe.params.treasury._transaction_list_params import (
        TransactionListParams,
    )
    from stripe.params.treasury._transaction_retrieve_params import (
        TransactionRetrieveParams,
    )
    from stripe.treasury._credit_reversal import CreditReversal
    from stripe.treasury._debit_reversal import DebitReversal
    from stripe.treasury._inbound_transfer import InboundTransfer
    from stripe.treasury._outbound_payment import OutboundPayment
    from stripe.treasury._outbound_transfer import OutboundTransfer
    from stripe.treasury._received_credit import ReceivedCredit
    from stripe.treasury._received_debit import ReceivedDebit
    from stripe.treasury._transaction_entry import TransactionEntry


class Transaction(ListableAPIResource["Transaction"]):
    """
    Transactions represent changes to a [FinancialAccount's](https://api.stripe.com#financial_accounts) balance.
    """

    OBJECT_NAME: ClassVar[Literal["treasury.transaction"]] = (
        "treasury.transaction"
    )

    class BalanceImpact(StripeObject):
        cash: int
        """
        The change made to funds the user can spend right now.
        """
        inbound_pending: int
        """
        The change made to funds that are not spendable yet, but will become available at a later time.
        """
        outbound_pending: int
        """
        The change made to funds in the account, but not spendable because they are being held for pending outbound flows.
        """

    class FlowDetails(StripeObject):
        credit_reversal: Optional["CreditReversal"]
        """
        You can reverse some [ReceivedCredits](https://api.stripe.com#received_credits) depending on their network and source flow. Reversing a ReceivedCredit leads to the creation of a new object known as a CreditReversal.
        """
        debit_reversal: Optional["DebitReversal"]
        """
        You can reverse some [ReceivedDebits](https://api.stripe.com#received_debits) depending on their network and source flow. Reversing a ReceivedDebit leads to the creation of a new object known as a DebitReversal.
        """
        inbound_transfer: Optional["InboundTransfer"]
        """
        Use [InboundTransfers](https://docs.stripe.com/docs/treasury/moving-money/financial-accounts/into/inbound-transfers) to add funds to your [FinancialAccount](https://api.stripe.com#financial_accounts) via a PaymentMethod that is owned by you. The funds will be transferred via an ACH debit.

        Related guide: [Moving money with Treasury using InboundTransfer objects](https://docs.stripe.com/docs/treasury/moving-money/financial-accounts/into/inbound-transfers)
        """
        issuing_authorization: Optional["Authorization"]
        """
        When an [issued card](https://docs.stripe.com/issuing) is used to make a purchase, an Issuing `Authorization`
        object is created. [Authorizations](https://docs.stripe.com/issuing/purchases/authorizations) must be approved for the
        purchase to be completed successfully.

        Related guide: [Issued card authorizations](https://docs.stripe.com/issuing/purchases/authorizations)
        """
        outbound_payment: Optional["OutboundPayment"]
        """
        Use [OutboundPayments](https://docs.stripe.com/docs/treasury/moving-money/financial-accounts/out-of/outbound-payments) to send funds to another party's external bank account or [FinancialAccount](https://api.stripe.com#financial_accounts). To send money to an account belonging to the same user, use an [OutboundTransfer](https://api.stripe.com#outbound_transfers).

        Simulate OutboundPayment state changes with the `/v1/test_helpers/treasury/outbound_payments` endpoints. These methods can only be called on test mode objects.

        Related guide: [Moving money with Treasury using OutboundPayment objects](https://docs.stripe.com/docs/treasury/moving-money/financial-accounts/out-of/outbound-payments)
        """
        outbound_transfer: Optional["OutboundTransfer"]
        """
        Use [OutboundTransfers](https://docs.stripe.com/docs/treasury/moving-money/financial-accounts/out-of/outbound-transfers) to transfer funds from a [FinancialAccount](https://api.stripe.com#financial_accounts) to a PaymentMethod belonging to the same entity. To send funds to a different party, use [OutboundPayments](https://api.stripe.com#outbound_payments) instead. You can send funds over ACH rails or through a domestic wire transfer to a user's own external bank account.

        Simulate OutboundTransfer state changes with the `/v1/test_helpers/treasury/outbound_transfers` endpoints. These methods can only be called on test mode objects.

        Related guide: [Moving money with Treasury using OutboundTransfer objects](https://docs.stripe.com/docs/treasury/moving-money/financial-accounts/out-of/outbound-transfers)
        """
        received_credit: Optional["ReceivedCredit"]
        """
        ReceivedCredits represent funds sent to a [FinancialAccount](https://api.stripe.com#financial_accounts) (for example, via ACH or wire). These money movements are not initiated from the FinancialAccount.
        """
        received_debit: Optional["ReceivedDebit"]
        """
        ReceivedDebits represent funds pulled from a [FinancialAccount](https://api.stripe.com#financial_accounts). These are not initiated from the FinancialAccount.
        """
        type: Literal[
            "credit_reversal",
            "debit_reversal",
            "inbound_transfer",
            "issuing_authorization",
            "other",
            "outbound_payment",
            "outbound_transfer",
            "received_credit",
            "received_debit",
        ]
        """
        Type of the flow that created the Transaction. Set to the same value as `flow_type`.
        """

    class StatusTransitions(StripeObject):
        posted_at: Optional[int]
        """
        Timestamp describing when the Transaction changed status to `posted`.
        """
        void_at: Optional[int]
        """
        Timestamp describing when the Transaction changed status to `void`.
        """

    amount: int
    """
    Amount (in cents) transferred.
    """
    balance_impact: BalanceImpact
    """
    Change to a FinancialAccount's balance
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    description: str
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    entries: Optional[ListObject["TransactionEntry"]]
    """
    A list of TransactionEntries that are part of this Transaction. This cannot be expanded in any list endpoints.
    """
    financial_account: str
    """
    The FinancialAccount associated with this object.
    """
    flow: Optional[str]
    """
    ID of the flow that created the Transaction.
    """
    flow_details: Optional[FlowDetails]
    """
    Details of the flow that created the Transaction.
    """
    flow_type: Literal[
        "credit_reversal",
        "debit_reversal",
        "inbound_transfer",
        "issuing_authorization",
        "other",
        "outbound_payment",
        "outbound_transfer",
        "received_credit",
        "received_debit",
    ]
    """
    Type of the flow that created the Transaction.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["treasury.transaction"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    status: Literal["open", "posted", "void"]
    """
    Status of the Transaction.
    """
    status_transitions: StatusTransitions

    @classmethod
    def list(
        cls, **params: Unpack["TransactionListParams"]
    ) -> ListObject["Transaction"]:
        """
        Retrieves a list of Transaction objects.
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
        cls, **params: Unpack["TransactionListParams"]
    ) -> ListObject["Transaction"]:
        """
        Retrieves a list of Transaction objects.
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
        cls, id: str, **params: Unpack["TransactionRetrieveParams"]
    ) -> "Transaction":
        """
        Retrieves the details of an existing Transaction.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["TransactionRetrieveParams"]
    ) -> "Transaction":
        """
        Retrieves the details of an existing Transaction.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "balance_impact": BalanceImpact,
        "flow_details": FlowDetails,
        "status_transitions": StatusTransitions,
    }
