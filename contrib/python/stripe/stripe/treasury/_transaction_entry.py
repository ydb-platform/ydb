# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from typing import ClassVar, Optional
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.issuing._authorization import Authorization
    from stripe.params.treasury._transaction_entry_list_params import (
        TransactionEntryListParams,
    )
    from stripe.params.treasury._transaction_entry_retrieve_params import (
        TransactionEntryRetrieveParams,
    )
    from stripe.treasury._credit_reversal import CreditReversal
    from stripe.treasury._debit_reversal import DebitReversal
    from stripe.treasury._inbound_transfer import InboundTransfer
    from stripe.treasury._outbound_payment import OutboundPayment
    from stripe.treasury._outbound_transfer import OutboundTransfer
    from stripe.treasury._received_credit import ReceivedCredit
    from stripe.treasury._received_debit import ReceivedDebit
    from stripe.treasury._transaction import Transaction


class TransactionEntry(ListableAPIResource["TransactionEntry"]):
    """
    TransactionEntries represent individual units of money movements within a single [Transaction](https://api.stripe.com#transactions).
    """

    OBJECT_NAME: ClassVar[Literal["treasury.transaction_entry"]] = (
        "treasury.transaction_entry"
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
    effective_at: int
    """
    When the TransactionEntry will impact the FinancialAccount's balance.
    """
    financial_account: str
    """
    The FinancialAccount associated with this object.
    """
    flow: Optional[str]
    """
    Token of the flow associated with the TransactionEntry.
    """
    flow_details: Optional[FlowDetails]
    """
    Details of the flow associated with the TransactionEntry.
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
    Type of the flow associated with the TransactionEntry.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["treasury.transaction_entry"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    transaction: ExpandableField["Transaction"]
    """
    The Transaction associated with this object.
    """
    type: Literal[
        "credit_reversal",
        "credit_reversal_posting",
        "debit_reversal",
        "inbound_transfer",
        "inbound_transfer_return",
        "issuing_authorization_hold",
        "issuing_authorization_release",
        "other",
        "outbound_payment",
        "outbound_payment_cancellation",
        "outbound_payment_failure",
        "outbound_payment_posting",
        "outbound_payment_return",
        "outbound_transfer",
        "outbound_transfer_cancellation",
        "outbound_transfer_failure",
        "outbound_transfer_posting",
        "outbound_transfer_return",
        "received_credit",
        "received_debit",
    ]
    """
    The specific money movement that generated the TransactionEntry.
    """

    @classmethod
    def list(
        cls, **params: Unpack["TransactionEntryListParams"]
    ) -> ListObject["TransactionEntry"]:
        """
        Retrieves a list of TransactionEntry objects.
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
        cls, **params: Unpack["TransactionEntryListParams"]
    ) -> ListObject["TransactionEntry"]:
        """
        Retrieves a list of TransactionEntry objects.
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
        cls, id: str, **params: Unpack["TransactionEntryRetrieveParams"]
    ) -> "TransactionEntry":
        """
        Retrieves a TransactionEntry object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["TransactionEntryRetrieveParams"]
    ) -> "TransactionEntry":
        """
        Retrieves a TransactionEntry object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def class_url(cls):
        return "/v1/treasury/transaction_entries"

    _inner_class_types = {
        "balance_impact": BalanceImpact,
        "flow_details": FlowDetails,
    }
