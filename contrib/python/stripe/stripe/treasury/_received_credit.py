# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._test_helpers import APIResourceTestHelpers
from typing import ClassVar, Optional, cast
from typing_extensions import Literal, Type, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._payout import Payout
    from stripe.params.treasury._received_credit_create_params import (
        ReceivedCreditCreateParams,
    )
    from stripe.params.treasury._received_credit_list_params import (
        ReceivedCreditListParams,
    )
    from stripe.params.treasury._received_credit_retrieve_params import (
        ReceivedCreditRetrieveParams,
    )
    from stripe.treasury._credit_reversal import CreditReversal
    from stripe.treasury._outbound_payment import OutboundPayment
    from stripe.treasury._outbound_transfer import OutboundTransfer
    from stripe.treasury._transaction import Transaction


class ReceivedCredit(ListableAPIResource["ReceivedCredit"]):
    """
    ReceivedCredits represent funds sent to a [FinancialAccount](https://api.stripe.com#financial_accounts) (for example, via ACH or wire). These money movements are not initiated from the FinancialAccount.
    """

    OBJECT_NAME: ClassVar[Literal["treasury.received_credit"]] = (
        "treasury.received_credit"
    )

    class InitiatingPaymentMethodDetails(StripeObject):
        class BillingDetails(StripeObject):
            class Address(StripeObject):
                city: Optional[str]
                """
                City, district, suburb, town, or village.
                """
                country: Optional[str]
                """
                Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
                """
                line1: Optional[str]
                """
                Address line 1, such as the street, PO Box, or company name.
                """
                line2: Optional[str]
                """
                Address line 2, such as the apartment, suite, unit, or building.
                """
                postal_code: Optional[str]
                """
                ZIP or postal code.
                """
                state: Optional[str]
                """
                State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
                """

            address: Address
            email: Optional[str]
            """
            Email address.
            """
            name: Optional[str]
            """
            Full name.
            """
            _inner_class_types = {"address": Address}

        class FinancialAccount(StripeObject):
            id: str
            """
            The FinancialAccount ID.
            """
            network: Literal["stripe"]
            """
            The rails the ReceivedCredit was sent over. A FinancialAccount can only send funds over `stripe`.
            """

        class UsBankAccount(StripeObject):
            bank_name: Optional[str]
            """
            Bank name.
            """
            last4: Optional[str]
            """
            The last four digits of the bank account number.
            """
            routing_number: Optional[str]
            """
            The routing number for the bank account.
            """

        balance: Optional[Literal["payments"]]
        """
        Set when `type` is `balance`.
        """
        billing_details: BillingDetails
        financial_account: Optional[FinancialAccount]
        issuing_card: Optional[str]
        """
        Set when `type` is `issuing_card`. This is an [Issuing Card](https://api.stripe.com#issuing_cards) ID.
        """
        type: Literal[
            "balance",
            "financial_account",
            "issuing_card",
            "stripe",
            "us_bank_account",
        ]
        """
        Polymorphic type matching the originating money movement's source. This can be an external account, a Stripe balance, or a FinancialAccount.
        """
        us_bank_account: Optional[UsBankAccount]
        _inner_class_types = {
            "billing_details": BillingDetails,
            "financial_account": FinancialAccount,
            "us_bank_account": UsBankAccount,
        }

    class LinkedFlows(StripeObject):
        class SourceFlowDetails(StripeObject):
            credit_reversal: Optional["CreditReversal"]
            """
            You can reverse some [ReceivedCredits](https://api.stripe.com#received_credits) depending on their network and source flow. Reversing a ReceivedCredit leads to the creation of a new object known as a CreditReversal.
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
            payout: Optional["Payout"]
            """
            A `Payout` object is created when you receive funds from Stripe, or when you
            initiate a payout to either a bank account or debit card of a [connected
            Stripe account](https://docs.stripe.com/docs/connect/bank-debit-card-payouts). You can retrieve individual payouts,
            and list all payouts. Payouts are made on [varying
            schedules](https://docs.stripe.com/docs/connect/manage-payout-schedule), depending on your country and
            industry.

            Related guide: [Receiving payouts](https://docs.stripe.com/payouts)
            """
            type: Literal[
                "credit_reversal",
                "other",
                "outbound_payment",
                "outbound_transfer",
                "payout",
            ]
            """
            The type of the source flow that originated the ReceivedCredit.
            """

        credit_reversal: Optional[str]
        """
        The CreditReversal created as a result of this ReceivedCredit being reversed.
        """
        issuing_authorization: Optional[str]
        """
        Set if the ReceivedCredit was created due to an [Issuing Authorization](https://api.stripe.com#issuing_authorizations) object.
        """
        issuing_transaction: Optional[str]
        """
        Set if the ReceivedCredit is also viewable as an [Issuing transaction](https://api.stripe.com#issuing_transactions) object.
        """
        source_flow: Optional[str]
        """
        ID of the source flow. Set if `network` is `stripe` and the source flow is visible to the user. Examples of source flows include OutboundPayments, payouts, or CreditReversals.
        """
        source_flow_details: Optional[SourceFlowDetails]
        """
        The expandable object of the source flow.
        """
        source_flow_type: Optional[str]
        """
        The type of flow that originated the ReceivedCredit (for example, `outbound_payment`).
        """
        _inner_class_types = {"source_flow_details": SourceFlowDetails}

    class ReversalDetails(StripeObject):
        deadline: Optional[int]
        """
        Time before which a ReceivedCredit can be reversed.
        """
        restricted_reason: Optional[
            Literal[
                "already_reversed",
                "deadline_passed",
                "network_restricted",
                "other",
                "source_flow_restricted",
            ]
        ]
        """
        Set if a ReceivedCredit cannot be reversed.
        """

    amount: int
    """
    Amount (in cents) transferred.
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
    failure_code: Optional[
        Literal[
            "account_closed",
            "account_frozen",
            "international_transaction",
            "other",
        ]
    ]
    """
    Reason for the failure. A ReceivedCredit might fail because the receiving FinancialAccount is closed or frozen.
    """
    financial_account: Optional[str]
    """
    The FinancialAccount that received the funds.
    """
    hosted_regulatory_receipt_url: Optional[str]
    """
    A [hosted transaction receipt](https://docs.stripe.com/treasury/moving-money/regulatory-receipts) URL that is provided when money movement is considered regulated under Stripe's money transmission licenses.
    """
    id: str
    """
    Unique identifier for the object.
    """
    initiating_payment_method_details: InitiatingPaymentMethodDetails
    linked_flows: LinkedFlows
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    network: Literal["ach", "card", "stripe", "us_domestic_wire"]
    """
    The rails used to send the funds.
    """
    object: Literal["treasury.received_credit"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    reversal_details: Optional[ReversalDetails]
    """
    Details describing when a ReceivedCredit may be reversed.
    """
    status: Literal["failed", "succeeded"]
    """
    Status of the ReceivedCredit. ReceivedCredits are created either `succeeded` (approved) or `failed` (declined). If a ReceivedCredit is declined, the failure reason can be found in the `failure_code` field.
    """
    transaction: Optional[ExpandableField["Transaction"]]
    """
    The Transaction associated with this object.
    """

    @classmethod
    def list(
        cls, **params: Unpack["ReceivedCreditListParams"]
    ) -> ListObject["ReceivedCredit"]:
        """
        Returns a list of ReceivedCredits.
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
        cls, **params: Unpack["ReceivedCreditListParams"]
    ) -> ListObject["ReceivedCredit"]:
        """
        Returns a list of ReceivedCredits.
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
        cls, id: str, **params: Unpack["ReceivedCreditRetrieveParams"]
    ) -> "ReceivedCredit":
        """
        Retrieves the details of an existing ReceivedCredit by passing the unique ReceivedCredit ID from the ReceivedCredit list.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["ReceivedCreditRetrieveParams"]
    ) -> "ReceivedCredit":
        """
        Retrieves the details of an existing ReceivedCredit by passing the unique ReceivedCredit ID from the ReceivedCredit list.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    class TestHelpers(APIResourceTestHelpers["ReceivedCredit"]):
        _resource_cls: Type["ReceivedCredit"]

        @classmethod
        def create(
            cls, **params: Unpack["ReceivedCreditCreateParams"]
        ) -> "ReceivedCredit":
            """
            Use this endpoint to simulate a test mode ReceivedCredit initiated by a third party. In live mode, you can't directly create ReceivedCredits initiated by third parties.
            """
            return cast(
                "ReceivedCredit",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/treasury/received_credits",
                    params=params,
                ),
            )

        @classmethod
        async def create_async(
            cls, **params: Unpack["ReceivedCreditCreateParams"]
        ) -> "ReceivedCredit":
            """
            Use this endpoint to simulate a test mode ReceivedCredit initiated by a third party. In live mode, you can't directly create ReceivedCredits initiated by third parties.
            """
            return cast(
                "ReceivedCredit",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/treasury/received_credits",
                    params=params,
                ),
            )

    @property
    def test_helpers(self):
        return self.TestHelpers(self)

    _inner_class_types = {
        "initiating_payment_method_details": InitiatingPaymentMethodDetails,
        "linked_flows": LinkedFlows,
        "reversal_details": ReversalDetails,
    }


ReceivedCredit.TestHelpers._resource_cls = ReceivedCredit
