# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._test_helpers import APIResourceTestHelpers
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, Optional, cast, overload
from typing_extensions import Literal, Type, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._mandate import Mandate
    from stripe.params.treasury._inbound_transfer_cancel_params import (
        InboundTransferCancelParams,
    )
    from stripe.params.treasury._inbound_transfer_create_params import (
        InboundTransferCreateParams,
    )
    from stripe.params.treasury._inbound_transfer_fail_params import (
        InboundTransferFailParams,
    )
    from stripe.params.treasury._inbound_transfer_list_params import (
        InboundTransferListParams,
    )
    from stripe.params.treasury._inbound_transfer_retrieve_params import (
        InboundTransferRetrieveParams,
    )
    from stripe.params.treasury._inbound_transfer_return_inbound_transfer_params import (
        InboundTransferReturnInboundTransferParams,
    )
    from stripe.params.treasury._inbound_transfer_succeed_params import (
        InboundTransferSucceedParams,
    )
    from stripe.treasury._transaction import Transaction


class InboundTransfer(
    CreateableAPIResource["InboundTransfer"],
    ListableAPIResource["InboundTransfer"],
):
    """
    Use [InboundTransfers](https://docs.stripe.com/docs/treasury/moving-money/financial-accounts/into/inbound-transfers) to add funds to your [FinancialAccount](https://api.stripe.com#financial_accounts) via a PaymentMethod that is owned by you. The funds will be transferred via an ACH debit.

    Related guide: [Moving money with Treasury using InboundTransfer objects](https://docs.stripe.com/docs/treasury/moving-money/financial-accounts/into/inbound-transfers)
    """

    OBJECT_NAME: ClassVar[Literal["treasury.inbound_transfer"]] = (
        "treasury.inbound_transfer"
    )

    class FailureDetails(StripeObject):
        code: Literal[
            "account_closed",
            "account_frozen",
            "bank_account_restricted",
            "bank_ownership_changed",
            "debit_not_authorized",
            "incorrect_account_holder_address",
            "incorrect_account_holder_name",
            "incorrect_account_holder_tax_id",
            "insufficient_funds",
            "invalid_account_number",
            "invalid_currency",
            "no_account",
            "other",
        ]
        """
        Reason for the failure.
        """

    class LinkedFlows(StripeObject):
        received_debit: Optional[str]
        """
        If funds for this flow were returned after the flow went to the `succeeded` state, this field contains a reference to the ReceivedDebit return.
        """

    class OriginPaymentMethodDetails(StripeObject):
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

        class UsBankAccount(StripeObject):
            account_holder_type: Optional[Literal["company", "individual"]]
            """
            Account holder type: individual or company.
            """
            account_type: Optional[Literal["checking", "savings"]]
            """
            Account type: checkings or savings. Defaults to checking if omitted.
            """
            bank_name: Optional[str]
            """
            Name of the bank associated with the bank account.
            """
            fingerprint: Optional[str]
            """
            Uniquely identifies this particular bank account. You can use this attribute to check whether two bank accounts are the same.
            """
            last4: Optional[str]
            """
            Last four digits of the bank account number.
            """
            mandate: Optional[ExpandableField["Mandate"]]
            """
            ID of the mandate used to make this payment.
            """
            network: Literal["ach"]
            """
            The network rails used. See the [docs](https://docs.stripe.com/treasury/money-movement/timelines) to learn more about money movement timelines for each network type.
            """
            routing_number: Optional[str]
            """
            Routing number of the bank account.
            """

        billing_details: BillingDetails
        type: Literal["us_bank_account"]
        """
        The type of the payment method used in the InboundTransfer.
        """
        us_bank_account: Optional[UsBankAccount]
        _inner_class_types = {
            "billing_details": BillingDetails,
            "us_bank_account": UsBankAccount,
        }

    class StatusTransitions(StripeObject):
        canceled_at: Optional[int]
        """
        Timestamp describing when an InboundTransfer changed status to `canceled`.
        """
        failed_at: Optional[int]
        """
        Timestamp describing when an InboundTransfer changed status to `failed`.
        """
        succeeded_at: Optional[int]
        """
        Timestamp describing when an InboundTransfer changed status to `succeeded`.
        """

    amount: int
    """
    Amount (in cents) transferred.
    """
    cancelable: bool
    """
    Returns `true` if the InboundTransfer is able to be canceled.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    description: Optional[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    failure_details: Optional[FailureDetails]
    """
    Details about this InboundTransfer's failure. Only set when status is `failed`.
    """
    financial_account: str
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
    linked_flows: LinkedFlows
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["treasury.inbound_transfer"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    origin_payment_method: Optional[str]
    """
    The origin payment method to be debited for an InboundTransfer.
    """
    origin_payment_method_details: Optional[OriginPaymentMethodDetails]
    """
    Details about the PaymentMethod for an InboundTransfer.
    """
    returned: Optional[bool]
    """
    Returns `true` if the funds for an InboundTransfer were returned after the InboundTransfer went to the `succeeded` state.
    """
    statement_descriptor: str
    """
    Statement descriptor shown when funds are debited from the source. Not all payment networks support `statement_descriptor`.
    """
    status: Literal["canceled", "failed", "processing", "succeeded"]
    """
    Status of the InboundTransfer: `processing`, `succeeded`, `failed`, and `canceled`. An InboundTransfer is `processing` if it is created and pending. The status changes to `succeeded` once the funds have been "confirmed" and a `transaction` is created and posted. The status changes to `failed` if the transfer fails.
    """
    status_transitions: StatusTransitions
    transaction: Optional[ExpandableField["Transaction"]]
    """
    The Transaction associated with this object.
    """

    @classmethod
    def _cls_cancel(
        cls,
        inbound_transfer: str,
        **params: Unpack["InboundTransferCancelParams"],
    ) -> "InboundTransfer":
        """
        Cancels an InboundTransfer.
        """
        return cast(
            "InboundTransfer",
            cls._static_request(
                "post",
                "/v1/treasury/inbound_transfers/{inbound_transfer}/cancel".format(
                    inbound_transfer=sanitize_id(inbound_transfer)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def cancel(
        inbound_transfer: str, **params: Unpack["InboundTransferCancelParams"]
    ) -> "InboundTransfer":
        """
        Cancels an InboundTransfer.
        """
        ...

    @overload
    def cancel(
        self, **params: Unpack["InboundTransferCancelParams"]
    ) -> "InboundTransfer":
        """
        Cancels an InboundTransfer.
        """
        ...

    @class_method_variant("_cls_cancel")
    def cancel(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InboundTransferCancelParams"]
    ) -> "InboundTransfer":
        """
        Cancels an InboundTransfer.
        """
        return cast(
            "InboundTransfer",
            self._request(
                "post",
                "/v1/treasury/inbound_transfers/{inbound_transfer}/cancel".format(
                    inbound_transfer=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_cancel_async(
        cls,
        inbound_transfer: str,
        **params: Unpack["InboundTransferCancelParams"],
    ) -> "InboundTransfer":
        """
        Cancels an InboundTransfer.
        """
        return cast(
            "InboundTransfer",
            await cls._static_request_async(
                "post",
                "/v1/treasury/inbound_transfers/{inbound_transfer}/cancel".format(
                    inbound_transfer=sanitize_id(inbound_transfer)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def cancel_async(
        inbound_transfer: str, **params: Unpack["InboundTransferCancelParams"]
    ) -> "InboundTransfer":
        """
        Cancels an InboundTransfer.
        """
        ...

    @overload
    async def cancel_async(
        self, **params: Unpack["InboundTransferCancelParams"]
    ) -> "InboundTransfer":
        """
        Cancels an InboundTransfer.
        """
        ...

    @class_method_variant("_cls_cancel_async")
    async def cancel_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["InboundTransferCancelParams"]
    ) -> "InboundTransfer":
        """
        Cancels an InboundTransfer.
        """
        return cast(
            "InboundTransfer",
            await self._request_async(
                "post",
                "/v1/treasury/inbound_transfers/{inbound_transfer}/cancel".format(
                    inbound_transfer=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(
        cls, **params: Unpack["InboundTransferCreateParams"]
    ) -> "InboundTransfer":
        """
        Creates an InboundTransfer.
        """
        return cast(
            "InboundTransfer",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["InboundTransferCreateParams"]
    ) -> "InboundTransfer":
        """
        Creates an InboundTransfer.
        """
        return cast(
            "InboundTransfer",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["InboundTransferListParams"]
    ) -> ListObject["InboundTransfer"]:
        """
        Returns a list of InboundTransfers sent from the specified FinancialAccount.
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
        cls, **params: Unpack["InboundTransferListParams"]
    ) -> ListObject["InboundTransfer"]:
        """
        Returns a list of InboundTransfers sent from the specified FinancialAccount.
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
        cls, id: str, **params: Unpack["InboundTransferRetrieveParams"]
    ) -> "InboundTransfer":
        """
        Retrieves the details of an existing InboundTransfer.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["InboundTransferRetrieveParams"]
    ) -> "InboundTransfer":
        """
        Retrieves the details of an existing InboundTransfer.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    class TestHelpers(APIResourceTestHelpers["InboundTransfer"]):
        _resource_cls: Type["InboundTransfer"]

        @classmethod
        def _cls_fail(
            cls, id: str, **params: Unpack["InboundTransferFailParams"]
        ) -> "InboundTransfer":
            """
            Transitions a test mode created InboundTransfer to the failed status. The InboundTransfer must already be in the processing state.
            """
            return cast(
                "InboundTransfer",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/treasury/inbound_transfers/{id}/fail".format(
                        id=sanitize_id(id)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def fail(
            id: str, **params: Unpack["InboundTransferFailParams"]
        ) -> "InboundTransfer":
            """
            Transitions a test mode created InboundTransfer to the failed status. The InboundTransfer must already be in the processing state.
            """
            ...

        @overload
        def fail(
            self, **params: Unpack["InboundTransferFailParams"]
        ) -> "InboundTransfer":
            """
            Transitions a test mode created InboundTransfer to the failed status. The InboundTransfer must already be in the processing state.
            """
            ...

        @class_method_variant("_cls_fail")
        def fail(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["InboundTransferFailParams"]
        ) -> "InboundTransfer":
            """
            Transitions a test mode created InboundTransfer to the failed status. The InboundTransfer must already be in the processing state.
            """
            return cast(
                "InboundTransfer",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/treasury/inbound_transfers/{id}/fail".format(
                        id=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_fail_async(
            cls, id: str, **params: Unpack["InboundTransferFailParams"]
        ) -> "InboundTransfer":
            """
            Transitions a test mode created InboundTransfer to the failed status. The InboundTransfer must already be in the processing state.
            """
            return cast(
                "InboundTransfer",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/treasury/inbound_transfers/{id}/fail".format(
                        id=sanitize_id(id)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def fail_async(
            id: str, **params: Unpack["InboundTransferFailParams"]
        ) -> "InboundTransfer":
            """
            Transitions a test mode created InboundTransfer to the failed status. The InboundTransfer must already be in the processing state.
            """
            ...

        @overload
        async def fail_async(
            self, **params: Unpack["InboundTransferFailParams"]
        ) -> "InboundTransfer":
            """
            Transitions a test mode created InboundTransfer to the failed status. The InboundTransfer must already be in the processing state.
            """
            ...

        @class_method_variant("_cls_fail_async")
        async def fail_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["InboundTransferFailParams"]
        ) -> "InboundTransfer":
            """
            Transitions a test mode created InboundTransfer to the failed status. The InboundTransfer must already be in the processing state.
            """
            return cast(
                "InboundTransfer",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/treasury/inbound_transfers/{id}/fail".format(
                        id=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_return_inbound_transfer(
            cls,
            id: str,
            **params: Unpack["InboundTransferReturnInboundTransferParams"],
        ) -> "InboundTransfer":
            """
            Marks the test mode InboundTransfer object as returned and links the InboundTransfer to a ReceivedDebit. The InboundTransfer must already be in the succeeded state.
            """
            return cast(
                "InboundTransfer",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/treasury/inbound_transfers/{id}/return".format(
                        id=sanitize_id(id)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def return_inbound_transfer(
            id: str,
            **params: Unpack["InboundTransferReturnInboundTransferParams"],
        ) -> "InboundTransfer":
            """
            Marks the test mode InboundTransfer object as returned and links the InboundTransfer to a ReceivedDebit. The InboundTransfer must already be in the succeeded state.
            """
            ...

        @overload
        def return_inbound_transfer(
            self,
            **params: Unpack["InboundTransferReturnInboundTransferParams"],
        ) -> "InboundTransfer":
            """
            Marks the test mode InboundTransfer object as returned and links the InboundTransfer to a ReceivedDebit. The InboundTransfer must already be in the succeeded state.
            """
            ...

        @class_method_variant("_cls_return_inbound_transfer")
        def return_inbound_transfer(  # pyright: ignore[reportGeneralTypeIssues]
            self,
            **params: Unpack["InboundTransferReturnInboundTransferParams"],
        ) -> "InboundTransfer":
            """
            Marks the test mode InboundTransfer object as returned and links the InboundTransfer to a ReceivedDebit. The InboundTransfer must already be in the succeeded state.
            """
            return cast(
                "InboundTransfer",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/treasury/inbound_transfers/{id}/return".format(
                        id=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_return_inbound_transfer_async(
            cls,
            id: str,
            **params: Unpack["InboundTransferReturnInboundTransferParams"],
        ) -> "InboundTransfer":
            """
            Marks the test mode InboundTransfer object as returned and links the InboundTransfer to a ReceivedDebit. The InboundTransfer must already be in the succeeded state.
            """
            return cast(
                "InboundTransfer",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/treasury/inbound_transfers/{id}/return".format(
                        id=sanitize_id(id)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def return_inbound_transfer_async(
            id: str,
            **params: Unpack["InboundTransferReturnInboundTransferParams"],
        ) -> "InboundTransfer":
            """
            Marks the test mode InboundTransfer object as returned and links the InboundTransfer to a ReceivedDebit. The InboundTransfer must already be in the succeeded state.
            """
            ...

        @overload
        async def return_inbound_transfer_async(
            self,
            **params: Unpack["InboundTransferReturnInboundTransferParams"],
        ) -> "InboundTransfer":
            """
            Marks the test mode InboundTransfer object as returned and links the InboundTransfer to a ReceivedDebit. The InboundTransfer must already be in the succeeded state.
            """
            ...

        @class_method_variant("_cls_return_inbound_transfer_async")
        async def return_inbound_transfer_async(  # pyright: ignore[reportGeneralTypeIssues]
            self,
            **params: Unpack["InboundTransferReturnInboundTransferParams"],
        ) -> "InboundTransfer":
            """
            Marks the test mode InboundTransfer object as returned and links the InboundTransfer to a ReceivedDebit. The InboundTransfer must already be in the succeeded state.
            """
            return cast(
                "InboundTransfer",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/treasury/inbound_transfers/{id}/return".format(
                        id=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_succeed(
            cls, id: str, **params: Unpack["InboundTransferSucceedParams"]
        ) -> "InboundTransfer":
            """
            Transitions a test mode created InboundTransfer to the succeeded status. The InboundTransfer must already be in the processing state.
            """
            return cast(
                "InboundTransfer",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/treasury/inbound_transfers/{id}/succeed".format(
                        id=sanitize_id(id)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def succeed(
            id: str, **params: Unpack["InboundTransferSucceedParams"]
        ) -> "InboundTransfer":
            """
            Transitions a test mode created InboundTransfer to the succeeded status. The InboundTransfer must already be in the processing state.
            """
            ...

        @overload
        def succeed(
            self, **params: Unpack["InboundTransferSucceedParams"]
        ) -> "InboundTransfer":
            """
            Transitions a test mode created InboundTransfer to the succeeded status. The InboundTransfer must already be in the processing state.
            """
            ...

        @class_method_variant("_cls_succeed")
        def succeed(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["InboundTransferSucceedParams"]
        ) -> "InboundTransfer":
            """
            Transitions a test mode created InboundTransfer to the succeeded status. The InboundTransfer must already be in the processing state.
            """
            return cast(
                "InboundTransfer",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/treasury/inbound_transfers/{id}/succeed".format(
                        id=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_succeed_async(
            cls, id: str, **params: Unpack["InboundTransferSucceedParams"]
        ) -> "InboundTransfer":
            """
            Transitions a test mode created InboundTransfer to the succeeded status. The InboundTransfer must already be in the processing state.
            """
            return cast(
                "InboundTransfer",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/treasury/inbound_transfers/{id}/succeed".format(
                        id=sanitize_id(id)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def succeed_async(
            id: str, **params: Unpack["InboundTransferSucceedParams"]
        ) -> "InboundTransfer":
            """
            Transitions a test mode created InboundTransfer to the succeeded status. The InboundTransfer must already be in the processing state.
            """
            ...

        @overload
        async def succeed_async(
            self, **params: Unpack["InboundTransferSucceedParams"]
        ) -> "InboundTransfer":
            """
            Transitions a test mode created InboundTransfer to the succeeded status. The InboundTransfer must already be in the processing state.
            """
            ...

        @class_method_variant("_cls_succeed_async")
        async def succeed_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["InboundTransferSucceedParams"]
        ) -> "InboundTransfer":
            """
            Transitions a test mode created InboundTransfer to the succeeded status. The InboundTransfer must already be in the processing state.
            """
            return cast(
                "InboundTransfer",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/treasury/inbound_transfers/{id}/succeed".format(
                        id=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

    @property
    def test_helpers(self):
        return self.TestHelpers(self)

    _inner_class_types = {
        "failure_details": FailureDetails,
        "linked_flows": LinkedFlows,
        "origin_payment_method_details": OriginPaymentMethodDetails,
        "status_transitions": StatusTransitions,
    }


InboundTransfer.TestHelpers._resource_cls = InboundTransfer
