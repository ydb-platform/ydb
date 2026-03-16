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
    from stripe.params.treasury._outbound_transfer_cancel_params import (
        OutboundTransferCancelParams,
    )
    from stripe.params.treasury._outbound_transfer_create_params import (
        OutboundTransferCreateParams,
    )
    from stripe.params.treasury._outbound_transfer_fail_params import (
        OutboundTransferFailParams,
    )
    from stripe.params.treasury._outbound_transfer_list_params import (
        OutboundTransferListParams,
    )
    from stripe.params.treasury._outbound_transfer_post_params import (
        OutboundTransferPostParams,
    )
    from stripe.params.treasury._outbound_transfer_retrieve_params import (
        OutboundTransferRetrieveParams,
    )
    from stripe.params.treasury._outbound_transfer_return_outbound_transfer_params import (
        OutboundTransferReturnOutboundTransferParams,
    )
    from stripe.params.treasury._outbound_transfer_update_params import (
        OutboundTransferUpdateParams,
    )
    from stripe.treasury._transaction import Transaction


class OutboundTransfer(
    CreateableAPIResource["OutboundTransfer"],
    ListableAPIResource["OutboundTransfer"],
):
    """
    Use [OutboundTransfers](https://docs.stripe.com/docs/treasury/moving-money/financial-accounts/out-of/outbound-transfers) to transfer funds from a [FinancialAccount](https://api.stripe.com#financial_accounts) to a PaymentMethod belonging to the same entity. To send funds to a different party, use [OutboundPayments](https://api.stripe.com#outbound_payments) instead. You can send funds over ACH rails or through a domestic wire transfer to a user's own external bank account.

    Simulate OutboundTransfer state changes with the `/v1/test_helpers/treasury/outbound_transfers` endpoints. These methods can only be called on test mode objects.

    Related guide: [Moving money with Treasury using OutboundTransfer objects](https://docs.stripe.com/docs/treasury/moving-money/financial-accounts/out-of/outbound-transfers)
    """

    OBJECT_NAME: ClassVar[Literal["treasury.outbound_transfer"]] = (
        "treasury.outbound_transfer"
    )

    class DestinationPaymentMethodDetails(StripeObject):
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
            Token of the FinancialAccount.
            """
            network: Literal["stripe"]
            """
            The rails used to send funds.
            """

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
            network: Literal["ach", "us_domestic_wire"]
            """
            The network rails used. See the [docs](https://docs.stripe.com/treasury/money-movement/timelines) to learn more about money movement timelines for each network type.
            """
            routing_number: Optional[str]
            """
            Routing number of the bank account.
            """

        billing_details: BillingDetails
        financial_account: Optional[FinancialAccount]
        type: Literal["financial_account", "us_bank_account"]
        """
        The type of the payment method used in the OutboundTransfer.
        """
        us_bank_account: Optional[UsBankAccount]
        _inner_class_types = {
            "billing_details": BillingDetails,
            "financial_account": FinancialAccount,
            "us_bank_account": UsBankAccount,
        }

    class ReturnedDetails(StripeObject):
        code: Literal[
            "account_closed",
            "account_frozen",
            "bank_account_restricted",
            "bank_ownership_changed",
            "declined",
            "incorrect_account_holder_name",
            "invalid_account_number",
            "invalid_currency",
            "no_account",
            "other",
        ]
        """
        Reason for the return.
        """
        transaction: ExpandableField["Transaction"]
        """
        The Transaction associated with this object.
        """

    class StatusTransitions(StripeObject):
        canceled_at: Optional[int]
        """
        Timestamp describing when an OutboundTransfer changed status to `canceled`
        """
        failed_at: Optional[int]
        """
        Timestamp describing when an OutboundTransfer changed status to `failed`
        """
        posted_at: Optional[int]
        """
        Timestamp describing when an OutboundTransfer changed status to `posted`
        """
        returned_at: Optional[int]
        """
        Timestamp describing when an OutboundTransfer changed status to `returned`
        """

    class TrackingDetails(StripeObject):
        class Ach(StripeObject):
            trace_id: str
            """
            ACH trace ID of the OutboundTransfer for transfers sent over the `ach` network.
            """

        class UsDomesticWire(StripeObject):
            chips: Optional[str]
            """
            CHIPS System Sequence Number (SSN) of the OutboundTransfer for transfers sent over the `us_domestic_wire` network.
            """
            imad: Optional[str]
            """
            IMAD of the OutboundTransfer for transfers sent over the `us_domestic_wire` network.
            """
            omad: Optional[str]
            """
            OMAD of the OutboundTransfer for transfers sent over the `us_domestic_wire` network.
            """

        ach: Optional[Ach]
        type: Literal["ach", "us_domestic_wire"]
        """
        The US bank account network used to send funds.
        """
        us_domestic_wire: Optional[UsDomesticWire]
        _inner_class_types = {"ach": Ach, "us_domestic_wire": UsDomesticWire}

    amount: int
    """
    Amount (in cents) transferred.
    """
    cancelable: bool
    """
    Returns `true` if the object can be canceled, and `false` otherwise.
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
    destination_payment_method: Optional[str]
    """
    The PaymentMethod used as the payment instrument for an OutboundTransfer.
    """
    destination_payment_method_details: DestinationPaymentMethodDetails
    expected_arrival_date: int
    """
    The date when funds are expected to arrive in the destination account.
    """
    financial_account: str
    """
    The FinancialAccount that funds were pulled from.
    """
    hosted_regulatory_receipt_url: Optional[str]
    """
    A [hosted transaction receipt](https://docs.stripe.com/treasury/moving-money/regulatory-receipts) URL that is provided when money movement is considered regulated under Stripe's money transmission licenses.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["treasury.outbound_transfer"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    returned_details: Optional[ReturnedDetails]
    """
    Details about a returned OutboundTransfer. Only set when the status is `returned`.
    """
    statement_descriptor: str
    """
    Information about the OutboundTransfer to be sent to the recipient account.
    """
    status: Literal["canceled", "failed", "posted", "processing", "returned"]
    """
    Current status of the OutboundTransfer: `processing`, `failed`, `canceled`, `posted`, `returned`. An OutboundTransfer is `processing` if it has been created and is pending. The status changes to `posted` once the OutboundTransfer has been "confirmed" and funds have left the account, or to `failed` or `canceled`. If an OutboundTransfer fails to arrive at its destination, its status will change to `returned`.
    """
    status_transitions: StatusTransitions
    tracking_details: Optional[TrackingDetails]
    """
    Details about network-specific tracking information if available.
    """
    transaction: ExpandableField["Transaction"]
    """
    The Transaction associated with this object.
    """

    @classmethod
    def _cls_cancel(
        cls,
        outbound_transfer: str,
        **params: Unpack["OutboundTransferCancelParams"],
    ) -> "OutboundTransfer":
        """
        An OutboundTransfer can be canceled if the funds have not yet been paid out.
        """
        return cast(
            "OutboundTransfer",
            cls._static_request(
                "post",
                "/v1/treasury/outbound_transfers/{outbound_transfer}/cancel".format(
                    outbound_transfer=sanitize_id(outbound_transfer)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def cancel(
        outbound_transfer: str,
        **params: Unpack["OutboundTransferCancelParams"],
    ) -> "OutboundTransfer":
        """
        An OutboundTransfer can be canceled if the funds have not yet been paid out.
        """
        ...

    @overload
    def cancel(
        self, **params: Unpack["OutboundTransferCancelParams"]
    ) -> "OutboundTransfer":
        """
        An OutboundTransfer can be canceled if the funds have not yet been paid out.
        """
        ...

    @class_method_variant("_cls_cancel")
    def cancel(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["OutboundTransferCancelParams"]
    ) -> "OutboundTransfer":
        """
        An OutboundTransfer can be canceled if the funds have not yet been paid out.
        """
        return cast(
            "OutboundTransfer",
            self._request(
                "post",
                "/v1/treasury/outbound_transfers/{outbound_transfer}/cancel".format(
                    outbound_transfer=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_cancel_async(
        cls,
        outbound_transfer: str,
        **params: Unpack["OutboundTransferCancelParams"],
    ) -> "OutboundTransfer":
        """
        An OutboundTransfer can be canceled if the funds have not yet been paid out.
        """
        return cast(
            "OutboundTransfer",
            await cls._static_request_async(
                "post",
                "/v1/treasury/outbound_transfers/{outbound_transfer}/cancel".format(
                    outbound_transfer=sanitize_id(outbound_transfer)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def cancel_async(
        outbound_transfer: str,
        **params: Unpack["OutboundTransferCancelParams"],
    ) -> "OutboundTransfer":
        """
        An OutboundTransfer can be canceled if the funds have not yet been paid out.
        """
        ...

    @overload
    async def cancel_async(
        self, **params: Unpack["OutboundTransferCancelParams"]
    ) -> "OutboundTransfer":
        """
        An OutboundTransfer can be canceled if the funds have not yet been paid out.
        """
        ...

    @class_method_variant("_cls_cancel_async")
    async def cancel_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["OutboundTransferCancelParams"]
    ) -> "OutboundTransfer":
        """
        An OutboundTransfer can be canceled if the funds have not yet been paid out.
        """
        return cast(
            "OutboundTransfer",
            await self._request_async(
                "post",
                "/v1/treasury/outbound_transfers/{outbound_transfer}/cancel".format(
                    outbound_transfer=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(
        cls, **params: Unpack["OutboundTransferCreateParams"]
    ) -> "OutboundTransfer":
        """
        Creates an OutboundTransfer.
        """
        return cast(
            "OutboundTransfer",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["OutboundTransferCreateParams"]
    ) -> "OutboundTransfer":
        """
        Creates an OutboundTransfer.
        """
        return cast(
            "OutboundTransfer",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["OutboundTransferListParams"]
    ) -> ListObject["OutboundTransfer"]:
        """
        Returns a list of OutboundTransfers sent from the specified FinancialAccount.
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
        cls, **params: Unpack["OutboundTransferListParams"]
    ) -> ListObject["OutboundTransfer"]:
        """
        Returns a list of OutboundTransfers sent from the specified FinancialAccount.
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
        cls, id: str, **params: Unpack["OutboundTransferRetrieveParams"]
    ) -> "OutboundTransfer":
        """
        Retrieves the details of an existing OutboundTransfer by passing the unique OutboundTransfer ID from either the OutboundTransfer creation request or OutboundTransfer list.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["OutboundTransferRetrieveParams"]
    ) -> "OutboundTransfer":
        """
        Retrieves the details of an existing OutboundTransfer by passing the unique OutboundTransfer ID from either the OutboundTransfer creation request or OutboundTransfer list.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    class TestHelpers(APIResourceTestHelpers["OutboundTransfer"]):
        _resource_cls: Type["OutboundTransfer"]

        @classmethod
        def _cls_fail(
            cls,
            outbound_transfer: str,
            **params: Unpack["OutboundTransferFailParams"],
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the failed status. The OutboundTransfer must already be in the processing state.
            """
            return cast(
                "OutboundTransfer",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/fail".format(
                        outbound_transfer=sanitize_id(outbound_transfer)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def fail(
            outbound_transfer: str,
            **params: Unpack["OutboundTransferFailParams"],
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the failed status. The OutboundTransfer must already be in the processing state.
            """
            ...

        @overload
        def fail(
            self, **params: Unpack["OutboundTransferFailParams"]
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the failed status. The OutboundTransfer must already be in the processing state.
            """
            ...

        @class_method_variant("_cls_fail")
        def fail(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["OutboundTransferFailParams"]
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the failed status. The OutboundTransfer must already be in the processing state.
            """
            return cast(
                "OutboundTransfer",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/fail".format(
                        outbound_transfer=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_fail_async(
            cls,
            outbound_transfer: str,
            **params: Unpack["OutboundTransferFailParams"],
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the failed status. The OutboundTransfer must already be in the processing state.
            """
            return cast(
                "OutboundTransfer",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/fail".format(
                        outbound_transfer=sanitize_id(outbound_transfer)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def fail_async(
            outbound_transfer: str,
            **params: Unpack["OutboundTransferFailParams"],
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the failed status. The OutboundTransfer must already be in the processing state.
            """
            ...

        @overload
        async def fail_async(
            self, **params: Unpack["OutboundTransferFailParams"]
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the failed status. The OutboundTransfer must already be in the processing state.
            """
            ...

        @class_method_variant("_cls_fail_async")
        async def fail_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["OutboundTransferFailParams"]
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the failed status. The OutboundTransfer must already be in the processing state.
            """
            return cast(
                "OutboundTransfer",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/fail".format(
                        outbound_transfer=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_post(
            cls,
            outbound_transfer: str,
            **params: Unpack["OutboundTransferPostParams"],
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the posted status. The OutboundTransfer must already be in the processing state.
            """
            return cast(
                "OutboundTransfer",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/post".format(
                        outbound_transfer=sanitize_id(outbound_transfer)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def post(
            outbound_transfer: str,
            **params: Unpack["OutboundTransferPostParams"],
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the posted status. The OutboundTransfer must already be in the processing state.
            """
            ...

        @overload
        def post(
            self, **params: Unpack["OutboundTransferPostParams"]
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the posted status. The OutboundTransfer must already be in the processing state.
            """
            ...

        @class_method_variant("_cls_post")
        def post(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["OutboundTransferPostParams"]
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the posted status. The OutboundTransfer must already be in the processing state.
            """
            return cast(
                "OutboundTransfer",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/post".format(
                        outbound_transfer=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_post_async(
            cls,
            outbound_transfer: str,
            **params: Unpack["OutboundTransferPostParams"],
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the posted status. The OutboundTransfer must already be in the processing state.
            """
            return cast(
                "OutboundTransfer",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/post".format(
                        outbound_transfer=sanitize_id(outbound_transfer)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def post_async(
            outbound_transfer: str,
            **params: Unpack["OutboundTransferPostParams"],
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the posted status. The OutboundTransfer must already be in the processing state.
            """
            ...

        @overload
        async def post_async(
            self, **params: Unpack["OutboundTransferPostParams"]
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the posted status. The OutboundTransfer must already be in the processing state.
            """
            ...

        @class_method_variant("_cls_post_async")
        async def post_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["OutboundTransferPostParams"]
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the posted status. The OutboundTransfer must already be in the processing state.
            """
            return cast(
                "OutboundTransfer",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/post".format(
                        outbound_transfer=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_return_outbound_transfer(
            cls,
            outbound_transfer: str,
            **params: Unpack["OutboundTransferReturnOutboundTransferParams"],
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the returned status. The OutboundTransfer must already be in the processing state.
            """
            return cast(
                "OutboundTransfer",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/return".format(
                        outbound_transfer=sanitize_id(outbound_transfer)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def return_outbound_transfer(
            outbound_transfer: str,
            **params: Unpack["OutboundTransferReturnOutboundTransferParams"],
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the returned status. The OutboundTransfer must already be in the processing state.
            """
            ...

        @overload
        def return_outbound_transfer(
            self,
            **params: Unpack["OutboundTransferReturnOutboundTransferParams"],
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the returned status. The OutboundTransfer must already be in the processing state.
            """
            ...

        @class_method_variant("_cls_return_outbound_transfer")
        def return_outbound_transfer(  # pyright: ignore[reportGeneralTypeIssues]
            self,
            **params: Unpack["OutboundTransferReturnOutboundTransferParams"],
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the returned status. The OutboundTransfer must already be in the processing state.
            """
            return cast(
                "OutboundTransfer",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/return".format(
                        outbound_transfer=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_return_outbound_transfer_async(
            cls,
            outbound_transfer: str,
            **params: Unpack["OutboundTransferReturnOutboundTransferParams"],
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the returned status. The OutboundTransfer must already be in the processing state.
            """
            return cast(
                "OutboundTransfer",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/return".format(
                        outbound_transfer=sanitize_id(outbound_transfer)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def return_outbound_transfer_async(
            outbound_transfer: str,
            **params: Unpack["OutboundTransferReturnOutboundTransferParams"],
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the returned status. The OutboundTransfer must already be in the processing state.
            """
            ...

        @overload
        async def return_outbound_transfer_async(
            self,
            **params: Unpack["OutboundTransferReturnOutboundTransferParams"],
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the returned status. The OutboundTransfer must already be in the processing state.
            """
            ...

        @class_method_variant("_cls_return_outbound_transfer_async")
        async def return_outbound_transfer_async(  # pyright: ignore[reportGeneralTypeIssues]
            self,
            **params: Unpack["OutboundTransferReturnOutboundTransferParams"],
        ) -> "OutboundTransfer":
            """
            Transitions a test mode created OutboundTransfer to the returned status. The OutboundTransfer must already be in the processing state.
            """
            return cast(
                "OutboundTransfer",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}/return".format(
                        outbound_transfer=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_update(
            cls,
            outbound_transfer: str,
            **params: Unpack["OutboundTransferUpdateParams"],
        ) -> "OutboundTransfer":
            """
            Updates a test mode created OutboundTransfer with tracking details. The OutboundTransfer must not be cancelable, and cannot be in the canceled or failed states.
            """
            return cast(
                "OutboundTransfer",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}".format(
                        outbound_transfer=sanitize_id(outbound_transfer)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def update(
            outbound_transfer: str,
            **params: Unpack["OutboundTransferUpdateParams"],
        ) -> "OutboundTransfer":
            """
            Updates a test mode created OutboundTransfer with tracking details. The OutboundTransfer must not be cancelable, and cannot be in the canceled or failed states.
            """
            ...

        @overload
        def update(
            self, **params: Unpack["OutboundTransferUpdateParams"]
        ) -> "OutboundTransfer":
            """
            Updates a test mode created OutboundTransfer with tracking details. The OutboundTransfer must not be cancelable, and cannot be in the canceled or failed states.
            """
            ...

        @class_method_variant("_cls_update")
        def update(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["OutboundTransferUpdateParams"]
        ) -> "OutboundTransfer":
            """
            Updates a test mode created OutboundTransfer with tracking details. The OutboundTransfer must not be cancelable, and cannot be in the canceled or failed states.
            """
            return cast(
                "OutboundTransfer",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}".format(
                        outbound_transfer=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_update_async(
            cls,
            outbound_transfer: str,
            **params: Unpack["OutboundTransferUpdateParams"],
        ) -> "OutboundTransfer":
            """
            Updates a test mode created OutboundTransfer with tracking details. The OutboundTransfer must not be cancelable, and cannot be in the canceled or failed states.
            """
            return cast(
                "OutboundTransfer",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}".format(
                        outbound_transfer=sanitize_id(outbound_transfer)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def update_async(
            outbound_transfer: str,
            **params: Unpack["OutboundTransferUpdateParams"],
        ) -> "OutboundTransfer":
            """
            Updates a test mode created OutboundTransfer with tracking details. The OutboundTransfer must not be cancelable, and cannot be in the canceled or failed states.
            """
            ...

        @overload
        async def update_async(
            self, **params: Unpack["OutboundTransferUpdateParams"]
        ) -> "OutboundTransfer":
            """
            Updates a test mode created OutboundTransfer with tracking details. The OutboundTransfer must not be cancelable, and cannot be in the canceled or failed states.
            """
            ...

        @class_method_variant("_cls_update_async")
        async def update_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["OutboundTransferUpdateParams"]
        ) -> "OutboundTransfer":
            """
            Updates a test mode created OutboundTransfer with tracking details. The OutboundTransfer must not be cancelable, and cannot be in the canceled or failed states.
            """
            return cast(
                "OutboundTransfer",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/treasury/outbound_transfers/{outbound_transfer}".format(
                        outbound_transfer=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

    @property
    def test_helpers(self):
        return self.TestHelpers(self)

    _inner_class_types = {
        "destination_payment_method_details": DestinationPaymentMethodDetails,
        "returned_details": ReturnedDetails,
        "status_transitions": StatusTransitions,
        "tracking_details": TrackingDetails,
    }


OutboundTransfer.TestHelpers._resource_cls = OutboundTransfer
