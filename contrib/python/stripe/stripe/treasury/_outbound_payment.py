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
    from stripe.params.treasury._outbound_payment_cancel_params import (
        OutboundPaymentCancelParams,
    )
    from stripe.params.treasury._outbound_payment_create_params import (
        OutboundPaymentCreateParams,
    )
    from stripe.params.treasury._outbound_payment_fail_params import (
        OutboundPaymentFailParams,
    )
    from stripe.params.treasury._outbound_payment_list_params import (
        OutboundPaymentListParams,
    )
    from stripe.params.treasury._outbound_payment_post_params import (
        OutboundPaymentPostParams,
    )
    from stripe.params.treasury._outbound_payment_retrieve_params import (
        OutboundPaymentRetrieveParams,
    )
    from stripe.params.treasury._outbound_payment_return_outbound_payment_params import (
        OutboundPaymentReturnOutboundPaymentParams,
    )
    from stripe.params.treasury._outbound_payment_update_params import (
        OutboundPaymentUpdateParams,
    )
    from stripe.treasury._transaction import Transaction


class OutboundPayment(
    CreateableAPIResource["OutboundPayment"],
    ListableAPIResource["OutboundPayment"],
):
    """
    Use [OutboundPayments](https://docs.stripe.com/docs/treasury/moving-money/financial-accounts/out-of/outbound-payments) to send funds to another party's external bank account or [FinancialAccount](https://api.stripe.com#financial_accounts). To send money to an account belonging to the same user, use an [OutboundTransfer](https://api.stripe.com#outbound_transfers).

    Simulate OutboundPayment state changes with the `/v1/test_helpers/treasury/outbound_payments` endpoints. These methods can only be called on test mode objects.

    Related guide: [Moving money with Treasury using OutboundPayment objects](https://docs.stripe.com/docs/treasury/moving-money/financial-accounts/out-of/outbound-payments)
    """

    OBJECT_NAME: ClassVar[Literal["treasury.outbound_payment"]] = (
        "treasury.outbound_payment"
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
        The type of the payment method used in the OutboundPayment.
        """
        us_bank_account: Optional[UsBankAccount]
        _inner_class_types = {
            "billing_details": BillingDetails,
            "financial_account": FinancialAccount,
            "us_bank_account": UsBankAccount,
        }

    class EndUserDetails(StripeObject):
        ip_address: Optional[str]
        """
        IP address of the user initiating the OutboundPayment. Set if `present` is set to `true`. IP address collection is required for risk and compliance reasons. This will be used to help determine if the OutboundPayment is authorized or should be blocked.
        """
        present: bool
        """
        `true` if the OutboundPayment creation request is being made on behalf of an end user by a platform. Otherwise, `false`.
        """

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
        Timestamp describing when an OutboundPayment changed status to `canceled`.
        """
        failed_at: Optional[int]
        """
        Timestamp describing when an OutboundPayment changed status to `failed`.
        """
        posted_at: Optional[int]
        """
        Timestamp describing when an OutboundPayment changed status to `posted`.
        """
        returned_at: Optional[int]
        """
        Timestamp describing when an OutboundPayment changed status to `returned`.
        """

    class TrackingDetails(StripeObject):
        class Ach(StripeObject):
            trace_id: str
            """
            ACH trace ID of the OutboundPayment for payments sent over the `ach` network.
            """

        class UsDomesticWire(StripeObject):
            chips: Optional[str]
            """
            CHIPS System Sequence Number (SSN) of the OutboundPayment for payments sent over the `us_domestic_wire` network.
            """
            imad: Optional[str]
            """
            IMAD of the OutboundPayment for payments sent over the `us_domestic_wire` network.
            """
            omad: Optional[str]
            """
            OMAD of the OutboundPayment for payments sent over the `us_domestic_wire` network.
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
    customer: Optional[str]
    """
    ID of the [customer](https://docs.stripe.com/api/customers) to whom an OutboundPayment is sent.
    """
    description: Optional[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    destination_payment_method: Optional[str]
    """
    The PaymentMethod via which an OutboundPayment is sent. This field can be empty if the OutboundPayment was created using `destination_payment_method_data`.
    """
    destination_payment_method_details: Optional[
        DestinationPaymentMethodDetails
    ]
    """
    Details about the PaymentMethod for an OutboundPayment.
    """
    end_user_details: Optional[EndUserDetails]
    """
    Details about the end user.
    """
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
    object: Literal["treasury.outbound_payment"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    returned_details: Optional[ReturnedDetails]
    """
    Details about a returned OutboundPayment. Only set when the status is `returned`.
    """
    statement_descriptor: str
    """
    The description that appears on the receiving end for an OutboundPayment (for example, bank statement for external bank transfer).
    """
    status: Literal["canceled", "failed", "posted", "processing", "returned"]
    """
    Current status of the OutboundPayment: `processing`, `failed`, `posted`, `returned`, `canceled`. An OutboundPayment is `processing` if it has been created and is pending. The status changes to `posted` once the OutboundPayment has been "confirmed" and funds have left the account, or to `failed` or `canceled`. If an OutboundPayment fails to arrive at its destination, its status will change to `returned`.
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
        cls, id: str, **params: Unpack["OutboundPaymentCancelParams"]
    ) -> "OutboundPayment":
        """
        Cancel an OutboundPayment.
        """
        return cast(
            "OutboundPayment",
            cls._static_request(
                "post",
                "/v1/treasury/outbound_payments/{id}/cancel".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def cancel(
        id: str, **params: Unpack["OutboundPaymentCancelParams"]
    ) -> "OutboundPayment":
        """
        Cancel an OutboundPayment.
        """
        ...

    @overload
    def cancel(
        self, **params: Unpack["OutboundPaymentCancelParams"]
    ) -> "OutboundPayment":
        """
        Cancel an OutboundPayment.
        """
        ...

    @class_method_variant("_cls_cancel")
    def cancel(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["OutboundPaymentCancelParams"]
    ) -> "OutboundPayment":
        """
        Cancel an OutboundPayment.
        """
        return cast(
            "OutboundPayment",
            self._request(
                "post",
                "/v1/treasury/outbound_payments/{id}/cancel".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_cancel_async(
        cls, id: str, **params: Unpack["OutboundPaymentCancelParams"]
    ) -> "OutboundPayment":
        """
        Cancel an OutboundPayment.
        """
        return cast(
            "OutboundPayment",
            await cls._static_request_async(
                "post",
                "/v1/treasury/outbound_payments/{id}/cancel".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def cancel_async(
        id: str, **params: Unpack["OutboundPaymentCancelParams"]
    ) -> "OutboundPayment":
        """
        Cancel an OutboundPayment.
        """
        ...

    @overload
    async def cancel_async(
        self, **params: Unpack["OutboundPaymentCancelParams"]
    ) -> "OutboundPayment":
        """
        Cancel an OutboundPayment.
        """
        ...

    @class_method_variant("_cls_cancel_async")
    async def cancel_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["OutboundPaymentCancelParams"]
    ) -> "OutboundPayment":
        """
        Cancel an OutboundPayment.
        """
        return cast(
            "OutboundPayment",
            await self._request_async(
                "post",
                "/v1/treasury/outbound_payments/{id}/cancel".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(
        cls, **params: Unpack["OutboundPaymentCreateParams"]
    ) -> "OutboundPayment":
        """
        Creates an OutboundPayment.
        """
        return cast(
            "OutboundPayment",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["OutboundPaymentCreateParams"]
    ) -> "OutboundPayment":
        """
        Creates an OutboundPayment.
        """
        return cast(
            "OutboundPayment",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["OutboundPaymentListParams"]
    ) -> ListObject["OutboundPayment"]:
        """
        Returns a list of OutboundPayments sent from the specified FinancialAccount.
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
        cls, **params: Unpack["OutboundPaymentListParams"]
    ) -> ListObject["OutboundPayment"]:
        """
        Returns a list of OutboundPayments sent from the specified FinancialAccount.
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
        cls, id: str, **params: Unpack["OutboundPaymentRetrieveParams"]
    ) -> "OutboundPayment":
        """
        Retrieves the details of an existing OutboundPayment by passing the unique OutboundPayment ID from either the OutboundPayment creation request or OutboundPayment list.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["OutboundPaymentRetrieveParams"]
    ) -> "OutboundPayment":
        """
        Retrieves the details of an existing OutboundPayment by passing the unique OutboundPayment ID from either the OutboundPayment creation request or OutboundPayment list.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    class TestHelpers(APIResourceTestHelpers["OutboundPayment"]):
        _resource_cls: Type["OutboundPayment"]

        @classmethod
        def _cls_fail(
            cls, id: str, **params: Unpack["OutboundPaymentFailParams"]
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the failed status. The OutboundPayment must already be in the processing state.
            """
            return cast(
                "OutboundPayment",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/treasury/outbound_payments/{id}/fail".format(
                        id=sanitize_id(id)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def fail(
            id: str, **params: Unpack["OutboundPaymentFailParams"]
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the failed status. The OutboundPayment must already be in the processing state.
            """
            ...

        @overload
        def fail(
            self, **params: Unpack["OutboundPaymentFailParams"]
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the failed status. The OutboundPayment must already be in the processing state.
            """
            ...

        @class_method_variant("_cls_fail")
        def fail(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["OutboundPaymentFailParams"]
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the failed status. The OutboundPayment must already be in the processing state.
            """
            return cast(
                "OutboundPayment",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/treasury/outbound_payments/{id}/fail".format(
                        id=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_fail_async(
            cls, id: str, **params: Unpack["OutboundPaymentFailParams"]
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the failed status. The OutboundPayment must already be in the processing state.
            """
            return cast(
                "OutboundPayment",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/treasury/outbound_payments/{id}/fail".format(
                        id=sanitize_id(id)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def fail_async(
            id: str, **params: Unpack["OutboundPaymentFailParams"]
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the failed status. The OutboundPayment must already be in the processing state.
            """
            ...

        @overload
        async def fail_async(
            self, **params: Unpack["OutboundPaymentFailParams"]
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the failed status. The OutboundPayment must already be in the processing state.
            """
            ...

        @class_method_variant("_cls_fail_async")
        async def fail_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["OutboundPaymentFailParams"]
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the failed status. The OutboundPayment must already be in the processing state.
            """
            return cast(
                "OutboundPayment",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/treasury/outbound_payments/{id}/fail".format(
                        id=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_post(
            cls, id: str, **params: Unpack["OutboundPaymentPostParams"]
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the posted status. The OutboundPayment must already be in the processing state.
            """
            return cast(
                "OutboundPayment",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/treasury/outbound_payments/{id}/post".format(
                        id=sanitize_id(id)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def post(
            id: str, **params: Unpack["OutboundPaymentPostParams"]
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the posted status. The OutboundPayment must already be in the processing state.
            """
            ...

        @overload
        def post(
            self, **params: Unpack["OutboundPaymentPostParams"]
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the posted status. The OutboundPayment must already be in the processing state.
            """
            ...

        @class_method_variant("_cls_post")
        def post(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["OutboundPaymentPostParams"]
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the posted status. The OutboundPayment must already be in the processing state.
            """
            return cast(
                "OutboundPayment",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/treasury/outbound_payments/{id}/post".format(
                        id=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_post_async(
            cls, id: str, **params: Unpack["OutboundPaymentPostParams"]
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the posted status. The OutboundPayment must already be in the processing state.
            """
            return cast(
                "OutboundPayment",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/treasury/outbound_payments/{id}/post".format(
                        id=sanitize_id(id)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def post_async(
            id: str, **params: Unpack["OutboundPaymentPostParams"]
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the posted status. The OutboundPayment must already be in the processing state.
            """
            ...

        @overload
        async def post_async(
            self, **params: Unpack["OutboundPaymentPostParams"]
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the posted status. The OutboundPayment must already be in the processing state.
            """
            ...

        @class_method_variant("_cls_post_async")
        async def post_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["OutboundPaymentPostParams"]
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the posted status. The OutboundPayment must already be in the processing state.
            """
            return cast(
                "OutboundPayment",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/treasury/outbound_payments/{id}/post".format(
                        id=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_return_outbound_payment(
            cls,
            id: str,
            **params: Unpack["OutboundPaymentReturnOutboundPaymentParams"],
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the returned status. The OutboundPayment must already be in the processing state.
            """
            return cast(
                "OutboundPayment",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/treasury/outbound_payments/{id}/return".format(
                        id=sanitize_id(id)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def return_outbound_payment(
            id: str,
            **params: Unpack["OutboundPaymentReturnOutboundPaymentParams"],
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the returned status. The OutboundPayment must already be in the processing state.
            """
            ...

        @overload
        def return_outbound_payment(
            self,
            **params: Unpack["OutboundPaymentReturnOutboundPaymentParams"],
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the returned status. The OutboundPayment must already be in the processing state.
            """
            ...

        @class_method_variant("_cls_return_outbound_payment")
        def return_outbound_payment(  # pyright: ignore[reportGeneralTypeIssues]
            self,
            **params: Unpack["OutboundPaymentReturnOutboundPaymentParams"],
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the returned status. The OutboundPayment must already be in the processing state.
            """
            return cast(
                "OutboundPayment",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/treasury/outbound_payments/{id}/return".format(
                        id=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_return_outbound_payment_async(
            cls,
            id: str,
            **params: Unpack["OutboundPaymentReturnOutboundPaymentParams"],
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the returned status. The OutboundPayment must already be in the processing state.
            """
            return cast(
                "OutboundPayment",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/treasury/outbound_payments/{id}/return".format(
                        id=sanitize_id(id)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def return_outbound_payment_async(
            id: str,
            **params: Unpack["OutboundPaymentReturnOutboundPaymentParams"],
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the returned status. The OutboundPayment must already be in the processing state.
            """
            ...

        @overload
        async def return_outbound_payment_async(
            self,
            **params: Unpack["OutboundPaymentReturnOutboundPaymentParams"],
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the returned status. The OutboundPayment must already be in the processing state.
            """
            ...

        @class_method_variant("_cls_return_outbound_payment_async")
        async def return_outbound_payment_async(  # pyright: ignore[reportGeneralTypeIssues]
            self,
            **params: Unpack["OutboundPaymentReturnOutboundPaymentParams"],
        ) -> "OutboundPayment":
            """
            Transitions a test mode created OutboundPayment to the returned status. The OutboundPayment must already be in the processing state.
            """
            return cast(
                "OutboundPayment",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/treasury/outbound_payments/{id}/return".format(
                        id=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_update(
            cls, id: str, **params: Unpack["OutboundPaymentUpdateParams"]
        ) -> "OutboundPayment":
            """
            Updates a test mode created OutboundPayment with tracking details. The OutboundPayment must not be cancelable, and cannot be in the canceled or failed states.
            """
            return cast(
                "OutboundPayment",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/treasury/outbound_payments/{id}".format(
                        id=sanitize_id(id)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def update(
            id: str, **params: Unpack["OutboundPaymentUpdateParams"]
        ) -> "OutboundPayment":
            """
            Updates a test mode created OutboundPayment with tracking details. The OutboundPayment must not be cancelable, and cannot be in the canceled or failed states.
            """
            ...

        @overload
        def update(
            self, **params: Unpack["OutboundPaymentUpdateParams"]
        ) -> "OutboundPayment":
            """
            Updates a test mode created OutboundPayment with tracking details. The OutboundPayment must not be cancelable, and cannot be in the canceled or failed states.
            """
            ...

        @class_method_variant("_cls_update")
        def update(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["OutboundPaymentUpdateParams"]
        ) -> "OutboundPayment":
            """
            Updates a test mode created OutboundPayment with tracking details. The OutboundPayment must not be cancelable, and cannot be in the canceled or failed states.
            """
            return cast(
                "OutboundPayment",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/treasury/outbound_payments/{id}".format(
                        id=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_update_async(
            cls, id: str, **params: Unpack["OutboundPaymentUpdateParams"]
        ) -> "OutboundPayment":
            """
            Updates a test mode created OutboundPayment with tracking details. The OutboundPayment must not be cancelable, and cannot be in the canceled or failed states.
            """
            return cast(
                "OutboundPayment",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/treasury/outbound_payments/{id}".format(
                        id=sanitize_id(id)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def update_async(
            id: str, **params: Unpack["OutboundPaymentUpdateParams"]
        ) -> "OutboundPayment":
            """
            Updates a test mode created OutboundPayment with tracking details. The OutboundPayment must not be cancelable, and cannot be in the canceled or failed states.
            """
            ...

        @overload
        async def update_async(
            self, **params: Unpack["OutboundPaymentUpdateParams"]
        ) -> "OutboundPayment":
            """
            Updates a test mode created OutboundPayment with tracking details. The OutboundPayment must not be cancelable, and cannot be in the canceled or failed states.
            """
            ...

        @class_method_variant("_cls_update_async")
        async def update_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["OutboundPaymentUpdateParams"]
        ) -> "OutboundPayment":
            """
            Updates a test mode created OutboundPayment with tracking details. The OutboundPayment must not be cancelable, and cannot be in the canceled or failed states.
            """
            return cast(
                "OutboundPayment",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/treasury/outbound_payments/{id}".format(
                        id=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

    @property
    def test_helpers(self):
        return self.TestHelpers(self)

    _inner_class_types = {
        "destination_payment_method_details": DestinationPaymentMethodDetails,
        "end_user_details": EndUserDetails,
        "returned_details": ReturnedDetails,
        "status_transitions": StatusTransitions,
        "tracking_details": TrackingDetails,
    }


OutboundPayment.TestHelpers._resource_cls = OutboundPayment
