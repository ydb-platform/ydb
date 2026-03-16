# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._test_helpers import APIResourceTestHelpers
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, cast, overload
from typing_extensions import Literal, Type, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._balance_transaction import BalanceTransaction
    from stripe.issuing._card import Card
    from stripe.issuing._cardholder import Cardholder
    from stripe.issuing._token import Token
    from stripe.issuing._transaction import Transaction
    from stripe.params.issuing._authorization_approve_params import (
        AuthorizationApproveParams,
    )
    from stripe.params.issuing._authorization_capture_params import (
        AuthorizationCaptureParams,
    )
    from stripe.params.issuing._authorization_create_params import (
        AuthorizationCreateParams,
    )
    from stripe.params.issuing._authorization_decline_params import (
        AuthorizationDeclineParams,
    )
    from stripe.params.issuing._authorization_expire_params import (
        AuthorizationExpireParams,
    )
    from stripe.params.issuing._authorization_finalize_amount_params import (
        AuthorizationFinalizeAmountParams,
    )
    from stripe.params.issuing._authorization_increment_params import (
        AuthorizationIncrementParams,
    )
    from stripe.params.issuing._authorization_list_params import (
        AuthorizationListParams,
    )
    from stripe.params.issuing._authorization_modify_params import (
        AuthorizationModifyParams,
    )
    from stripe.params.issuing._authorization_respond_params import (
        AuthorizationRespondParams,
    )
    from stripe.params.issuing._authorization_retrieve_params import (
        AuthorizationRetrieveParams,
    )
    from stripe.params.issuing._authorization_reverse_params import (
        AuthorizationReverseParams,
    )


class Authorization(
    ListableAPIResource["Authorization"],
    UpdateableAPIResource["Authorization"],
):
    """
    When an [issued card](https://docs.stripe.com/issuing) is used to make a purchase, an Issuing `Authorization`
    object is created. [Authorizations](https://docs.stripe.com/issuing/purchases/authorizations) must be approved for the
    purchase to be completed successfully.

    Related guide: [Issued card authorizations](https://docs.stripe.com/issuing/purchases/authorizations)
    """

    OBJECT_NAME: ClassVar[Literal["issuing.authorization"]] = (
        "issuing.authorization"
    )

    class AmountDetails(StripeObject):
        atm_fee: Optional[int]
        """
        The fee charged by the ATM for the cash withdrawal.
        """
        cashback_amount: Optional[int]
        """
        The amount of cash requested by the cardholder.
        """

    class Fleet(StripeObject):
        class CardholderPromptData(StripeObject):
            alphanumeric_id: Optional[str]
            """
            [Deprecated] An alphanumeric ID, though typical point of sales only support numeric entry. The card program can be configured to prompt for a vehicle ID, driver ID, or generic ID.
            """
            driver_id: Optional[str]
            """
            Driver ID.
            """
            odometer: Optional[int]
            """
            Odometer reading.
            """
            unspecified_id: Optional[str]
            """
            An alphanumeric ID. This field is used when a vehicle ID, driver ID, or generic ID is entered by the cardholder, but the merchant or card network did not specify the prompt type.
            """
            user_id: Optional[str]
            """
            User ID.
            """
            vehicle_number: Optional[str]
            """
            Vehicle number.
            """

        class ReportedBreakdown(StripeObject):
            class Fuel(StripeObject):
                gross_amount_decimal: Optional[str]
                """
                Gross fuel amount that should equal Fuel Quantity multiplied by Fuel Unit Cost, inclusive of taxes.
                """

            class NonFuel(StripeObject):
                gross_amount_decimal: Optional[str]
                """
                Gross non-fuel amount that should equal the sum of the line items, inclusive of taxes.
                """

            class Tax(StripeObject):
                local_amount_decimal: Optional[str]
                """
                Amount of state or provincial Sales Tax included in the transaction amount. `null` if not reported by merchant or not subject to tax.
                """
                national_amount_decimal: Optional[str]
                """
                Amount of national Sales Tax or VAT included in the transaction amount. `null` if not reported by merchant or not subject to tax.
                """

            fuel: Optional[Fuel]
            """
            Breakdown of fuel portion of the purchase.
            """
            non_fuel: Optional[NonFuel]
            """
            Breakdown of non-fuel portion of the purchase.
            """
            tax: Optional[Tax]
            """
            Information about tax included in this transaction.
            """
            _inner_class_types = {
                "fuel": Fuel,
                "non_fuel": NonFuel,
                "tax": Tax,
            }

        cardholder_prompt_data: Optional[CardholderPromptData]
        """
        Answers to prompts presented to the cardholder at the point of sale. Prompted fields vary depending on the configuration of your physical fleet cards. Typical points of sale support only numeric entry.
        """
        purchase_type: Optional[
            Literal[
                "fuel_and_non_fuel_purchase",
                "fuel_purchase",
                "non_fuel_purchase",
            ]
        ]
        """
        The type of purchase.
        """
        reported_breakdown: Optional[ReportedBreakdown]
        """
        More information about the total amount. Typically this information is received from the merchant after the authorization has been approved and the fuel dispensed. This information is not guaranteed to be accurate as some merchants may provide unreliable data.
        """
        service_type: Optional[
            Literal["full_service", "non_fuel_transaction", "self_service"]
        ]
        """
        The type of fuel service.
        """
        _inner_class_types = {
            "cardholder_prompt_data": CardholderPromptData,
            "reported_breakdown": ReportedBreakdown,
        }

    class FraudChallenge(StripeObject):
        channel: Literal["sms"]
        """
        The method by which the fraud challenge was delivered to the cardholder.
        """
        status: Literal[
            "expired", "pending", "rejected", "undeliverable", "verified"
        ]
        """
        The status of the fraud challenge.
        """
        undeliverable_reason: Optional[
            Literal["no_phone_number", "unsupported_phone_number"]
        ]
        """
        If the challenge is not deliverable, the reason why.
        """

    class Fuel(StripeObject):
        industry_product_code: Optional[str]
        """
        [Conexxus Payment System Product Code](https://www.conexxus.org/conexxus-payment-system-product-codes) identifying the primary fuel product purchased.
        """
        quantity_decimal: Optional[str]
        """
        The quantity of `unit`s of fuel that was dispensed, represented as a decimal string with at most 12 decimal places.
        """
        type: Optional[
            Literal[
                "diesel",
                "other",
                "unleaded_plus",
                "unleaded_regular",
                "unleaded_super",
            ]
        ]
        """
        The type of fuel that was purchased.
        """
        unit: Optional[
            Literal[
                "charging_minute",
                "imperial_gallon",
                "kilogram",
                "kilowatt_hour",
                "liter",
                "other",
                "pound",
                "us_gallon",
            ]
        ]
        """
        The units for `quantity_decimal`.
        """
        unit_cost_decimal: Optional[str]
        """
        The cost in cents per each unit of fuel, represented as a decimal string with at most 12 decimal places.
        """

    class MerchantData(StripeObject):
        category: str
        """
        A categorization of the seller's type of business. See our [merchant categories guide](https://docs.stripe.com/issuing/merchant-categories) for a list of possible values.
        """
        category_code: str
        """
        The merchant category code for the seller's business
        """
        city: Optional[str]
        """
        City where the seller is located
        """
        country: Optional[str]
        """
        Country where the seller is located
        """
        name: Optional[str]
        """
        Name of the seller
        """
        network_id: str
        """
        Identifier assigned to the seller by the card network. Different card networks may assign different network_id fields to the same merchant.
        """
        postal_code: Optional[str]
        """
        Postal code where the seller is located
        """
        state: Optional[str]
        """
        State where the seller is located
        """
        tax_id: Optional[str]
        """
        The seller's tax identification number. Currently populated for French merchants only.
        """
        terminal_id: Optional[str]
        """
        An ID assigned by the seller to the location of the sale.
        """
        url: Optional[str]
        """
        URL provided by the merchant on a 3DS request
        """

    class NetworkData(StripeObject):
        acquiring_institution_id: Optional[str]
        """
        Identifier assigned to the acquirer by the card network. Sometimes this value is not provided by the network; in this case, the value will be `null`.
        """
        system_trace_audit_number: Optional[str]
        """
        The System Trace Audit Number (STAN) is a 6-digit identifier assigned by the acquirer. Prefer `network_data.transaction_id` if present, unless you have special requirements.
        """
        transaction_id: Optional[str]
        """
        Unique identifier for the authorization assigned by the card network used to match subsequent messages, disputes, and transactions.
        """

    class PendingRequest(StripeObject):
        class AmountDetails(StripeObject):
            atm_fee: Optional[int]
            """
            The fee charged by the ATM for the cash withdrawal.
            """
            cashback_amount: Optional[int]
            """
            The amount of cash requested by the cardholder.
            """

        amount: int
        """
        The additional amount Stripe will hold if the authorization is approved, in the card's [currency](https://docs.stripe.com/api#issuing_authorization_object-pending-request-currency) and in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
        """
        amount_details: Optional[AmountDetails]
        """
        Detailed breakdown of amount components. These amounts are denominated in `currency` and in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
        """
        currency: str
        """
        Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        is_amount_controllable: bool
        """
        If set `true`, you may provide [amount](https://docs.stripe.com/api/issuing/authorizations/approve#approve_issuing_authorization-amount) to control how much to hold for the authorization.
        """
        merchant_amount: int
        """
        The amount the merchant is requesting to be authorized in the `merchant_currency`. The amount is in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
        """
        merchant_currency: str
        """
        The local currency the merchant is requesting to authorize.
        """
        network_risk_score: Optional[int]
        """
        The card network's estimate of the likelihood that an authorization is fraudulent. Takes on values between 1 and 99.
        """
        _inner_class_types = {"amount_details": AmountDetails}

    class RequestHistory(StripeObject):
        class AmountDetails(StripeObject):
            atm_fee: Optional[int]
            """
            The fee charged by the ATM for the cash withdrawal.
            """
            cashback_amount: Optional[int]
            """
            The amount of cash requested by the cardholder.
            """

        amount: int
        """
        The `pending_request.amount` at the time of the request, presented in your card's currency and in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal). Stripe held this amount from your account to fund the authorization if the request was approved.
        """
        amount_details: Optional[AmountDetails]
        """
        Detailed breakdown of amount components. These amounts are denominated in `currency` and in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
        """
        approved: bool
        """
        Whether this request was approved.
        """
        authorization_code: Optional[str]
        """
        A code created by Stripe which is shared with the merchant to validate the authorization. This field will be populated if the authorization message was approved. The code typically starts with the letter "S", followed by a six-digit number. For example, "S498162". Please note that the code is not guaranteed to be unique across authorizations.
        """
        created: int
        """
        Time at which the object was created. Measured in seconds since the Unix epoch.
        """
        currency: str
        """
        Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        merchant_amount: int
        """
        The `pending_request.merchant_amount` at the time of the request, presented in the `merchant_currency` and in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
        """
        merchant_currency: str
        """
        The currency that was collected by the merchant and presented to the cardholder for the authorization. Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        network_risk_score: Optional[int]
        """
        The card network's estimate of the likelihood that an authorization is fraudulent. Takes on values between 1 and 99.
        """
        reason: Literal[
            "account_disabled",
            "card_active",
            "card_canceled",
            "card_expired",
            "card_inactive",
            "cardholder_blocked",
            "cardholder_inactive",
            "cardholder_verification_required",
            "insecure_authorization_method",
            "insufficient_funds",
            "network_fallback",
            "not_allowed",
            "pin_blocked",
            "spending_controls",
            "suspected_fraud",
            "verification_failed",
            "webhook_approved",
            "webhook_declined",
            "webhook_error",
            "webhook_timeout",
        ]
        """
        When an authorization is approved or declined by you or by Stripe, this field provides additional detail on the reason for the outcome.
        """
        reason_message: Optional[str]
        """
        If the `request_history.reason` is `webhook_error` because the direct webhook response is invalid (for example, parsing errors or missing parameters), we surface a more detailed error message via this field.
        """
        requested_at: Optional[int]
        """
        Time when the card network received an authorization request from the acquirer in UTC. Referred to by networks as transmission time.
        """
        _inner_class_types = {"amount_details": AmountDetails}

    class Treasury(StripeObject):
        received_credits: List[str]
        """
        The array of [ReceivedCredits](https://docs.stripe.com/api/treasury/received_credits) associated with this authorization
        """
        received_debits: List[str]
        """
        The array of [ReceivedDebits](https://docs.stripe.com/api/treasury/received_debits) associated with this authorization
        """
        transaction: Optional[str]
        """
        The Treasury [Transaction](https://docs.stripe.com/api/treasury/transactions) associated with this authorization
        """

    class VerificationData(StripeObject):
        class AuthenticationExemption(StripeObject):
            claimed_by: Literal["acquirer", "issuer"]
            """
            The entity that requested the exemption, either the acquiring merchant or the Issuing user.
            """
            type: Literal[
                "low_value_transaction", "transaction_risk_analysis", "unknown"
            ]
            """
            The specific exemption claimed for this authorization.
            """

        class ThreeDSecure(StripeObject):
            result: Literal[
                "attempt_acknowledged", "authenticated", "failed", "required"
            ]
            """
            The outcome of the 3D Secure authentication request.
            """

        address_line1_check: Literal["match", "mismatch", "not_provided"]
        """
        Whether the cardholder provided an address first line and if it matched the cardholder's `billing.address.line1`.
        """
        address_postal_code_check: Literal["match", "mismatch", "not_provided"]
        """
        Whether the cardholder provided a postal code and if it matched the cardholder's `billing.address.postal_code`.
        """
        authentication_exemption: Optional[AuthenticationExemption]
        """
        The exemption applied to this authorization.
        """
        cvc_check: Literal["match", "mismatch", "not_provided"]
        """
        Whether the cardholder provided a CVC and if it matched Stripe's record.
        """
        expiry_check: Literal["match", "mismatch", "not_provided"]
        """
        Whether the cardholder provided an expiry date and if it matched Stripe's record.
        """
        postal_code: Optional[str]
        """
        The postal code submitted as part of the authorization used for postal code verification.
        """
        three_d_secure: Optional[ThreeDSecure]
        """
        3D Secure details.
        """
        _inner_class_types = {
            "authentication_exemption": AuthenticationExemption,
            "three_d_secure": ThreeDSecure,
        }

    amount: int
    """
    The total amount that was authorized or rejected. This amount is in `currency` and in the [smallest currency unit](https://stripe.com/docs/currencies#zero-decimal). `amount` should be the same as `merchant_amount`, unless `currency` and `merchant_currency` are different.
    """
    amount_details: Optional[AmountDetails]
    """
    Detailed breakdown of amount components. These amounts are denominated in `currency` and in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal).
    """
    approved: bool
    """
    Whether the authorization has been approved.
    """
    authorization_method: Literal[
        "chip", "contactless", "keyed_in", "online", "swipe"
    ]
    """
    How the card details were provided.
    """
    balance_transactions: List["BalanceTransaction"]
    """
    List of balance transactions associated with this authorization.
    """
    card: "Card"
    """
    You can [create physical or virtual cards](https://docs.stripe.com/issuing) that are issued to cardholders.
    """
    cardholder: Optional[ExpandableField["Cardholder"]]
    """
    The cardholder to whom this authorization belongs.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    The currency of the cardholder. This currency can be different from the currency presented at authorization and the `merchant_currency` field on this authorization. Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    fleet: Optional[Fleet]
    """
    Fleet-specific information for authorizations using Fleet cards.
    """
    fraud_challenges: Optional[List[FraudChallenge]]
    """
    Fraud challenges sent to the cardholder, if this authorization was declined for fraud risk reasons.
    """
    fuel: Optional[Fuel]
    """
    Information about fuel that was purchased with this transaction. Typically this information is received from the merchant after the authorization has been approved and the fuel dispensed.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    merchant_amount: int
    """
    The total amount that was authorized or rejected. This amount is in the `merchant_currency` and in the [smallest currency unit](https://stripe.com/docs/currencies#zero-decimal). `merchant_amount` should be the same as `amount`, unless `merchant_currency` and `currency` are different.
    """
    merchant_currency: str
    """
    The local currency that was presented to the cardholder for the authorization. This currency can be different from the cardholder currency and the `currency` field on this authorization. Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    merchant_data: MerchantData
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    network_data: Optional[NetworkData]
    """
    Details about the authorization, such as identifiers, set by the card network.
    """
    object: Literal["issuing.authorization"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    pending_request: Optional[PendingRequest]
    """
    The pending authorization request. This field will only be non-null during an `issuing_authorization.request` webhook.
    """
    request_history: List[RequestHistory]
    """
    History of every time a `pending_request` authorization was approved/declined, either by you directly or by Stripe (e.g. based on your spending_controls). If the merchant changes the authorization by performing an incremental authorization, you can look at this field to see the previous requests for the authorization. This field can be helpful in determining why a given authorization was approved/declined.
    """
    status: Literal["closed", "expired", "pending", "reversed"]
    """
    The current status of the authorization in its lifecycle.
    """
    token: Optional[ExpandableField["Token"]]
    """
    [Token](https://docs.stripe.com/api/issuing/tokens/object) object used for this authorization. If a network token was not used for this authorization, this field will be null.
    """
    transactions: List["Transaction"]
    """
    List of [transactions](https://docs.stripe.com/api/issuing/transactions) associated with this authorization.
    """
    treasury: Optional[Treasury]
    """
    [Treasury](https://docs.stripe.com/api/treasury) details related to this authorization if it was created on a [FinancialAccount](https://docs.stripe.com/api/treasury/financial_accounts).
    """
    verification_data: VerificationData
    verified_by_fraud_challenge: Optional[bool]
    """
    Whether the authorization bypassed fraud risk checks because the cardholder has previously completed a fraud challenge on a similar high-risk authorization from the same merchant.
    """
    wallet: Optional[str]
    """
    The digital wallet used for this transaction. One of `apple_pay`, `google_pay`, or `samsung_pay`. Will populate as `null` when no digital wallet was utilized.
    """

    @classmethod
    def _cls_approve(
        cls, authorization: str, **params: Unpack["AuthorizationApproveParams"]
    ) -> "Authorization":
        """
        [Deprecated] Approves a pending Issuing Authorization object. This request should be made within the timeout window of the [real-time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to approve an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        return cast(
            "Authorization",
            cls._static_request(
                "post",
                "/v1/issuing/authorizations/{authorization}/approve".format(
                    authorization=sanitize_id(authorization)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def approve(
        authorization: str, **params: Unpack["AuthorizationApproveParams"]
    ) -> "Authorization":
        """
        [Deprecated] Approves a pending Issuing Authorization object. This request should be made within the timeout window of the [real-time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to approve an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        ...

    @overload
    def approve(
        self, **params: Unpack["AuthorizationApproveParams"]
    ) -> "Authorization":
        """
        [Deprecated] Approves a pending Issuing Authorization object. This request should be made within the timeout window of the [real-time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to approve an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        ...

    @class_method_variant("_cls_approve")
    def approve(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AuthorizationApproveParams"]
    ) -> "Authorization":
        """
        [Deprecated] Approves a pending Issuing Authorization object. This request should be made within the timeout window of the [real-time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to approve an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        return cast(
            "Authorization",
            self._request(
                "post",
                "/v1/issuing/authorizations/{authorization}/approve".format(
                    authorization=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_approve_async(
        cls, authorization: str, **params: Unpack["AuthorizationApproveParams"]
    ) -> "Authorization":
        """
        [Deprecated] Approves a pending Issuing Authorization object. This request should be made within the timeout window of the [real-time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to approve an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        return cast(
            "Authorization",
            await cls._static_request_async(
                "post",
                "/v1/issuing/authorizations/{authorization}/approve".format(
                    authorization=sanitize_id(authorization)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def approve_async(
        authorization: str, **params: Unpack["AuthorizationApproveParams"]
    ) -> "Authorization":
        """
        [Deprecated] Approves a pending Issuing Authorization object. This request should be made within the timeout window of the [real-time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to approve an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        ...

    @overload
    async def approve_async(
        self, **params: Unpack["AuthorizationApproveParams"]
    ) -> "Authorization":
        """
        [Deprecated] Approves a pending Issuing Authorization object. This request should be made within the timeout window of the [real-time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to approve an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        ...

    @class_method_variant("_cls_approve_async")
    async def approve_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AuthorizationApproveParams"]
    ) -> "Authorization":
        """
        [Deprecated] Approves a pending Issuing Authorization object. This request should be made within the timeout window of the [real-time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to approve an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        return cast(
            "Authorization",
            await self._request_async(
                "post",
                "/v1/issuing/authorizations/{authorization}/approve".format(
                    authorization=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_decline(
        cls, authorization: str, **params: Unpack["AuthorizationDeclineParams"]
    ) -> "Authorization":
        """
        [Deprecated] Declines a pending Issuing Authorization object. This request should be made within the timeout window of the [real time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to decline an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        return cast(
            "Authorization",
            cls._static_request(
                "post",
                "/v1/issuing/authorizations/{authorization}/decline".format(
                    authorization=sanitize_id(authorization)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def decline(
        authorization: str, **params: Unpack["AuthorizationDeclineParams"]
    ) -> "Authorization":
        """
        [Deprecated] Declines a pending Issuing Authorization object. This request should be made within the timeout window of the [real time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to decline an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        ...

    @overload
    def decline(
        self, **params: Unpack["AuthorizationDeclineParams"]
    ) -> "Authorization":
        """
        [Deprecated] Declines a pending Issuing Authorization object. This request should be made within the timeout window of the [real time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to decline an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        ...

    @class_method_variant("_cls_decline")
    def decline(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AuthorizationDeclineParams"]
    ) -> "Authorization":
        """
        [Deprecated] Declines a pending Issuing Authorization object. This request should be made within the timeout window of the [real time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to decline an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        return cast(
            "Authorization",
            self._request(
                "post",
                "/v1/issuing/authorizations/{authorization}/decline".format(
                    authorization=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_decline_async(
        cls, authorization: str, **params: Unpack["AuthorizationDeclineParams"]
    ) -> "Authorization":
        """
        [Deprecated] Declines a pending Issuing Authorization object. This request should be made within the timeout window of the [real time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to decline an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        return cast(
            "Authorization",
            await cls._static_request_async(
                "post",
                "/v1/issuing/authorizations/{authorization}/decline".format(
                    authorization=sanitize_id(authorization)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def decline_async(
        authorization: str, **params: Unpack["AuthorizationDeclineParams"]
    ) -> "Authorization":
        """
        [Deprecated] Declines a pending Issuing Authorization object. This request should be made within the timeout window of the [real time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to decline an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        ...

    @overload
    async def decline_async(
        self, **params: Unpack["AuthorizationDeclineParams"]
    ) -> "Authorization":
        """
        [Deprecated] Declines a pending Issuing Authorization object. This request should be made within the timeout window of the [real time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to decline an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        ...

    @class_method_variant("_cls_decline_async")
    async def decline_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AuthorizationDeclineParams"]
    ) -> "Authorization":
        """
        [Deprecated] Declines a pending Issuing Authorization object. This request should be made within the timeout window of the [real time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to decline an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        return cast(
            "Authorization",
            await self._request_async(
                "post",
                "/v1/issuing/authorizations/{authorization}/decline".format(
                    authorization=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["AuthorizationListParams"]
    ) -> ListObject["Authorization"]:
        """
        Returns a list of Issuing Authorization objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
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
        cls, **params: Unpack["AuthorizationListParams"]
    ) -> ListObject["Authorization"]:
        """
        Returns a list of Issuing Authorization objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
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
    def modify(
        cls, id: str, **params: Unpack["AuthorizationModifyParams"]
    ) -> "Authorization":
        """
        Updates the specified Issuing Authorization object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Authorization",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["AuthorizationModifyParams"]
    ) -> "Authorization":
        """
        Updates the specified Issuing Authorization object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Authorization",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["AuthorizationRetrieveParams"]
    ) -> "Authorization":
        """
        Retrieves an Issuing Authorization object.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["AuthorizationRetrieveParams"]
    ) -> "Authorization":
        """
        Retrieves an Issuing Authorization object.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    class TestHelpers(APIResourceTestHelpers["Authorization"]):
        _resource_cls: Type["Authorization"]

        @classmethod
        def _cls_capture(
            cls,
            authorization: str,
            **params: Unpack["AuthorizationCaptureParams"],
        ) -> "Authorization":
            """
            Capture a test-mode authorization.
            """
            return cast(
                "Authorization",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/capture".format(
                        authorization=sanitize_id(authorization)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def capture(
            authorization: str, **params: Unpack["AuthorizationCaptureParams"]
        ) -> "Authorization":
            """
            Capture a test-mode authorization.
            """
            ...

        @overload
        def capture(
            self, **params: Unpack["AuthorizationCaptureParams"]
        ) -> "Authorization":
            """
            Capture a test-mode authorization.
            """
            ...

        @class_method_variant("_cls_capture")
        def capture(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["AuthorizationCaptureParams"]
        ) -> "Authorization":
            """
            Capture a test-mode authorization.
            """
            return cast(
                "Authorization",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/capture".format(
                        authorization=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_capture_async(
            cls,
            authorization: str,
            **params: Unpack["AuthorizationCaptureParams"],
        ) -> "Authorization":
            """
            Capture a test-mode authorization.
            """
            return cast(
                "Authorization",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/capture".format(
                        authorization=sanitize_id(authorization)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def capture_async(
            authorization: str, **params: Unpack["AuthorizationCaptureParams"]
        ) -> "Authorization":
            """
            Capture a test-mode authorization.
            """
            ...

        @overload
        async def capture_async(
            self, **params: Unpack["AuthorizationCaptureParams"]
        ) -> "Authorization":
            """
            Capture a test-mode authorization.
            """
            ...

        @class_method_variant("_cls_capture_async")
        async def capture_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["AuthorizationCaptureParams"]
        ) -> "Authorization":
            """
            Capture a test-mode authorization.
            """
            return cast(
                "Authorization",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/capture".format(
                        authorization=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def create(
            cls, **params: Unpack["AuthorizationCreateParams"]
        ) -> "Authorization":
            """
            Create a test-mode authorization.
            """
            return cast(
                "Authorization",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/authorizations",
                    params=params,
                ),
            )

        @classmethod
        async def create_async(
            cls, **params: Unpack["AuthorizationCreateParams"]
        ) -> "Authorization":
            """
            Create a test-mode authorization.
            """
            return cast(
                "Authorization",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/authorizations",
                    params=params,
                ),
            )

        @classmethod
        def _cls_expire(
            cls,
            authorization: str,
            **params: Unpack["AuthorizationExpireParams"],
        ) -> "Authorization":
            """
            Expire a test-mode Authorization.
            """
            return cast(
                "Authorization",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/expire".format(
                        authorization=sanitize_id(authorization)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def expire(
            authorization: str, **params: Unpack["AuthorizationExpireParams"]
        ) -> "Authorization":
            """
            Expire a test-mode Authorization.
            """
            ...

        @overload
        def expire(
            self, **params: Unpack["AuthorizationExpireParams"]
        ) -> "Authorization":
            """
            Expire a test-mode Authorization.
            """
            ...

        @class_method_variant("_cls_expire")
        def expire(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["AuthorizationExpireParams"]
        ) -> "Authorization":
            """
            Expire a test-mode Authorization.
            """
            return cast(
                "Authorization",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/expire".format(
                        authorization=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_expire_async(
            cls,
            authorization: str,
            **params: Unpack["AuthorizationExpireParams"],
        ) -> "Authorization":
            """
            Expire a test-mode Authorization.
            """
            return cast(
                "Authorization",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/expire".format(
                        authorization=sanitize_id(authorization)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def expire_async(
            authorization: str, **params: Unpack["AuthorizationExpireParams"]
        ) -> "Authorization":
            """
            Expire a test-mode Authorization.
            """
            ...

        @overload
        async def expire_async(
            self, **params: Unpack["AuthorizationExpireParams"]
        ) -> "Authorization":
            """
            Expire a test-mode Authorization.
            """
            ...

        @class_method_variant("_cls_expire_async")
        async def expire_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["AuthorizationExpireParams"]
        ) -> "Authorization":
            """
            Expire a test-mode Authorization.
            """
            return cast(
                "Authorization",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/expire".format(
                        authorization=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_finalize_amount(
            cls,
            authorization: str,
            **params: Unpack["AuthorizationFinalizeAmountParams"],
        ) -> "Authorization":
            """
            Finalize the amount on an Authorization prior to capture, when the initial authorization was for an estimated amount.
            """
            return cast(
                "Authorization",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/finalize_amount".format(
                        authorization=sanitize_id(authorization)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def finalize_amount(
            authorization: str,
            **params: Unpack["AuthorizationFinalizeAmountParams"],
        ) -> "Authorization":
            """
            Finalize the amount on an Authorization prior to capture, when the initial authorization was for an estimated amount.
            """
            ...

        @overload
        def finalize_amount(
            self, **params: Unpack["AuthorizationFinalizeAmountParams"]
        ) -> "Authorization":
            """
            Finalize the amount on an Authorization prior to capture, when the initial authorization was for an estimated amount.
            """
            ...

        @class_method_variant("_cls_finalize_amount")
        def finalize_amount(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["AuthorizationFinalizeAmountParams"]
        ) -> "Authorization":
            """
            Finalize the amount on an Authorization prior to capture, when the initial authorization was for an estimated amount.
            """
            return cast(
                "Authorization",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/finalize_amount".format(
                        authorization=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_finalize_amount_async(
            cls,
            authorization: str,
            **params: Unpack["AuthorizationFinalizeAmountParams"],
        ) -> "Authorization":
            """
            Finalize the amount on an Authorization prior to capture, when the initial authorization was for an estimated amount.
            """
            return cast(
                "Authorization",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/finalize_amount".format(
                        authorization=sanitize_id(authorization)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def finalize_amount_async(
            authorization: str,
            **params: Unpack["AuthorizationFinalizeAmountParams"],
        ) -> "Authorization":
            """
            Finalize the amount on an Authorization prior to capture, when the initial authorization was for an estimated amount.
            """
            ...

        @overload
        async def finalize_amount_async(
            self, **params: Unpack["AuthorizationFinalizeAmountParams"]
        ) -> "Authorization":
            """
            Finalize the amount on an Authorization prior to capture, when the initial authorization was for an estimated amount.
            """
            ...

        @class_method_variant("_cls_finalize_amount_async")
        async def finalize_amount_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["AuthorizationFinalizeAmountParams"]
        ) -> "Authorization":
            """
            Finalize the amount on an Authorization prior to capture, when the initial authorization was for an estimated amount.
            """
            return cast(
                "Authorization",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/finalize_amount".format(
                        authorization=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_increment(
            cls,
            authorization: str,
            **params: Unpack["AuthorizationIncrementParams"],
        ) -> "Authorization":
            """
            Increment a test-mode Authorization.
            """
            return cast(
                "Authorization",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/increment".format(
                        authorization=sanitize_id(authorization)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def increment(
            authorization: str,
            **params: Unpack["AuthorizationIncrementParams"],
        ) -> "Authorization":
            """
            Increment a test-mode Authorization.
            """
            ...

        @overload
        def increment(
            self, **params: Unpack["AuthorizationIncrementParams"]
        ) -> "Authorization":
            """
            Increment a test-mode Authorization.
            """
            ...

        @class_method_variant("_cls_increment")
        def increment(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["AuthorizationIncrementParams"]
        ) -> "Authorization":
            """
            Increment a test-mode Authorization.
            """
            return cast(
                "Authorization",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/increment".format(
                        authorization=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_increment_async(
            cls,
            authorization: str,
            **params: Unpack["AuthorizationIncrementParams"],
        ) -> "Authorization":
            """
            Increment a test-mode Authorization.
            """
            return cast(
                "Authorization",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/increment".format(
                        authorization=sanitize_id(authorization)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def increment_async(
            authorization: str,
            **params: Unpack["AuthorizationIncrementParams"],
        ) -> "Authorization":
            """
            Increment a test-mode Authorization.
            """
            ...

        @overload
        async def increment_async(
            self, **params: Unpack["AuthorizationIncrementParams"]
        ) -> "Authorization":
            """
            Increment a test-mode Authorization.
            """
            ...

        @class_method_variant("_cls_increment_async")
        async def increment_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["AuthorizationIncrementParams"]
        ) -> "Authorization":
            """
            Increment a test-mode Authorization.
            """
            return cast(
                "Authorization",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/increment".format(
                        authorization=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_respond(
            cls,
            authorization: str,
            **params: Unpack["AuthorizationRespondParams"],
        ) -> "Authorization":
            """
            Respond to a fraud challenge on a testmode Issuing authorization, simulating either a confirmation of fraud or a correction of legitimacy.
            """
            return cast(
                "Authorization",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/fraud_challenges/respond".format(
                        authorization=sanitize_id(authorization)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def respond(
            authorization: str, **params: Unpack["AuthorizationRespondParams"]
        ) -> "Authorization":
            """
            Respond to a fraud challenge on a testmode Issuing authorization, simulating either a confirmation of fraud or a correction of legitimacy.
            """
            ...

        @overload
        def respond(
            self, **params: Unpack["AuthorizationRespondParams"]
        ) -> "Authorization":
            """
            Respond to a fraud challenge on a testmode Issuing authorization, simulating either a confirmation of fraud or a correction of legitimacy.
            """
            ...

        @class_method_variant("_cls_respond")
        def respond(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["AuthorizationRespondParams"]
        ) -> "Authorization":
            """
            Respond to a fraud challenge on a testmode Issuing authorization, simulating either a confirmation of fraud or a correction of legitimacy.
            """
            return cast(
                "Authorization",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/fraud_challenges/respond".format(
                        authorization=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_respond_async(
            cls,
            authorization: str,
            **params: Unpack["AuthorizationRespondParams"],
        ) -> "Authorization":
            """
            Respond to a fraud challenge on a testmode Issuing authorization, simulating either a confirmation of fraud or a correction of legitimacy.
            """
            return cast(
                "Authorization",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/fraud_challenges/respond".format(
                        authorization=sanitize_id(authorization)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def respond_async(
            authorization: str, **params: Unpack["AuthorizationRespondParams"]
        ) -> "Authorization":
            """
            Respond to a fraud challenge on a testmode Issuing authorization, simulating either a confirmation of fraud or a correction of legitimacy.
            """
            ...

        @overload
        async def respond_async(
            self, **params: Unpack["AuthorizationRespondParams"]
        ) -> "Authorization":
            """
            Respond to a fraud challenge on a testmode Issuing authorization, simulating either a confirmation of fraud or a correction of legitimacy.
            """
            ...

        @class_method_variant("_cls_respond_async")
        async def respond_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["AuthorizationRespondParams"]
        ) -> "Authorization":
            """
            Respond to a fraud challenge on a testmode Issuing authorization, simulating either a confirmation of fraud or a correction of legitimacy.
            """
            return cast(
                "Authorization",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/fraud_challenges/respond".format(
                        authorization=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        def _cls_reverse(
            cls,
            authorization: str,
            **params: Unpack["AuthorizationReverseParams"],
        ) -> "Authorization":
            """
            Reverse a test-mode Authorization.
            """
            return cast(
                "Authorization",
                cls._static_request(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/reverse".format(
                        authorization=sanitize_id(authorization)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        def reverse(
            authorization: str, **params: Unpack["AuthorizationReverseParams"]
        ) -> "Authorization":
            """
            Reverse a test-mode Authorization.
            """
            ...

        @overload
        def reverse(
            self, **params: Unpack["AuthorizationReverseParams"]
        ) -> "Authorization":
            """
            Reverse a test-mode Authorization.
            """
            ...

        @class_method_variant("_cls_reverse")
        def reverse(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["AuthorizationReverseParams"]
        ) -> "Authorization":
            """
            Reverse a test-mode Authorization.
            """
            return cast(
                "Authorization",
                self.resource._request(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/reverse".format(
                        authorization=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

        @classmethod
        async def _cls_reverse_async(
            cls,
            authorization: str,
            **params: Unpack["AuthorizationReverseParams"],
        ) -> "Authorization":
            """
            Reverse a test-mode Authorization.
            """
            return cast(
                "Authorization",
                await cls._static_request_async(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/reverse".format(
                        authorization=sanitize_id(authorization)
                    ),
                    params=params,
                ),
            )

        @overload
        @staticmethod
        async def reverse_async(
            authorization: str, **params: Unpack["AuthorizationReverseParams"]
        ) -> "Authorization":
            """
            Reverse a test-mode Authorization.
            """
            ...

        @overload
        async def reverse_async(
            self, **params: Unpack["AuthorizationReverseParams"]
        ) -> "Authorization":
            """
            Reverse a test-mode Authorization.
            """
            ...

        @class_method_variant("_cls_reverse_async")
        async def reverse_async(  # pyright: ignore[reportGeneralTypeIssues]
            self, **params: Unpack["AuthorizationReverseParams"]
        ) -> "Authorization":
            """
            Reverse a test-mode Authorization.
            """
            return cast(
                "Authorization",
                await self.resource._request_async(
                    "post",
                    "/v1/test_helpers/issuing/authorizations/{authorization}/reverse".format(
                        authorization=sanitize_id(self.resource.get("id"))
                    ),
                    params=params,
                ),
            )

    @property
    def test_helpers(self):
        return self.TestHelpers(self)

    _inner_class_types = {
        "amount_details": AmountDetails,
        "fleet": Fleet,
        "fraud_challenges": FraudChallenge,
        "fuel": Fuel,
        "merchant_data": MerchantData,
        "network_data": NetworkData,
        "pending_request": PendingRequest,
        "request_history": RequestHistory,
        "treasury": Treasury,
        "verification_data": VerificationData,
    }


Authorization.TestHelpers._resource_cls = Authorization
