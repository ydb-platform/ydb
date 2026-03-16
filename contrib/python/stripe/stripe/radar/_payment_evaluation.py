# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._stripe_object import StripeObject
from typing import ClassVar, Dict, List, Optional, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._payment_method import PaymentMethod
    from stripe.params.radar._payment_evaluation_create_params import (
        PaymentEvaluationCreateParams,
    )


class PaymentEvaluation(CreateableAPIResource["PaymentEvaluation"]):
    """
    Payment Evaluations represent the risk lifecycle of an externally processed payment. It includes the Radar risk score from Stripe, payment outcome taken by the merchant or processor, and any post transaction events, such as refunds or disputes. See the [Radar API guide](https://docs.stripe.com/radar/multiprocessor) for integration steps.
    """

    OBJECT_NAME: ClassVar[Literal["radar.payment_evaluation"]] = (
        "radar.payment_evaluation"
    )

    class ClientDeviceMetadataDetails(StripeObject):
        radar_session: str
        """
        ID for the Radar Session associated with the payment evaluation. A [Radar Session](https://docs.stripe.com/radar/radar-session) is a snapshot of the browser metadata and device details that help Radar make more accurate predictions on your payments.
        """

    class CustomerDetails(StripeObject):
        customer: Optional[str]
        """
        The ID of the customer associated with the payment evaluation.
        """
        customer_account: Optional[str]
        """
        The ID of the Account representing the customer associated with the payment evaluation.
        """
        email: Optional[str]
        """
        The customer's email address.
        """
        name: Optional[str]
        """
        The customer's full name or business name.
        """
        phone: Optional[str]
        """
        The customer's phone number.
        """

    class Event(StripeObject):
        class DisputeOpened(StripeObject):
            amount: int
            """
            Amount to dispute for this payment. A positive integer representing how much to charge in [the smallest currency unit](https://docs.stripe.com/currencies#zero-decimal) (for example, 100 cents to charge 1.00 USD or 100 to charge 100 Yen, a zero-decimal currency).
            """
            currency: str
            """
            Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
            """
            reason: Literal[
                "account_not_available",
                "credit_not_processed",
                "customer_initiated",
                "duplicate",
                "fraudulent",
                "general",
                "noncompliant",
                "product_not_received",
                "product_unacceptable",
                "subscription_canceled",
                "unrecognized",
            ]
            """
            Reason given by cardholder for dispute.
            """

        class EarlyFraudWarningReceived(StripeObject):
            fraud_type: Literal[
                "made_with_lost_card",
                "made_with_stolen_card",
                "other",
                "unauthorized_use_of_card",
            ]
            """
            The type of fraud labeled by the issuer.
            """

        class Refunded(StripeObject):
            amount: int
            """
            Amount refunded for this payment. A positive integer representing how much to charge in [the smallest currency unit](https://docs.stripe.com/currencies#zero-decimal) (for example, 100 cents to charge 1.00 USD or 100 to charge 100 Yen, a zero-decimal currency).
            """
            currency: str
            """
            Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
            """
            reason: Literal[
                "duplicate", "fraudulent", "other", "requested_by_customer"
            ]
            """
            Indicates the reason for the refund.
            """

        class UserInterventionRaised(StripeObject):
            class Custom(StripeObject):
                type: str
                """
                Custom type of user intervention raised. The string must use a snake case description for the type of intervention performed.
                """

            custom: Optional[Custom]
            """
            User intervention raised custom event details attached to this payment evaluation
            """
            key: str
            """
            Unique identifier for the user intervention event.
            """
            type: Literal["3ds", "captcha", "custom"]
            """
            Type of user intervention raised.
            """
            _inner_class_types = {"custom": Custom}

        class UserInterventionResolved(StripeObject):
            key: str
            """
            Unique ID of this intervention. Use this to provide the result.
            """
            outcome: Optional[Literal["abandoned", "failed", "passed"]]
            """
            Result of the intervention if it has been completed.
            """

        dispute_opened: Optional[DisputeOpened]
        """
        Dispute opened event details attached to this payment evaluation.
        """
        early_fraud_warning_received: Optional[EarlyFraudWarningReceived]
        """
        Early Fraud Warning Received event details attached to this payment evaluation.
        """
        occurred_at: int
        """
        Timestamp when the event occurred.
        """
        refunded: Optional[Refunded]
        """
        Refunded Event details attached to this payment evaluation.
        """
        type: Literal[
            "dispute_opened",
            "early_fraud_warning_received",
            "refunded",
            "user_intervention_raised",
            "user_intervention_resolved",
        ]
        """
        Indicates the type of event attached to the payment evaluation.
        """
        user_intervention_raised: Optional[UserInterventionRaised]
        """
        User intervention raised event details attached to this payment evaluation
        """
        user_intervention_resolved: Optional[UserInterventionResolved]
        """
        User Intervention Resolved Event details attached to this payment evaluation
        """
        _inner_class_types = {
            "dispute_opened": DisputeOpened,
            "early_fraud_warning_received": EarlyFraudWarningReceived,
            "refunded": Refunded,
            "user_intervention_raised": UserInterventionRaised,
            "user_intervention_resolved": UserInterventionResolved,
        }

    class Insights(StripeObject):
        class FraudulentDispute(StripeObject):
            recommended_action: Literal["block", "continue"]
            """
            Recommended action based on the risk score. Possible values are `block` and `continue`.
            """
            risk_score: int
            """
            Stripe Radar's evaluation of the risk level of the payment. Possible values for evaluated payments are between 0 and 100, with higher scores indicating higher risk.
            """

        evaluated_at: int
        """
        The timestamp when the evaluation was performed.
        """
        fraudulent_dispute: FraudulentDispute
        """
        Scores, insights and recommended action for one scorer for this PaymentEvaluation.
        """
        _inner_class_types = {"fraudulent_dispute": FraudulentDispute}

    class Outcome(StripeObject):
        class MerchantBlocked(StripeObject):
            reason: Literal[
                "authentication_required",
                "blocked_for_fraud",
                "invalid_payment",
                "other",
            ]
            """
            The reason the payment was blocked by the merchant.
            """

        class Rejected(StripeObject):
            class Card(StripeObject):
                address_line1_check: Literal[
                    "fail", "pass", "unavailable", "unchecked"
                ]
                """
                Result of the address line 1 check.
                """
                address_postal_code_check: Literal[
                    "fail", "pass", "unavailable", "unchecked"
                ]
                """
                Indicates whether the cardholder provided a postal code and if it matched the cardholder's billing address.
                """
                cvc_check: Literal["fail", "pass", "unavailable", "unchecked"]
                """
                Result of the CVC check.
                """
                reason: Literal[
                    "authentication_failed",
                    "do_not_honor",
                    "expired",
                    "incorrect_cvc",
                    "incorrect_number",
                    "incorrect_postal_code",
                    "insufficient_funds",
                    "invalid_account",
                    "lost_card",
                    "other",
                    "processing_error",
                    "reported_stolen",
                    "try_again_later",
                ]
                """
                Card issuer's reason for the network decline.
                """

            card: Optional[Card]
            """
            Details of an rejected card outcome attached to this payment evaluation.
            """
            _inner_class_types = {"card": Card}

        class Succeeded(StripeObject):
            class Card(StripeObject):
                address_line1_check: Literal[
                    "fail", "pass", "unavailable", "unchecked"
                ]
                """
                Result of the address line 1 check.
                """
                address_postal_code_check: Literal[
                    "fail", "pass", "unavailable", "unchecked"
                ]
                """
                Indicates whether the cardholder provided a postal code and if it matched the cardholder's billing address.
                """
                cvc_check: Literal["fail", "pass", "unavailable", "unchecked"]
                """
                Result of the CVC check.
                """

            card: Optional[Card]
            """
            Details of an succeeded card outcome attached to this payment evaluation.
            """
            _inner_class_types = {"card": Card}

        merchant_blocked: Optional[MerchantBlocked]
        """
        Details of a merchant_blocked outcome attached to this payment evaluation.
        """
        payment_intent_id: Optional[str]
        """
        The PaymentIntent ID associated with the payment evaluation.
        """
        rejected: Optional[Rejected]
        """
        Details of an rejected outcome attached to this payment evaluation.
        """
        succeeded: Optional[Succeeded]
        """
        Details of a succeeded outcome attached to this payment evaluation.
        """
        type: Literal["failed", "merchant_blocked", "rejected", "succeeded"]
        """
        Indicates the outcome of the payment evaluation.
        """
        _inner_class_types = {
            "merchant_blocked": MerchantBlocked,
            "rejected": Rejected,
            "succeeded": Succeeded,
        }

    class PaymentDetails(StripeObject):
        class MoneyMovementDetails(StripeObject):
            class Card(StripeObject):
                customer_presence: Optional[
                    Literal["off_session", "on_session"]
                ]
                """
                Describes the presence of the customer during the payment.
                """
                payment_type: Optional[
                    Literal[
                        "one_off",
                        "recurring",
                        "setup_one_off",
                        "setup_recurring",
                    ]
                ]
                """
                Describes the type of payment.
                """

            card: Optional[Card]
            """
            Describes card money movement details for the payment evaluation.
            """
            money_movement_type: Literal["card"]
            """
            Describes the type of money movement. Currently only `card` is supported.
            """
            _inner_class_types = {"card": Card}

        class PaymentMethodDetails(StripeObject):
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
                """
                Address data.
                """
                email: Optional[str]
                """
                Email address.
                """
                name: Optional[str]
                """
                Full name.
                """
                phone: Optional[str]
                """
                Billing phone number (including extension).
                """
                _inner_class_types = {"address": Address}

            billing_details: Optional[BillingDetails]
            """
            Billing information associated with the payment evaluation.
            """
            payment_method: ExpandableField["PaymentMethod"]
            """
            The payment method used in this payment evaluation.
            """
            _inner_class_types = {"billing_details": BillingDetails}

        class ShippingDetails(StripeObject):
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
            """
            Address data.
            """
            name: Optional[str]
            """
            Shipping name.
            """
            phone: Optional[str]
            """
            Shipping phone number.
            """
            _inner_class_types = {"address": Address}

        amount: int
        """
        Amount intended to be collected by this payment. A positive integer representing how much to charge in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal) (e.g., 100 cents to charge $1.00 or 100 to charge ¥100, a zero-decimal currency). The minimum amount is $0.50 US or [equivalent in charge currency](https://docs.stripe.com/currencies#minimum-and-maximum-charge-amounts). The amount value supports up to eight digits (e.g., a value of 99999999 for a USD charge of $999,999.99).
        """
        currency: str
        """
        Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        description: Optional[str]
        """
        An arbitrary string attached to the object. Often useful for displaying to users.
        """
        money_movement_details: Optional[MoneyMovementDetails]
        """
        Details about the payment's customer presence and type.
        """
        payment_method_details: Optional[PaymentMethodDetails]
        """
        Details about the payment method used for the payment.
        """
        shipping_details: Optional[ShippingDetails]
        """
        Shipping details for the payment evaluation.
        """
        statement_descriptor: Optional[str]
        """
        Payment statement descriptor.
        """
        _inner_class_types = {
            "money_movement_details": MoneyMovementDetails,
            "payment_method_details": PaymentMethodDetails,
            "shipping_details": ShippingDetails,
        }

    client_device_metadata_details: Optional[ClientDeviceMetadataDetails]
    """
    Client device metadata attached to this payment evaluation.
    """
    created_at: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    customer_details: Optional[CustomerDetails]
    """
    Customer details attached to this payment evaluation.
    """
    events: Optional[List[Event]]
    """
    Event information associated with the payment evaluation, such as refunds, dispute, early fraud warnings, or user interventions.
    """
    id: str
    """
    Unique identifier for the object.
    """
    insights: Insights
    """
    Collection of scores and insights for this payment evaluation.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["radar.payment_evaluation"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    outcome: Optional[Outcome]
    """
    Indicates the final outcome for the payment evaluation.
    """
    payment_details: Optional[PaymentDetails]
    """
    Payment details attached to this payment evaluation.
    """

    @classmethod
    def create(
        cls, **params: Unpack["PaymentEvaluationCreateParams"]
    ) -> "PaymentEvaluation":
        """
        Request a Radar API fraud risk score from Stripe for a payment before sending it for external processor authorization.
        """
        return cast(
            "PaymentEvaluation",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["PaymentEvaluationCreateParams"]
    ) -> "PaymentEvaluation":
        """
        Request a Radar API fraud risk score from Stripe for a payment before sending it for external processor authorization.
        """
        return cast(
            "PaymentEvaluation",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    _inner_class_types = {
        "client_device_metadata_details": ClientDeviceMetadataDetails,
        "customer_details": CustomerDetails,
        "events": Event,
        "insights": Insights,
        "outcome": Outcome,
        "payment_details": PaymentDetails,
    }
