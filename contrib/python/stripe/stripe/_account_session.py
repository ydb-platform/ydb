# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._stripe_object import StripeObject
from typing import ClassVar, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params._account_session_create_params import (
        AccountSessionCreateParams,
    )


class AccountSession(CreateableAPIResource["AccountSession"]):
    """
    An AccountSession allows a Connect platform to grant access to a connected account in Connect embedded components.

    We recommend that you create an AccountSession each time you need to display an embedded component
    to your user. Do not save AccountSessions to your database as they expire relatively
    quickly, and cannot be used more than once.

    Related guide: [Connect embedded components](https://docs.stripe.com/connect/get-started-connect-embedded-components)
    """

    OBJECT_NAME: ClassVar[Literal["account_session"]] = "account_session"

    class Components(StripeObject):
        class AccountManagement(StripeObject):
            class Features(StripeObject):
                disable_stripe_user_authentication: bool
                """
                Whether Stripe user authentication is disabled. This value can only be `true` for accounts where `controller.requirement_collection` is `application` for the account. The default value is the opposite of the `external_account_collection` value. For example, if you don't set `external_account_collection`, it defaults to `true` and `disable_stripe_user_authentication` defaults to `false`.
                """
                external_account_collection: bool
                """
                Whether external account collection is enabled. This feature can only be `false` for accounts where you're responsible for collecting updated information when requirements are due or change, like Custom accounts. The default value for this feature is `true`.
                """

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class AccountOnboarding(StripeObject):
            class Features(StripeObject):
                disable_stripe_user_authentication: bool
                """
                Whether Stripe user authentication is disabled. This value can only be `true` for accounts where `controller.requirement_collection` is `application` for the account. The default value is the opposite of the `external_account_collection` value. For example, if you don't set `external_account_collection`, it defaults to `true` and `disable_stripe_user_authentication` defaults to `false`.
                """
                external_account_collection: bool
                """
                Whether external account collection is enabled. This feature can only be `false` for accounts where you're responsible for collecting updated information when requirements are due or change, like Custom accounts. The default value for this feature is `true`.
                """

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class Balances(StripeObject):
            class Features(StripeObject):
                disable_stripe_user_authentication: bool
                """
                Whether Stripe user authentication is disabled. This value can only be `true` for accounts where `controller.requirement_collection` is `application` for the account. The default value is the opposite of the `external_account_collection` value. For example, if you don't set `external_account_collection`, it defaults to `true` and `disable_stripe_user_authentication` defaults to `false`.
                """
                edit_payout_schedule: bool
                """
                Whether to allow payout schedule to be changed. Defaults to `true` when `controller.losses.payments` is set to `stripe` for the account, otherwise `false`.
                """
                external_account_collection: bool
                """
                Whether external account collection is enabled. This feature can only be `false` for accounts where you're responsible for collecting updated information when requirements are due or change, like Custom accounts. The default value for this feature is `true`.
                """
                instant_payouts: bool
                """
                Whether to allow creation of instant payouts. The default value is `enabled` when Stripe is responsible for negative account balances, and `use_dashboard_rules` otherwise.
                """
                standard_payouts: bool
                """
                Whether to allow creation of standard payouts. Defaults to `true` when `controller.losses.payments` is set to `stripe` for the account, otherwise `false`.
                """

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class DisputesList(StripeObject):
            class Features(StripeObject):
                capture_payments: bool
                """
                Whether to allow capturing and cancelling payment intents. This is `true` by default.
                """
                destination_on_behalf_of_charge_management: bool
                """
                Whether connected accounts can manage destination charges that are created on behalf of them. This is `false` by default.
                """
                dispute_management: bool
                """
                Whether responding to disputes is enabled, including submitting evidence and accepting disputes. This is `true` by default.
                """
                refund_management: bool
                """
                Whether sending refunds is enabled. This is `true` by default.
                """

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class Documents(StripeObject):
            class Features(StripeObject):
                pass

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class FinancialAccount(StripeObject):
            class Features(StripeObject):
                disable_stripe_user_authentication: bool
                """
                Whether Stripe user authentication is disabled. This value can only be `true` for accounts where `controller.requirement_collection` is `application` for the account. The default value is the opposite of the `external_account_collection` value. For example, if you don't set `external_account_collection`, it defaults to `true` and `disable_stripe_user_authentication` defaults to `false`.
                """
                external_account_collection: bool
                """
                Whether external account collection is enabled. This feature can only be `false` for accounts where you're responsible for collecting updated information when requirements are due or change, like Custom accounts. The default value for this feature is `true`.
                """
                send_money: bool
                """
                Whether to allow sending money.
                """
                transfer_balance: bool
                """
                Whether to allow transferring balance.
                """

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class FinancialAccountTransactions(StripeObject):
            class Features(StripeObject):
                card_spend_dispute_management: bool
                """
                Whether to allow card spend dispute management features.
                """

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class InstantPayoutsPromotion(StripeObject):
            class Features(StripeObject):
                disable_stripe_user_authentication: bool
                """
                Whether Stripe user authentication is disabled. This value can only be `true` for accounts where `controller.requirement_collection` is `application` for the account. The default value is the opposite of the `external_account_collection` value. For example, if you don't set `external_account_collection`, it defaults to `true` and `disable_stripe_user_authentication` defaults to `false`.
                """
                external_account_collection: bool
                """
                Whether external account collection is enabled. This feature can only be `false` for accounts where you're responsible for collecting updated information when requirements are due or change, like Custom accounts. The default value for this feature is `true`.
                """
                instant_payouts: bool
                """
                Whether to allow creation of instant payouts. The default value is `enabled` when Stripe is responsible for negative account balances, and `use_dashboard_rules` otherwise.
                """

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class IssuingCard(StripeObject):
            class Features(StripeObject):
                card_management: bool
                """
                Whether to allow card management features.
                """
                card_spend_dispute_management: bool
                """
                Whether to allow card spend dispute management features.
                """
                cardholder_management: bool
                """
                Whether to allow cardholder management features.
                """
                spend_control_management: bool
                """
                Whether to allow spend control management features.
                """

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class IssuingCardsList(StripeObject):
            class Features(StripeObject):
                card_management: bool
                """
                Whether to allow card management features.
                """
                card_spend_dispute_management: bool
                """
                Whether to allow card spend dispute management features.
                """
                cardholder_management: bool
                """
                Whether to allow cardholder management features.
                """
                disable_stripe_user_authentication: bool
                """
                Whether Stripe user authentication is disabled. This value can only be `true` for accounts where `controller.requirement_collection` is `application` for the account. The default value is the opposite of the `external_account_collection` value. For example, if you don't set `external_account_collection`, it defaults to `true` and `disable_stripe_user_authentication` defaults to `false`.
                """
                spend_control_management: bool
                """
                Whether to allow spend control management features.
                """

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class NotificationBanner(StripeObject):
            class Features(StripeObject):
                disable_stripe_user_authentication: bool
                """
                Whether Stripe user authentication is disabled. This value can only be `true` for accounts where `controller.requirement_collection` is `application` for the account. The default value is the opposite of the `external_account_collection` value. For example, if you don't set `external_account_collection`, it defaults to `true` and `disable_stripe_user_authentication` defaults to `false`.
                """
                external_account_collection: bool
                """
                Whether external account collection is enabled. This feature can only be `false` for accounts where you're responsible for collecting updated information when requirements are due or change, like Custom accounts. The default value for this feature is `true`.
                """

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class PaymentDetails(StripeObject):
            class Features(StripeObject):
                capture_payments: bool
                """
                Whether to allow capturing and cancelling payment intents. This is `true` by default.
                """
                destination_on_behalf_of_charge_management: bool
                """
                Whether connected accounts can manage destination charges that are created on behalf of them. This is `false` by default.
                """
                dispute_management: bool
                """
                Whether responding to disputes is enabled, including submitting evidence and accepting disputes. This is `true` by default.
                """
                refund_management: bool
                """
                Whether sending refunds is enabled. This is `true` by default.
                """

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class PaymentDisputes(StripeObject):
            class Features(StripeObject):
                destination_on_behalf_of_charge_management: bool
                """
                Whether connected accounts can manage destination charges that are created on behalf of them. This is `false` by default.
                """
                dispute_management: bool
                """
                Whether responding to disputes is enabled, including submitting evidence and accepting disputes. This is `true` by default.
                """
                refund_management: bool
                """
                Whether sending refunds is enabled. This is `true` by default.
                """

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class Payments(StripeObject):
            class Features(StripeObject):
                capture_payments: bool
                """
                Whether to allow capturing and cancelling payment intents. This is `true` by default.
                """
                destination_on_behalf_of_charge_management: bool
                """
                Whether connected accounts can manage destination charges that are created on behalf of them. This is `false` by default.
                """
                dispute_management: bool
                """
                Whether responding to disputes is enabled, including submitting evidence and accepting disputes. This is `true` by default.
                """
                refund_management: bool
                """
                Whether sending refunds is enabled. This is `true` by default.
                """

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class PayoutDetails(StripeObject):
            class Features(StripeObject):
                pass

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class Payouts(StripeObject):
            class Features(StripeObject):
                disable_stripe_user_authentication: bool
                """
                Whether Stripe user authentication is disabled. This value can only be `true` for accounts where `controller.requirement_collection` is `application` for the account. The default value is the opposite of the `external_account_collection` value. For example, if you don't set `external_account_collection`, it defaults to `true` and `disable_stripe_user_authentication` defaults to `false`.
                """
                edit_payout_schedule: bool
                """
                Whether to allow payout schedule to be changed. Defaults to `true` when `controller.losses.payments` is set to `stripe` for the account, otherwise `false`.
                """
                external_account_collection: bool
                """
                Whether external account collection is enabled. This feature can only be `false` for accounts where you're responsible for collecting updated information when requirements are due or change, like Custom accounts. The default value for this feature is `true`.
                """
                instant_payouts: bool
                """
                Whether to allow creation of instant payouts. The default value is `enabled` when Stripe is responsible for negative account balances, and `use_dashboard_rules` otherwise.
                """
                standard_payouts: bool
                """
                Whether to allow creation of standard payouts. Defaults to `true` when `controller.losses.payments` is set to `stripe` for the account, otherwise `false`.
                """

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class PayoutsList(StripeObject):
            class Features(StripeObject):
                pass

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class TaxRegistrations(StripeObject):
            class Features(StripeObject):
                pass

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        class TaxSettings(StripeObject):
            class Features(StripeObject):
                pass

            enabled: bool
            """
            Whether the embedded component is enabled.
            """
            features: Features
            _inner_class_types = {"features": Features}

        account_management: AccountManagement
        account_onboarding: AccountOnboarding
        balances: Balances
        disputes_list: DisputesList
        documents: Documents
        financial_account: FinancialAccount
        financial_account_transactions: FinancialAccountTransactions
        instant_payouts_promotion: InstantPayoutsPromotion
        issuing_card: IssuingCard
        issuing_cards_list: IssuingCardsList
        notification_banner: NotificationBanner
        payment_details: PaymentDetails
        payment_disputes: PaymentDisputes
        payments: Payments
        payout_details: PayoutDetails
        payouts: Payouts
        payouts_list: PayoutsList
        tax_registrations: TaxRegistrations
        tax_settings: TaxSettings
        _inner_class_types = {
            "account_management": AccountManagement,
            "account_onboarding": AccountOnboarding,
            "balances": Balances,
            "disputes_list": DisputesList,
            "documents": Documents,
            "financial_account": FinancialAccount,
            "financial_account_transactions": FinancialAccountTransactions,
            "instant_payouts_promotion": InstantPayoutsPromotion,
            "issuing_card": IssuingCard,
            "issuing_cards_list": IssuingCardsList,
            "notification_banner": NotificationBanner,
            "payment_details": PaymentDetails,
            "payment_disputes": PaymentDisputes,
            "payments": Payments,
            "payout_details": PayoutDetails,
            "payouts": Payouts,
            "payouts_list": PayoutsList,
            "tax_registrations": TaxRegistrations,
            "tax_settings": TaxSettings,
        }

    account: str
    """
    The ID of the account the AccountSession was created for
    """
    client_secret: str
    """
    The client secret of this AccountSession. Used on the client to set up secure access to the given `account`.

    The client secret can be used to provide access to `account` from your frontend. It should not be stored, logged, or exposed to anyone other than the connected account. Make sure that you have TLS enabled on any page that includes the client secret.

    Refer to our docs to [setup Connect embedded components](https://docs.stripe.com/connect/get-started-connect-embedded-components) and learn about how `client_secret` should be handled.
    """
    components: Components
    expires_at: int
    """
    The timestamp at which this AccountSession will expire.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["account_session"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """

    @classmethod
    def create(
        cls, **params: Unpack["AccountSessionCreateParams"]
    ) -> "AccountSession":
        """
        Creates a AccountSession object that includes a single-use token that the platform can use on their front-end to grant client-side API access.
        """
        return cast(
            "AccountSession",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["AccountSessionCreateParams"]
    ) -> "AccountSession":
        """
        Creates a AccountSession object that includes a single-use token that the platform can use on their front-end to grant client-side API access.
        """
        return cast(
            "AccountSession",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    _inner_class_types = {"components": Components}
