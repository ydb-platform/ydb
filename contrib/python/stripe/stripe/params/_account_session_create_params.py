# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import NotRequired, TypedDict


class AccountSessionCreateParams(RequestOptions):
    account: str
    """
    The identifier of the account to create an Account Session for.
    """
    components: "AccountSessionCreateParamsComponents"
    """
    Each key of the dictionary represents an embedded component, and each embedded component maps to its configuration (e.g. whether it has been enabled or not).
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """


class AccountSessionCreateParamsComponents(TypedDict):
    account_management: NotRequired[
        "AccountSessionCreateParamsComponentsAccountManagement"
    ]
    """
    Configuration for the [account management](https://docs.stripe.com/connect/supported-embedded-components/account-management/) embedded component.
    """
    account_onboarding: NotRequired[
        "AccountSessionCreateParamsComponentsAccountOnboarding"
    ]
    """
    Configuration for the [account onboarding](https://docs.stripe.com/connect/supported-embedded-components/account-onboarding/) embedded component.
    """
    balances: NotRequired["AccountSessionCreateParamsComponentsBalances"]
    """
    Configuration for the [balances](https://docs.stripe.com/connect/supported-embedded-components/balances/) embedded component.
    """
    disputes_list: NotRequired[
        "AccountSessionCreateParamsComponentsDisputesList"
    ]
    """
    Configuration for the [disputes list](https://docs.stripe.com/connect/supported-embedded-components/disputes-list/) embedded component.
    """
    documents: NotRequired["AccountSessionCreateParamsComponentsDocuments"]
    """
    Configuration for the [documents](https://docs.stripe.com/connect/supported-embedded-components/documents/) embedded component.
    """
    financial_account: NotRequired[
        "AccountSessionCreateParamsComponentsFinancialAccount"
    ]
    """
    Configuration for the [financial account](https://docs.stripe.com/connect/supported-embedded-components/financial-account/) embedded component.
    """
    financial_account_transactions: NotRequired[
        "AccountSessionCreateParamsComponentsFinancialAccountTransactions"
    ]
    """
    Configuration for the [financial account transactions](https://docs.stripe.com/connect/supported-embedded-components/financial-account-transactions/) embedded component.
    """
    instant_payouts_promotion: NotRequired[
        "AccountSessionCreateParamsComponentsInstantPayoutsPromotion"
    ]
    """
    Configuration for the [instant payouts promotion](https://docs.stripe.com/connect/supported-embedded-components/instant-payouts-promotion/) embedded component.
    """
    issuing_card: NotRequired[
        "AccountSessionCreateParamsComponentsIssuingCard"
    ]
    """
    Configuration for the [issuing card](https://docs.stripe.com/connect/supported-embedded-components/issuing-card/) embedded component.
    """
    issuing_cards_list: NotRequired[
        "AccountSessionCreateParamsComponentsIssuingCardsList"
    ]
    """
    Configuration for the [issuing cards list](https://docs.stripe.com/connect/supported-embedded-components/issuing-cards-list/) embedded component.
    """
    notification_banner: NotRequired[
        "AccountSessionCreateParamsComponentsNotificationBanner"
    ]
    """
    Configuration for the [notification banner](https://docs.stripe.com/connect/supported-embedded-components/notification-banner/) embedded component.
    """
    payment_details: NotRequired[
        "AccountSessionCreateParamsComponentsPaymentDetails"
    ]
    """
    Configuration for the [payment details](https://docs.stripe.com/connect/supported-embedded-components/payment-details/) embedded component.
    """
    payment_disputes: NotRequired[
        "AccountSessionCreateParamsComponentsPaymentDisputes"
    ]
    """
    Configuration for the [payment disputes](https://docs.stripe.com/connect/supported-embedded-components/payment-disputes/) embedded component.
    """
    payments: NotRequired["AccountSessionCreateParamsComponentsPayments"]
    """
    Configuration for the [payments](https://docs.stripe.com/connect/supported-embedded-components/payments/) embedded component.
    """
    payout_details: NotRequired[
        "AccountSessionCreateParamsComponentsPayoutDetails"
    ]
    """
    Configuration for the [payout details](https://docs.stripe.com/connect/supported-embedded-components/payout-details/) embedded component.
    """
    payouts: NotRequired["AccountSessionCreateParamsComponentsPayouts"]
    """
    Configuration for the [payouts](https://docs.stripe.com/connect/supported-embedded-components/payouts/) embedded component.
    """
    payouts_list: NotRequired[
        "AccountSessionCreateParamsComponentsPayoutsList"
    ]
    """
    Configuration for the [payouts list](https://docs.stripe.com/connect/supported-embedded-components/payouts-list/) embedded component.
    """
    tax_registrations: NotRequired[
        "AccountSessionCreateParamsComponentsTaxRegistrations"
    ]
    """
    Configuration for the [tax registrations](https://docs.stripe.com/connect/supported-embedded-components/tax-registrations/) embedded component.
    """
    tax_settings: NotRequired[
        "AccountSessionCreateParamsComponentsTaxSettings"
    ]
    """
    Configuration for the [tax settings](https://docs.stripe.com/connect/supported-embedded-components/tax-settings/) embedded component.
    """


class AccountSessionCreateParamsComponentsAccountManagement(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsAccountManagementFeatures"
    ]
    """
    The list of features enabled in the embedded component.
    """


class AccountSessionCreateParamsComponentsAccountManagementFeatures(TypedDict):
    disable_stripe_user_authentication: NotRequired[bool]
    """
    Whether Stripe user authentication is disabled. This value can only be `true` for accounts where `controller.requirement_collection` is `application` for the account. The default value is the opposite of the `external_account_collection` value. For example, if you don't set `external_account_collection`, it defaults to `true` and `disable_stripe_user_authentication` defaults to `false`.
    """
    external_account_collection: NotRequired[bool]
    """
    Whether external account collection is enabled. This feature can only be `false` for accounts where you're responsible for collecting updated information when requirements are due or change, like Custom accounts. The default value for this feature is `true`.
    """


class AccountSessionCreateParamsComponentsAccountOnboarding(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsAccountOnboardingFeatures"
    ]
    """
    The list of features enabled in the embedded component.
    """


class AccountSessionCreateParamsComponentsAccountOnboardingFeatures(TypedDict):
    disable_stripe_user_authentication: NotRequired[bool]
    """
    Whether Stripe user authentication is disabled. This value can only be `true` for accounts where `controller.requirement_collection` is `application` for the account. The default value is the opposite of the `external_account_collection` value. For example, if you don't set `external_account_collection`, it defaults to `true` and `disable_stripe_user_authentication` defaults to `false`.
    """
    external_account_collection: NotRequired[bool]
    """
    Whether external account collection is enabled. This feature can only be `false` for accounts where you're responsible for collecting updated information when requirements are due or change, like Custom accounts. The default value for this feature is `true`.
    """


class AccountSessionCreateParamsComponentsBalances(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsBalancesFeatures"
    ]
    """
    The list of features enabled in the embedded component.
    """


class AccountSessionCreateParamsComponentsBalancesFeatures(TypedDict):
    disable_stripe_user_authentication: NotRequired[bool]
    """
    Whether Stripe user authentication is disabled. This value can only be `true` for accounts where `controller.requirement_collection` is `application` for the account. The default value is the opposite of the `external_account_collection` value. For example, if you don't set `external_account_collection`, it defaults to `true` and `disable_stripe_user_authentication` defaults to `false`.
    """
    edit_payout_schedule: NotRequired[bool]
    """
    Whether to allow payout schedule to be changed. Defaults to `true` when `controller.losses.payments` is set to `stripe` for the account, otherwise `false`.
    """
    external_account_collection: NotRequired[bool]
    """
    Whether external account collection is enabled. This feature can only be `false` for accounts where you're responsible for collecting updated information when requirements are due or change, like Custom accounts. The default value for this feature is `true`.
    """
    instant_payouts: NotRequired[bool]
    """
    Whether instant payouts are enabled for this component.
    """
    standard_payouts: NotRequired[bool]
    """
    Whether to allow creation of standard payouts. Defaults to `true` when `controller.losses.payments` is set to `stripe` for the account, otherwise `false`.
    """


class AccountSessionCreateParamsComponentsDisputesList(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsDisputesListFeatures"
    ]
    """
    The list of features enabled in the embedded component.
    """


class AccountSessionCreateParamsComponentsDisputesListFeatures(TypedDict):
    capture_payments: NotRequired[bool]
    """
    Whether to allow capturing and cancelling payment intents. This is `true` by default.
    """
    destination_on_behalf_of_charge_management: NotRequired[bool]
    """
    Whether connected accounts can manage destination charges that are created on behalf of them. This is `false` by default.
    """
    dispute_management: NotRequired[bool]
    """
    Whether responding to disputes is enabled, including submitting evidence and accepting disputes. This is `true` by default.
    """
    refund_management: NotRequired[bool]
    """
    Whether sending refunds is enabled. This is `true` by default.
    """


class AccountSessionCreateParamsComponentsDocuments(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsDocumentsFeatures"
    ]
    """
    An empty list, because this embedded component has no features.
    """


class AccountSessionCreateParamsComponentsDocumentsFeatures(TypedDict):
    pass


class AccountSessionCreateParamsComponentsFinancialAccount(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsFinancialAccountFeatures"
    ]
    """
    The list of features enabled in the embedded component.
    """


class AccountSessionCreateParamsComponentsFinancialAccountFeatures(TypedDict):
    disable_stripe_user_authentication: NotRequired[bool]
    """
    Whether Stripe user authentication is disabled. This value can only be `true` for accounts where `controller.requirement_collection` is `application` for the account. The default value is the opposite of the `external_account_collection` value. For example, if you don't set `external_account_collection`, it defaults to `true` and `disable_stripe_user_authentication` defaults to `false`.
    """
    external_account_collection: NotRequired[bool]
    """
    Whether external account collection is enabled. This feature can only be `false` for accounts where you're responsible for collecting updated information when requirements are due or change, like Custom accounts. The default value for this feature is `true`.
    """
    send_money: NotRequired[bool]
    """
    Whether to allow sending money.
    """
    transfer_balance: NotRequired[bool]
    """
    Whether to allow transferring balance.
    """


class AccountSessionCreateParamsComponentsFinancialAccountTransactions(
    TypedDict,
):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsFinancialAccountTransactionsFeatures"
    ]
    """
    The list of features enabled in the embedded component.
    """


class AccountSessionCreateParamsComponentsFinancialAccountTransactionsFeatures(
    TypedDict,
):
    card_spend_dispute_management: NotRequired[bool]
    """
    Whether to allow card spend dispute management features.
    """


class AccountSessionCreateParamsComponentsInstantPayoutsPromotion(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsInstantPayoutsPromotionFeatures"
    ]
    """
    The list of features enabled in the embedded component.
    """


class AccountSessionCreateParamsComponentsInstantPayoutsPromotionFeatures(
    TypedDict,
):
    disable_stripe_user_authentication: NotRequired[bool]
    """
    Whether Stripe user authentication is disabled. This value can only be `true` for accounts where `controller.requirement_collection` is `application` for the account. The default value is the opposite of the `external_account_collection` value. For example, if you don't set `external_account_collection`, it defaults to `true` and `disable_stripe_user_authentication` defaults to `false`.
    """
    external_account_collection: NotRequired[bool]
    """
    Whether external account collection is enabled. This feature can only be `false` for accounts where you're responsible for collecting updated information when requirements are due or change, like Custom accounts. The default value for this feature is `true`.
    """
    instant_payouts: NotRequired[bool]
    """
    Whether instant payouts are enabled for this component.
    """


class AccountSessionCreateParamsComponentsIssuingCard(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsIssuingCardFeatures"
    ]
    """
    The list of features enabled in the embedded component.
    """


class AccountSessionCreateParamsComponentsIssuingCardFeatures(TypedDict):
    card_management: NotRequired[bool]
    """
    Whether to allow card management features.
    """
    card_spend_dispute_management: NotRequired[bool]
    """
    Whether to allow card spend dispute management features.
    """
    cardholder_management: NotRequired[bool]
    """
    Whether to allow cardholder management features.
    """
    spend_control_management: NotRequired[bool]
    """
    Whether to allow spend control management features.
    """


class AccountSessionCreateParamsComponentsIssuingCardsList(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsIssuingCardsListFeatures"
    ]
    """
    The list of features enabled in the embedded component.
    """


class AccountSessionCreateParamsComponentsIssuingCardsListFeatures(TypedDict):
    card_management: NotRequired[bool]
    """
    Whether to allow card management features.
    """
    card_spend_dispute_management: NotRequired[bool]
    """
    Whether to allow card spend dispute management features.
    """
    cardholder_management: NotRequired[bool]
    """
    Whether to allow cardholder management features.
    """
    disable_stripe_user_authentication: NotRequired[bool]
    """
    Whether Stripe user authentication is disabled. This value can only be `true` for accounts where `controller.requirement_collection` is `application` for the account. The default value is the opposite of the `external_account_collection` value. For example, if you don't set `external_account_collection`, it defaults to `true` and `disable_stripe_user_authentication` defaults to `false`.
    """
    spend_control_management: NotRequired[bool]
    """
    Whether to allow spend control management features.
    """


class AccountSessionCreateParamsComponentsNotificationBanner(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsNotificationBannerFeatures"
    ]
    """
    The list of features enabled in the embedded component.
    """


class AccountSessionCreateParamsComponentsNotificationBannerFeatures(
    TypedDict
):
    disable_stripe_user_authentication: NotRequired[bool]
    """
    Whether Stripe user authentication is disabled. This value can only be `true` for accounts where `controller.requirement_collection` is `application` for the account. The default value is the opposite of the `external_account_collection` value. For example, if you don't set `external_account_collection`, it defaults to `true` and `disable_stripe_user_authentication` defaults to `false`.
    """
    external_account_collection: NotRequired[bool]
    """
    Whether external account collection is enabled. This feature can only be `false` for accounts where you're responsible for collecting updated information when requirements are due or change, like Custom accounts. The default value for this feature is `true`.
    """


class AccountSessionCreateParamsComponentsPaymentDetails(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsPaymentDetailsFeatures"
    ]
    """
    The list of features enabled in the embedded component.
    """


class AccountSessionCreateParamsComponentsPaymentDetailsFeatures(TypedDict):
    capture_payments: NotRequired[bool]
    """
    Whether to allow capturing and cancelling payment intents. This is `true` by default.
    """
    destination_on_behalf_of_charge_management: NotRequired[bool]
    """
    Whether connected accounts can manage destination charges that are created on behalf of them. This is `false` by default.
    """
    dispute_management: NotRequired[bool]
    """
    Whether responding to disputes is enabled, including submitting evidence and accepting disputes. This is `true` by default.
    """
    refund_management: NotRequired[bool]
    """
    Whether sending refunds is enabled. This is `true` by default.
    """


class AccountSessionCreateParamsComponentsPaymentDisputes(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsPaymentDisputesFeatures"
    ]
    """
    The list of features enabled in the embedded component.
    """


class AccountSessionCreateParamsComponentsPaymentDisputesFeatures(TypedDict):
    destination_on_behalf_of_charge_management: NotRequired[bool]
    """
    Whether connected accounts can manage destination charges that are created on behalf of them. This is `false` by default.
    """
    dispute_management: NotRequired[bool]
    """
    Whether responding to disputes is enabled, including submitting evidence and accepting disputes. This is `true` by default.
    """
    refund_management: NotRequired[bool]
    """
    Whether sending refunds is enabled. This is `true` by default.
    """


class AccountSessionCreateParamsComponentsPayments(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsPaymentsFeatures"
    ]
    """
    The list of features enabled in the embedded component.
    """


class AccountSessionCreateParamsComponentsPaymentsFeatures(TypedDict):
    capture_payments: NotRequired[bool]
    """
    Whether to allow capturing and cancelling payment intents. This is `true` by default.
    """
    destination_on_behalf_of_charge_management: NotRequired[bool]
    """
    Whether connected accounts can manage destination charges that are created on behalf of them. This is `false` by default.
    """
    dispute_management: NotRequired[bool]
    """
    Whether responding to disputes is enabled, including submitting evidence and accepting disputes. This is `true` by default.
    """
    refund_management: NotRequired[bool]
    """
    Whether sending refunds is enabled. This is `true` by default.
    """


class AccountSessionCreateParamsComponentsPayoutDetails(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsPayoutDetailsFeatures"
    ]
    """
    An empty list, because this embedded component has no features.
    """


class AccountSessionCreateParamsComponentsPayoutDetailsFeatures(TypedDict):
    pass


class AccountSessionCreateParamsComponentsPayouts(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsPayoutsFeatures"
    ]
    """
    The list of features enabled in the embedded component.
    """


class AccountSessionCreateParamsComponentsPayoutsFeatures(TypedDict):
    disable_stripe_user_authentication: NotRequired[bool]
    """
    Whether Stripe user authentication is disabled. This value can only be `true` for accounts where `controller.requirement_collection` is `application` for the account. The default value is the opposite of the `external_account_collection` value. For example, if you don't set `external_account_collection`, it defaults to `true` and `disable_stripe_user_authentication` defaults to `false`.
    """
    edit_payout_schedule: NotRequired[bool]
    """
    Whether to allow payout schedule to be changed. Defaults to `true` when `controller.losses.payments` is set to `stripe` for the account, otherwise `false`.
    """
    external_account_collection: NotRequired[bool]
    """
    Whether external account collection is enabled. This feature can only be `false` for accounts where you're responsible for collecting updated information when requirements are due or change, like Custom accounts. The default value for this feature is `true`.
    """
    instant_payouts: NotRequired[bool]
    """
    Whether instant payouts are enabled for this component.
    """
    standard_payouts: NotRequired[bool]
    """
    Whether to allow creation of standard payouts. Defaults to `true` when `controller.losses.payments` is set to `stripe` for the account, otherwise `false`.
    """


class AccountSessionCreateParamsComponentsPayoutsList(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsPayoutsListFeatures"
    ]
    """
    An empty list, because this embedded component has no features.
    """


class AccountSessionCreateParamsComponentsPayoutsListFeatures(TypedDict):
    pass


class AccountSessionCreateParamsComponentsTaxRegistrations(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsTaxRegistrationsFeatures"
    ]
    """
    An empty list, because this embedded component has no features.
    """


class AccountSessionCreateParamsComponentsTaxRegistrationsFeatures(TypedDict):
    pass


class AccountSessionCreateParamsComponentsTaxSettings(TypedDict):
    enabled: bool
    """
    Whether the embedded component is enabled.
    """
    features: NotRequired[
        "AccountSessionCreateParamsComponentsTaxSettingsFeatures"
    ]
    """
    An empty list, because this embedded component has no features.
    """


class AccountSessionCreateParamsComponentsTaxSettingsFeatures(TypedDict):
    pass
