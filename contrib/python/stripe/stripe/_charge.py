# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._nested_resource_class_methods import nested_resource_class_methods
from stripe._search_result_object import SearchResultObject
from stripe._searchable_api_resource import SearchableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import (
    AsyncIterator,
    ClassVar,
    Dict,
    Iterator,
    List,
    Optional,
    Union,
    cast,
    overload,
)
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._account import Account
    from stripe._application import Application
    from stripe._application_fee import ApplicationFee
    from stripe._balance_transaction import BalanceTransaction
    from stripe._bank_account import BankAccount
    from stripe._card import Card as CardResource
    from stripe._customer import Customer
    from stripe._mandate import Mandate
    from stripe._payment_intent import PaymentIntent
    from stripe._payment_method import PaymentMethod
    from stripe._refund import Refund
    from stripe._review import Review
    from stripe._source import Source
    from stripe._transfer import Transfer
    from stripe.params._charge_capture_params import ChargeCaptureParams
    from stripe.params._charge_create_params import ChargeCreateParams
    from stripe.params._charge_list_params import ChargeListParams
    from stripe.params._charge_list_refunds_params import (
        ChargeListRefundsParams,
    )
    from stripe.params._charge_modify_params import ChargeModifyParams
    from stripe.params._charge_retrieve_params import ChargeRetrieveParams
    from stripe.params._charge_retrieve_refund_params import (
        ChargeRetrieveRefundParams,
    )
    from stripe.params._charge_search_params import ChargeSearchParams


@nested_resource_class_methods("refund")
class Charge(
    CreateableAPIResource["Charge"],
    ListableAPIResource["Charge"],
    SearchableAPIResource["Charge"],
    UpdateableAPIResource["Charge"],
):
    """
    The `Charge` object represents a single attempt to move money into your Stripe account.
    PaymentIntent confirmation is the most common way to create Charges, but [Account Debits](https://docs.stripe.com/connect/account-debits) may also create Charges.
    Some legacy payment flows create Charges directly, which is not recommended for new integrations.
    """

    OBJECT_NAME: ClassVar[Literal["charge"]] = "charge"

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

        address: Optional[Address]
        """
        Billing address.
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
        tax_id: Optional[str]
        """
        Taxpayer identification number. Used only for transactions between LATAM buyers and non-LATAM sellers.
        """
        _inner_class_types = {"address": Address}

    class FraudDetails(StripeObject):
        stripe_report: Optional[str]
        """
        Assessments from Stripe. If set, the value is `fraudulent`.
        """
        user_report: Optional[str]
        """
        Assessments reported by you. If set, possible values of are `safe` and `fraudulent`.
        """

    class Level3(StripeObject):
        class LineItem(StripeObject):
            discount_amount: Optional[int]
            product_code: str
            product_description: str
            quantity: Optional[int]
            tax_amount: Optional[int]
            unit_cost: Optional[int]

        customer_reference: Optional[str]
        line_items: List[LineItem]
        merchant_reference: str
        shipping_address_zip: Optional[str]
        shipping_amount: Optional[int]
        shipping_from_zip: Optional[str]
        _inner_class_types = {"line_items": LineItem}

    class Outcome(StripeObject):
        class Rule(StripeObject):
            action: str
            """
            The action taken on the payment.
            """
            id: str
            """
            Unique identifier for the object.
            """
            predicate: str
            """
            The predicate to evaluate the payment against.
            """

        advice_code: Optional[
            Literal["confirm_card_data", "do_not_try_again", "try_again_later"]
        ]
        """
        An enumerated value providing a more detailed explanation on [how to proceed with an error](https://docs.stripe.com/declines#retrying-issuer-declines).
        """
        network_advice_code: Optional[str]
        """
        For charges declined by the network, a 2 digit code which indicates the advice returned by the network on how to proceed with an error.
        """
        network_decline_code: Optional[str]
        """
        For charges declined by the network, an alphanumeric code which indicates the reason the charge failed.
        """
        network_status: Optional[str]
        """
        Possible values are `approved_by_network`, `declined_by_network`, `not_sent_to_network`, and `reversed_after_approval`. The value `reversed_after_approval` indicates the payment was [blocked by Stripe](https://docs.stripe.com/declines#blocked-payments) after bank authorization, and may temporarily appear as "pending" on a cardholder's statement.
        """
        reason: Optional[str]
        """
        An enumerated value providing a more detailed explanation of the outcome's `type`. Charges blocked by Radar's default block rule have the value `highest_risk_level`. Charges placed in review by Radar's default review rule have the value `elevated_risk_level`. Charges blocked because the payment is unlikely to be authorized have the value `low_probability_of_authorization`. Charges authorized, blocked, or placed in review by custom rules have the value `rule`. See [understanding declines](https://docs.stripe.com/declines) for more details.
        """
        risk_level: Optional[str]
        """
        Stripe Radar's evaluation of the riskiness of the payment. Possible values for evaluated payments are `normal`, `elevated`, `highest`. For non-card payments, and card-based payments predating the public assignment of risk levels, this field will have the value `not_assessed`. In the event of an error in the evaluation, this field will have the value `unknown`. This field is only available with Radar.
        """
        risk_score: Optional[int]
        """
        Stripe Radar's evaluation of the riskiness of the payment. Possible values for evaluated payments are between 0 and 100. For non-card payments, card-based payments predating the public assignment of risk scores, or in the event of an error during evaluation, this field will not be present. This field is only available with Radar for Fraud Teams.
        """
        rule: Optional[ExpandableField[Rule]]
        """
        The ID of the Radar rule that matched the payment, if applicable.
        """
        seller_message: Optional[str]
        """
        A human-readable description of the outcome type and reason, designed for you (the recipient of the payment), not your customer.
        """
        type: str
        """
        Possible values are `authorized`, `manual_review`, `issuer_declined`, `blocked`, and `invalid`. See [understanding declines](https://docs.stripe.com/declines) and [Radar reviews](https://docs.stripe.com/radar/reviews) for details.
        """
        _inner_class_types = {"rule": Rule}

    class PaymentMethodDetails(StripeObject):
        class AchCreditTransfer(StripeObject):
            account_number: Optional[str]
            """
            Account number to transfer funds to.
            """
            bank_name: Optional[str]
            """
            Name of the bank associated with the routing number.
            """
            routing_number: Optional[str]
            """
            Routing transit number for the bank account to transfer funds to.
            """
            swift_code: Optional[str]
            """
            SWIFT code of the bank associated with the routing number.
            """

        class AchDebit(StripeObject):
            account_holder_type: Optional[Literal["company", "individual"]]
            """
            Type of entity that holds the account. This can be either `individual` or `company`.
            """
            bank_name: Optional[str]
            """
            Name of the bank associated with the bank account.
            """
            country: Optional[str]
            """
            Two-letter ISO code representing the country the bank account is located in.
            """
            fingerprint: Optional[str]
            """
            Uniquely identifies this particular bank account. You can use this attribute to check whether two bank accounts are the same.
            """
            last4: Optional[str]
            """
            Last four digits of the bank account number.
            """
            routing_number: Optional[str]
            """
            Routing transit number of the bank account.
            """

        class AcssDebit(StripeObject):
            bank_name: Optional[str]
            """
            Name of the bank associated with the bank account.
            """
            expected_debit_date: Optional[str]
            """
            Estimated date to debit the customer's bank account. A date string in YYYY-MM-DD format.
            """
            fingerprint: Optional[str]
            """
            Uniquely identifies this particular bank account. You can use this attribute to check whether two bank accounts are the same.
            """
            institution_number: Optional[str]
            """
            Institution number of the bank account
            """
            last4: Optional[str]
            """
            Last four digits of the bank account number.
            """
            mandate: Optional[str]
            """
            ID of the mandate used to make this payment.
            """
            transit_number: Optional[str]
            """
            Transit number of the bank account.
            """

        class Affirm(StripeObject):
            location: Optional[str]
            """
            ID of the [location](https://docs.stripe.com/api/terminal/locations) that this transaction's reader is assigned to.
            """
            reader: Optional[str]
            """
            ID of the [reader](https://docs.stripe.com/api/terminal/readers) this transaction was made on.
            """
            transaction_id: Optional[str]
            """
            The Affirm transaction ID associated with this payment.
            """

        class AfterpayClearpay(StripeObject):
            order_id: Optional[str]
            """
            The Afterpay order ID associated with this payment intent.
            """
            reference: Optional[str]
            """
            Order identifier shown to the merchant in Afterpay's online portal.
            """

        class Alipay(StripeObject):
            buyer_id: Optional[str]
            """
            Uniquely identifies this particular Alipay account. You can use this attribute to check whether two Alipay accounts are the same.
            """
            fingerprint: Optional[str]
            """
            Uniquely identifies this particular Alipay account. You can use this attribute to check whether two Alipay accounts are the same.
            """
            transaction_id: Optional[str]
            """
            Transaction ID of this particular Alipay transaction.
            """

        class Alma(StripeObject):
            class Installments(StripeObject):
                count: int
                """
                The number of installments.
                """

            installments: Optional[Installments]
            transaction_id: Optional[str]
            """
            The Alma transaction ID associated with this payment.
            """
            _inner_class_types = {"installments": Installments}

        class AmazonPay(StripeObject):
            class Funding(StripeObject):
                class Card(StripeObject):
                    brand: Optional[str]
                    """
                    Card brand. Can be `amex`, `cartes_bancaires`, `diners`, `discover`, `eftpos_au`, `jcb`, `link`, `mastercard`, `unionpay`, `visa` or `unknown`.
                    """
                    country: Optional[str]
                    """
                    Two-letter ISO code representing the country of the card. You could use this attribute to get a sense of the international breakdown of cards you've collected.
                    """
                    exp_month: Optional[int]
                    """
                    Two-digit number representing the card's expiration month.
                    """
                    exp_year: Optional[int]
                    """
                    Four-digit number representing the card's expiration year.
                    """
                    funding: Optional[str]
                    """
                    Card funding type. Can be `credit`, `debit`, `prepaid`, or `unknown`.
                    """
                    last4: Optional[str]
                    """
                    The last four digits of the card.
                    """

                card: Optional[Card]
                type: Optional[Literal["card"]]
                """
                funding type of the underlying payment method.
                """
                _inner_class_types = {"card": Card}

            funding: Optional[Funding]
            transaction_id: Optional[str]
            """
            The Amazon Pay transaction ID associated with this payment.
            """
            _inner_class_types = {"funding": Funding}

        class AuBecsDebit(StripeObject):
            bsb_number: Optional[str]
            """
            Bank-State-Branch number of the bank account.
            """
            expected_debit_date: Optional[str]
            """
            Estimated date to debit the customer's bank account. A date string in YYYY-MM-DD format.
            """
            fingerprint: Optional[str]
            """
            Uniquely identifies this particular bank account. You can use this attribute to check whether two bank accounts are the same.
            """
            last4: Optional[str]
            """
            Last four digits of the bank account number.
            """
            mandate: Optional[str]
            """
            ID of the mandate used to make this payment.
            """

        class BacsDebit(StripeObject):
            expected_debit_date: Optional[str]
            """
            Estimated date to debit the customer's bank account. A date string in YYYY-MM-DD format.
            """
            fingerprint: Optional[str]
            """
            Uniquely identifies this particular bank account. You can use this attribute to check whether two bank accounts are the same.
            """
            last4: Optional[str]
            """
            Last four digits of the bank account number.
            """
            mandate: Optional[str]
            """
            ID of the mandate used to make this payment.
            """
            sort_code: Optional[str]
            """
            Sort code of the bank account. (e.g., `10-20-30`)
            """

        class Bancontact(StripeObject):
            bank_code: Optional[str]
            """
            Bank code of bank associated with the bank account.
            """
            bank_name: Optional[str]
            """
            Name of the bank associated with the bank account.
            """
            bic: Optional[str]
            """
            Bank Identifier Code of the bank associated with the bank account.
            """
            generated_sepa_debit: Optional[ExpandableField["PaymentMethod"]]
            """
            The ID of the SEPA Direct Debit PaymentMethod which was generated by this Charge.
            """
            generated_sepa_debit_mandate: Optional[ExpandableField["Mandate"]]
            """
            The mandate for the SEPA Direct Debit PaymentMethod which was generated by this Charge.
            """
            iban_last4: Optional[str]
            """
            Last four characters of the IBAN.
            """
            preferred_language: Optional[Literal["de", "en", "fr", "nl"]]
            """
            Preferred language of the Bancontact authorization page that the customer is redirected to.
            Can be one of `en`, `de`, `fr`, or `nl`
            """
            verified_name: Optional[str]
            """
            Owner's verified full name. Values are verified or provided by Bancontact directly
            (if supported) at the time of authorization or settlement. They cannot be set or mutated.
            """

        class Billie(StripeObject):
            transaction_id: Optional[str]
            """
            The Billie transaction ID associated with this payment.
            """

        class Blik(StripeObject):
            buyer_id: Optional[str]
            """
            A unique and immutable identifier assigned by BLIK to every buyer.
            """

        class Boleto(StripeObject):
            tax_id: str
            """
            The tax ID of the customer (CPF for individuals consumers or CNPJ for businesses consumers)
            """

        class Card(StripeObject):
            class Checks(StripeObject):
                address_line1_check: Optional[str]
                """
                If a address line1 was provided, results of the check, one of `pass`, `fail`, `unavailable`, or `unchecked`.
                """
                address_postal_code_check: Optional[str]
                """
                If a address postal code was provided, results of the check, one of `pass`, `fail`, `unavailable`, or `unchecked`.
                """
                cvc_check: Optional[str]
                """
                If a CVC was provided, results of the check, one of `pass`, `fail`, `unavailable`, or `unchecked`.
                """

            class ExtendedAuthorization(StripeObject):
                status: Literal["disabled", "enabled"]
                """
                Indicates whether or not the capture window is extended beyond the standard authorization.
                """

            class IncrementalAuthorization(StripeObject):
                status: Literal["available", "unavailable"]
                """
                Indicates whether or not the incremental authorization feature is supported.
                """

            class Installments(StripeObject):
                class Plan(StripeObject):
                    count: Optional[int]
                    """
                    For `fixed_count` installment plans, this is the number of installment payments your customer will make to their credit card.
                    """
                    interval: Optional[Literal["month"]]
                    """
                    For `fixed_count` installment plans, this is the interval between installment payments your customer will make to their credit card.
                    One of `month`.
                    """
                    type: Literal["bonus", "fixed_count", "revolving"]
                    """
                    Type of installment plan, one of `fixed_count`, `bonus`, or `revolving`.
                    """

                plan: Optional[Plan]
                """
                Installment plan selected for the payment.
                """
                _inner_class_types = {"plan": Plan}

            class Multicapture(StripeObject):
                status: Literal["available", "unavailable"]
                """
                Indicates whether or not multiple captures are supported.
                """

            class NetworkToken(StripeObject):
                used: bool
                """
                Indicates if Stripe used a network token, either user provided or Stripe managed when processing the transaction.
                """

            class Overcapture(StripeObject):
                maximum_amount_capturable: int
                """
                The maximum amount that can be captured.
                """
                status: Literal["available", "unavailable"]
                """
                Indicates whether or not the authorized amount can be over-captured.
                """

            class ThreeDSecure(StripeObject):
                authentication_flow: Optional[
                    Literal["challenge", "frictionless"]
                ]
                """
                For authenticated transactions: how the customer was authenticated by
                the issuing bank.
                """
                electronic_commerce_indicator: Optional[
                    Literal["01", "02", "05", "06", "07"]
                ]
                """
                The Electronic Commerce Indicator (ECI). A protocol-level field
                indicating what degree of authentication was performed.
                """
                exemption_indicator: Optional[Literal["low_risk", "none"]]
                """
                The exemption requested via 3DS and accepted by the issuer at authentication time.
                """
                exemption_indicator_applied: Optional[bool]
                """
                Whether Stripe requested the value of `exemption_indicator` in the transaction. This will depend on
                the outcome of Stripe's internal risk assessment.
                """
                result: Optional[
                    Literal[
                        "attempt_acknowledged",
                        "authenticated",
                        "exempted",
                        "failed",
                        "not_supported",
                        "processing_error",
                    ]
                ]
                """
                Indicates the outcome of 3D Secure authentication.
                """
                result_reason: Optional[
                    Literal[
                        "abandoned",
                        "bypassed",
                        "canceled",
                        "card_not_enrolled",
                        "network_not_supported",
                        "protocol_error",
                        "rejected",
                    ]
                ]
                """
                Additional information about why 3D Secure succeeded or failed based
                on the `result`.
                """
                transaction_id: Optional[str]
                """
                The 3D Secure 1 XID or 3D Secure 2 Directory Server Transaction ID
                (dsTransId) for this payment.
                """
                version: Optional[
                    Literal["1.0.2", "2.1.0", "2.2.0", "2.3.0", "2.3.1"]
                ]
                """
                The version of 3D Secure that was used.
                """

            class Wallet(StripeObject):
                class AmexExpressCheckout(StripeObject):
                    pass

                class ApplePay(StripeObject):
                    pass

                class GooglePay(StripeObject):
                    pass

                class Link(StripeObject):
                    pass

                class Masterpass(StripeObject):
                    class BillingAddress(StripeObject):
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

                    class ShippingAddress(StripeObject):
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

                    billing_address: Optional[BillingAddress]
                    """
                    Owner's verified billing address. Values are verified or provided by the wallet directly (if supported) at the time of authorization or settlement. They cannot be set or mutated.
                    """
                    email: Optional[str]
                    """
                    Owner's verified email. Values are verified or provided by the wallet directly (if supported) at the time of authorization or settlement. They cannot be set or mutated.
                    """
                    name: Optional[str]
                    """
                    Owner's verified full name. Values are verified or provided by the wallet directly (if supported) at the time of authorization or settlement. They cannot be set or mutated.
                    """
                    shipping_address: Optional[ShippingAddress]
                    """
                    Owner's verified shipping address. Values are verified or provided by the wallet directly (if supported) at the time of authorization or settlement. They cannot be set or mutated.
                    """
                    _inner_class_types = {
                        "billing_address": BillingAddress,
                        "shipping_address": ShippingAddress,
                    }

                class SamsungPay(StripeObject):
                    pass

                class VisaCheckout(StripeObject):
                    class BillingAddress(StripeObject):
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

                    class ShippingAddress(StripeObject):
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

                    billing_address: Optional[BillingAddress]
                    """
                    Owner's verified billing address. Values are verified or provided by the wallet directly (if supported) at the time of authorization or settlement. They cannot be set or mutated.
                    """
                    email: Optional[str]
                    """
                    Owner's verified email. Values are verified or provided by the wallet directly (if supported) at the time of authorization or settlement. They cannot be set or mutated.
                    """
                    name: Optional[str]
                    """
                    Owner's verified full name. Values are verified or provided by the wallet directly (if supported) at the time of authorization or settlement. They cannot be set or mutated.
                    """
                    shipping_address: Optional[ShippingAddress]
                    """
                    Owner's verified shipping address. Values are verified or provided by the wallet directly (if supported) at the time of authorization or settlement. They cannot be set or mutated.
                    """
                    _inner_class_types = {
                        "billing_address": BillingAddress,
                        "shipping_address": ShippingAddress,
                    }

                amex_express_checkout: Optional[AmexExpressCheckout]
                apple_pay: Optional[ApplePay]
                dynamic_last4: Optional[str]
                """
                (For tokenized numbers only.) The last four digits of the device account number.
                """
                google_pay: Optional[GooglePay]
                link: Optional[Link]
                masterpass: Optional[Masterpass]
                samsung_pay: Optional[SamsungPay]
                type: Literal[
                    "amex_express_checkout",
                    "apple_pay",
                    "google_pay",
                    "link",
                    "masterpass",
                    "samsung_pay",
                    "visa_checkout",
                ]
                """
                The type of the card wallet, one of `amex_express_checkout`, `apple_pay`, `google_pay`, `masterpass`, `samsung_pay`, `visa_checkout`, or `link`. An additional hash is included on the Wallet subhash with a name matching this value. It contains additional information specific to the card wallet type.
                """
                visa_checkout: Optional[VisaCheckout]
                _inner_class_types = {
                    "amex_express_checkout": AmexExpressCheckout,
                    "apple_pay": ApplePay,
                    "google_pay": GooglePay,
                    "link": Link,
                    "masterpass": Masterpass,
                    "samsung_pay": SamsungPay,
                    "visa_checkout": VisaCheckout,
                }

            amount_authorized: Optional[int]
            """
            The authorized amount.
            """
            authorization_code: Optional[str]
            """
            Authorization code on the charge.
            """
            brand: Optional[str]
            """
            Card brand. Can be `amex`, `cartes_bancaires`, `diners`, `discover`, `eftpos_au`, `jcb`, `link`, `mastercard`, `unionpay`, `visa` or `unknown`.
            """
            capture_before: Optional[int]
            """
            When using manual capture, a future timestamp at which the charge will be automatically refunded if uncaptured.
            """
            checks: Optional[Checks]
            """
            Check results by Card networks on Card address and CVC at time of payment.
            """
            country: Optional[str]
            """
            Two-letter ISO code representing the country of the card. You could use this attribute to get a sense of the international breakdown of cards you've collected.
            """
            description: Optional[str]
            """
            A high-level description of the type of cards issued in this range. (For internal use only and not typically available in standard API requests.)
            """
            exp_month: int
            """
            Two-digit number representing the card's expiration month.
            """
            exp_year: int
            """
            Four-digit number representing the card's expiration year.
            """
            extended_authorization: Optional[ExtendedAuthorization]
            fingerprint: Optional[str]
            """
            Uniquely identifies this particular card number. You can use this attribute to check whether two customers who've signed up with you are using the same card number, for example. For payment methods that tokenize card information (Apple Pay, Google Pay), the tokenized number might be provided instead of the underlying card number.

            *As of May 1, 2021, card fingerprint in India for Connect changed to allow two fingerprints for the same card---one for India and one for the rest of the world.*
            """
            funding: Optional[str]
            """
            Card funding type. Can be `credit`, `debit`, `prepaid`, or `unknown`.
            """
            iin: Optional[str]
            """
            Issuer identification number of the card. (For internal use only and not typically available in standard API requests.)
            """
            incremental_authorization: Optional[IncrementalAuthorization]
            installments: Optional[Installments]
            """
            Installment details for this payment.

            For more information, see the [installments integration guide](https://docs.stripe.com/payments/installments).
            """
            issuer: Optional[str]
            """
            The name of the card's issuing bank. (For internal use only and not typically available in standard API requests.)
            """
            last4: Optional[str]
            """
            The last four digits of the card.
            """
            mandate: Optional[str]
            """
            ID of the mandate used to make this payment or created by it.
            """
            moto: Optional[bool]
            """
            True if this payment was marked as MOTO and out of scope for SCA.
            """
            multicapture: Optional[Multicapture]
            network: Optional[str]
            """
            Identifies which network this charge was processed on. Can be `amex`, `cartes_bancaires`, `diners`, `discover`, `eftpos_au`, `interac`, `jcb`, `link`, `mastercard`, `unionpay`, `visa`, or `unknown`.
            """
            network_token: Optional[NetworkToken]
            """
            If this card has network token credentials, this contains the details of the network token credentials.
            """
            network_transaction_id: Optional[str]
            """
            This is used by the financial networks to identify a transaction. Visa calls this the Transaction ID, Mastercard calls this the Trace ID, and American Express calls this the Acquirer Reference Data. This value will be present if it is returned by the financial network in the authorization response, and null otherwise.
            """
            overcapture: Optional[Overcapture]
            regulated_status: Optional[Literal["regulated", "unregulated"]]
            """
            Status of a card based on the card issuer.
            """
            three_d_secure: Optional[ThreeDSecure]
            """
            Populated if this transaction used 3D Secure authentication.
            """
            wallet: Optional[Wallet]
            """
            If this Card is part of a card wallet, this contains the details of the card wallet.
            """
            _inner_class_types = {
                "checks": Checks,
                "extended_authorization": ExtendedAuthorization,
                "incremental_authorization": IncrementalAuthorization,
                "installments": Installments,
                "multicapture": Multicapture,
                "network_token": NetworkToken,
                "overcapture": Overcapture,
                "three_d_secure": ThreeDSecure,
                "wallet": Wallet,
            }

        class CardPresent(StripeObject):
            class Offline(StripeObject):
                stored_at: Optional[int]
                """
                Time at which the payment was collected while offline
                """
                type: Optional[Literal["deferred"]]
                """
                The method used to process this payment method offline. Only deferred is allowed.
                """

            class Receipt(StripeObject):
                account_type: Optional[
                    Literal["checking", "credit", "prepaid", "unknown"]
                ]
                """
                The type of account being debited or credited
                """
                application_cryptogram: Optional[str]
                """
                The Application Cryptogram, a unique value generated by the card to authenticate the transaction with issuers.
                """
                application_preferred_name: Optional[str]
                """
                The Application Identifier (AID) on the card used to determine which networks are eligible to process the transaction. Referenced from EMV tag 9F12, data encoded on the card's chip.
                """
                authorization_code: Optional[str]
                """
                Identifier for this transaction.
                """
                authorization_response_code: Optional[str]
                """
                EMV tag 8A. A code returned by the card issuer.
                """
                cardholder_verification_method: Optional[str]
                """
                Describes the method used by the cardholder to verify ownership of the card. One of the following: `approval`, `failure`, `none`, `offline_pin`, `offline_pin_and_signature`, `online_pin`, or `signature`.
                """
                dedicated_file_name: Optional[str]
                """
                Similar to the application_preferred_name, identifying the applications (AIDs) available on the card. Referenced from EMV tag 84.
                """
                terminal_verification_results: Optional[str]
                """
                A 5-byte string that records the checks and validations that occur between the card and the terminal. These checks determine how the terminal processes the transaction and what risk tolerance is acceptable. Referenced from EMV Tag 95.
                """
                transaction_status_information: Optional[str]
                """
                An indication of which steps were completed during the card read process. Referenced from EMV Tag 9B.
                """

            class Wallet(StripeObject):
                type: Literal[
                    "apple_pay", "google_pay", "samsung_pay", "unknown"
                ]
                """
                The type of mobile wallet, one of `apple_pay`, `google_pay`, `samsung_pay`, or `unknown`.
                """

            amount_authorized: Optional[int]
            """
            The authorized amount
            """
            brand: Optional[str]
            """
            Card brand. Can be `amex`, `cartes_bancaires`, `diners`, `discover`, `eftpos_au`, `jcb`, `link`, `mastercard`, `unionpay`, `visa` or `unknown`.
            """
            brand_product: Optional[str]
            """
            The [product code](https://stripe.com/docs/card-product-codes) that identifies the specific program or product associated with a card.
            """
            capture_before: Optional[int]
            """
            When using manual capture, a future timestamp after which the charge will be automatically refunded if uncaptured.
            """
            cardholder_name: Optional[str]
            """
            The cardholder name as read from the card, in [ISO 7813](https://en.wikipedia.org/wiki/ISO/IEC_7813) format. May include alphanumeric characters, special characters and first/last name separator (`/`). In some cases, the cardholder name may not be available depending on how the issuer has configured the card. Cardholder name is typically not available on swipe or contactless payments, such as those made with Apple Pay and Google Pay.
            """
            country: Optional[str]
            """
            Two-letter ISO code representing the country of the card. You could use this attribute to get a sense of the international breakdown of cards you've collected.
            """
            description: Optional[str]
            """
            A high-level description of the type of cards issued in this range. (For internal use only and not typically available in standard API requests.)
            """
            emv_auth_data: Optional[str]
            """
            Authorization response cryptogram.
            """
            exp_month: int
            """
            Two-digit number representing the card's expiration month.
            """
            exp_year: int
            """
            Four-digit number representing the card's expiration year.
            """
            fingerprint: Optional[str]
            """
            Uniquely identifies this particular card number. You can use this attribute to check whether two customers who've signed up with you are using the same card number, for example. For payment methods that tokenize card information (Apple Pay, Google Pay), the tokenized number might be provided instead of the underlying card number.

            *As of May 1, 2021, card fingerprint in India for Connect changed to allow two fingerprints for the same card---one for India and one for the rest of the world.*
            """
            funding: Optional[str]
            """
            Card funding type. Can be `credit`, `debit`, `prepaid`, or `unknown`.
            """
            generated_card: Optional[str]
            """
            ID of a card PaymentMethod generated from the card_present PaymentMethod that may be attached to a Customer for future transactions. Only present if it was possible to generate a card PaymentMethod.
            """
            iin: Optional[str]
            """
            Issuer identification number of the card. (For internal use only and not typically available in standard API requests.)
            """
            incremental_authorization_supported: bool
            """
            Whether this [PaymentIntent](https://docs.stripe.com/api/payment_intents) is eligible for incremental authorizations. Request support using [request_incremental_authorization_support](https://docs.stripe.com/api/payment_intents/create#create_payment_intent-payment_method_options-card_present-request_incremental_authorization_support).
            """
            issuer: Optional[str]
            """
            The name of the card's issuing bank. (For internal use only and not typically available in standard API requests.)
            """
            last4: Optional[str]
            """
            The last four digits of the card.
            """
            location: Optional[str]
            """
            ID of the [location](https://docs.stripe.com/api/terminal/locations) that this transaction's reader is assigned to.
            """
            network: Optional[str]
            """
            Identifies which network this charge was processed on. Can be `amex`, `cartes_bancaires`, `diners`, `discover`, `eftpos_au`, `interac`, `jcb`, `link`, `mastercard`, `unionpay`, `visa`, or `unknown`.
            """
            network_transaction_id: Optional[str]
            """
            This is used by the financial networks to identify a transaction. Visa calls this the Transaction ID, Mastercard calls this the Trace ID, and American Express calls this the Acquirer Reference Data. This value will be present if it is returned by the financial network in the authorization response, and null otherwise.
            """
            offline: Optional[Offline]
            """
            Details about payments collected offline.
            """
            overcapture_supported: bool
            """
            Defines whether the authorized amount can be over-captured or not
            """
            preferred_locales: Optional[List[str]]
            """
            The languages that the issuing bank recommends using for localizing any customer-facing text, as read from the card. Referenced from EMV tag 5F2D, data encoded on the card's chip.
            """
            read_method: Optional[
                Literal[
                    "contact_emv",
                    "contactless_emv",
                    "contactless_magstripe_mode",
                    "magnetic_stripe_fallback",
                    "magnetic_stripe_track2",
                ]
            ]
            """
            How card details were read in this transaction.
            """
            reader: Optional[str]
            """
            ID of the [reader](https://docs.stripe.com/api/terminal/readers) this transaction was made on.
            """
            receipt: Optional[Receipt]
            """
            A collection of fields required to be displayed on receipts. Only required for EMV transactions.
            """
            wallet: Optional[Wallet]
            _inner_class_types = {
                "offline": Offline,
                "receipt": Receipt,
                "wallet": Wallet,
            }

        class Cashapp(StripeObject):
            buyer_id: Optional[str]
            """
            A unique and immutable identifier assigned by Cash App to every buyer.
            """
            cashtag: Optional[str]
            """
            A public identifier for buyers using Cash App.
            """
            transaction_id: Optional[str]
            """
            A unique and immutable identifier of payments assigned by Cash App
            """

        class Crypto(StripeObject):
            buyer_address: Optional[str]
            """
            The wallet address of the customer.
            """
            network: Optional[Literal["base", "ethereum", "polygon", "solana"]]
            """
            The blockchain network that the transaction was sent on.
            """
            token_currency: Optional[Literal["usdc", "usdg", "usdp"]]
            """
            The token currency that the transaction was sent with.
            """
            transaction_hash: Optional[str]
            """
            The blockchain transaction hash of the crypto payment.
            """

        class CustomerBalance(StripeObject):
            pass

        class Eps(StripeObject):
            bank: Optional[
                Literal[
                    "arzte_und_apotheker_bank",
                    "austrian_anadi_bank_ag",
                    "bank_austria",
                    "bankhaus_carl_spangler",
                    "bankhaus_schelhammer_und_schattera_ag",
                    "bawag_psk_ag",
                    "bks_bank_ag",
                    "brull_kallmus_bank_ag",
                    "btv_vier_lander_bank",
                    "capital_bank_grawe_gruppe_ag",
                    "deutsche_bank_ag",
                    "dolomitenbank",
                    "easybank_ag",
                    "erste_bank_und_sparkassen",
                    "hypo_alpeadriabank_international_ag",
                    "hypo_bank_burgenland_aktiengesellschaft",
                    "hypo_noe_lb_fur_niederosterreich_u_wien",
                    "hypo_oberosterreich_salzburg_steiermark",
                    "hypo_tirol_bank_ag",
                    "hypo_vorarlberg_bank_ag",
                    "marchfelder_bank",
                    "oberbank_ag",
                    "raiffeisen_bankengruppe_osterreich",
                    "schoellerbank_ag",
                    "sparda_bank_wien",
                    "volksbank_gruppe",
                    "volkskreditbank_ag",
                    "vr_bank_braunau",
                ]
            ]
            """
            The customer's bank. Should be one of `arzte_und_apotheker_bank`, `austrian_anadi_bank_ag`, `bank_austria`, `bankhaus_carl_spangler`, `bankhaus_schelhammer_und_schattera_ag`, `bawag_psk_ag`, `bks_bank_ag`, `brull_kallmus_bank_ag`, `btv_vier_lander_bank`, `capital_bank_grawe_gruppe_ag`, `deutsche_bank_ag`, `dolomitenbank`, `easybank_ag`, `erste_bank_und_sparkassen`, `hypo_alpeadriabank_international_ag`, `hypo_noe_lb_fur_niederosterreich_u_wien`, `hypo_oberosterreich_salzburg_steiermark`, `hypo_tirol_bank_ag`, `hypo_vorarlberg_bank_ag`, `hypo_bank_burgenland_aktiengesellschaft`, `marchfelder_bank`, `oberbank_ag`, `raiffeisen_bankengruppe_osterreich`, `schoellerbank_ag`, `sparda_bank_wien`, `volksbank_gruppe`, `volkskreditbank_ag`, or `vr_bank_braunau`.
            """
            verified_name: Optional[str]
            """
            Owner's verified full name. Values are verified or provided by EPS directly
            (if supported) at the time of authorization or settlement. They cannot be set or mutated.
            EPS rarely provides this information so the attribute is usually empty.
            """

        class Fpx(StripeObject):
            account_holder_type: Optional[Literal["company", "individual"]]
            """
            Account holder type, if provided. Can be one of `individual` or `company`.
            """
            bank: Literal[
                "affin_bank",
                "agrobank",
                "alliance_bank",
                "ambank",
                "bank_islam",
                "bank_muamalat",
                "bank_of_china",
                "bank_rakyat",
                "bsn",
                "cimb",
                "deutsche_bank",
                "hong_leong_bank",
                "hsbc",
                "kfh",
                "maybank2e",
                "maybank2u",
                "ocbc",
                "pb_enterprise",
                "public_bank",
                "rhb",
                "standard_chartered",
                "uob",
            ]
            """
            The customer's bank. Can be one of `affin_bank`, `agrobank`, `alliance_bank`, `ambank`, `bank_islam`, `bank_muamalat`, `bank_rakyat`, `bsn`, `cimb`, `hong_leong_bank`, `hsbc`, `kfh`, `maybank2u`, `ocbc`, `public_bank`, `rhb`, `standard_chartered`, `uob`, `deutsche_bank`, `maybank2e`, `pb_enterprise`, or `bank_of_china`.
            """
            transaction_id: Optional[str]
            """
            Unique transaction id generated by FPX for every request from the merchant
            """

        class Giropay(StripeObject):
            bank_code: Optional[str]
            """
            Bank code of bank associated with the bank account.
            """
            bank_name: Optional[str]
            """
            Name of the bank associated with the bank account.
            """
            bic: Optional[str]
            """
            Bank Identifier Code of the bank associated with the bank account.
            """
            verified_name: Optional[str]
            """
            Owner's verified full name. Values are verified or provided by Giropay directly
            (if supported) at the time of authorization or settlement. They cannot be set or mutated.
            Giropay rarely provides this information so the attribute is usually empty.
            """

        class Grabpay(StripeObject):
            transaction_id: Optional[str]
            """
            Unique transaction id generated by GrabPay
            """

        class Ideal(StripeObject):
            bank: Optional[
                Literal[
                    "abn_amro",
                    "adyen",
                    "asn_bank",
                    "bunq",
                    "buut",
                    "finom",
                    "handelsbanken",
                    "ing",
                    "knab",
                    "mollie",
                    "moneyou",
                    "n26",
                    "nn",
                    "rabobank",
                    "regiobank",
                    "revolut",
                    "sns_bank",
                    "triodos_bank",
                    "van_lanschot",
                    "yoursafe",
                ]
            ]
            """
            The customer's bank. Can be one of `abn_amro`, `adyen`, `asn_bank`, `bunq`, `buut`, `finom`, `handelsbanken`, `ing`, `knab`, `mollie`, `moneyou`, `n26`, `nn`, `rabobank`, `regiobank`, `revolut`, `sns_bank`, `triodos_bank`, `van_lanschot`, or `yoursafe`.
            """
            bic: Optional[
                Literal[
                    "ABNANL2A",
                    "ADYBNL2A",
                    "ASNBNL21",
                    "BITSNL2A",
                    "BUNQNL2A",
                    "BUUTNL2A",
                    "FNOMNL22",
                    "FVLBNL22",
                    "HANDNL2A",
                    "INGBNL2A",
                    "KNABNL2H",
                    "MLLENL2A",
                    "MOYONL21",
                    "NNBANL2G",
                    "NTSBDEB1",
                    "RABONL2U",
                    "RBRBNL21",
                    "REVOIE23",
                    "REVOLT21",
                    "SNSBNL2A",
                    "TRIONL2U",
                ]
            ]
            """
            The Bank Identifier Code of the customer's bank.
            """
            generated_sepa_debit: Optional[ExpandableField["PaymentMethod"]]
            """
            The ID of the SEPA Direct Debit PaymentMethod which was generated by this Charge.
            """
            generated_sepa_debit_mandate: Optional[ExpandableField["Mandate"]]
            """
            The mandate for the SEPA Direct Debit PaymentMethod which was generated by this Charge.
            """
            iban_last4: Optional[str]
            """
            Last four characters of the IBAN.
            """
            transaction_id: Optional[str]
            """
            Unique transaction ID generated by iDEAL.
            """
            verified_name: Optional[str]
            """
            Owner's verified full name. Values are verified or provided by iDEAL directly
            (if supported) at the time of authorization or settlement. They cannot be set or mutated.
            """

        class InteracPresent(StripeObject):
            class Receipt(StripeObject):
                account_type: Optional[
                    Literal["checking", "savings", "unknown"]
                ]
                """
                The type of account being debited or credited
                """
                application_cryptogram: Optional[str]
                """
                The Application Cryptogram, a unique value generated by the card to authenticate the transaction with issuers.
                """
                application_preferred_name: Optional[str]
                """
                The Application Identifier (AID) on the card used to determine which networks are eligible to process the transaction. Referenced from EMV tag 9F12, data encoded on the card's chip.
                """
                authorization_code: Optional[str]
                """
                Identifier for this transaction.
                """
                authorization_response_code: Optional[str]
                """
                EMV tag 8A. A code returned by the card issuer.
                """
                cardholder_verification_method: Optional[str]
                """
                Describes the method used by the cardholder to verify ownership of the card. One of the following: `approval`, `failure`, `none`, `offline_pin`, `offline_pin_and_signature`, `online_pin`, or `signature`.
                """
                dedicated_file_name: Optional[str]
                """
                Similar to the application_preferred_name, identifying the applications (AIDs) available on the card. Referenced from EMV tag 84.
                """
                terminal_verification_results: Optional[str]
                """
                A 5-byte string that records the checks and validations that occur between the card and the terminal. These checks determine how the terminal processes the transaction and what risk tolerance is acceptable. Referenced from EMV Tag 95.
                """
                transaction_status_information: Optional[str]
                """
                An indication of which steps were completed during the card read process. Referenced from EMV Tag 9B.
                """

            brand: Optional[str]
            """
            Card brand. Can be `interac`, `mastercard` or `visa`.
            """
            cardholder_name: Optional[str]
            """
            The cardholder name as read from the card, in [ISO 7813](https://en.wikipedia.org/wiki/ISO/IEC_7813) format. May include alphanumeric characters, special characters and first/last name separator (`/`). In some cases, the cardholder name may not be available depending on how the issuer has configured the card. Cardholder name is typically not available on swipe or contactless payments, such as those made with Apple Pay and Google Pay.
            """
            country: Optional[str]
            """
            Two-letter ISO code representing the country of the card. You could use this attribute to get a sense of the international breakdown of cards you've collected.
            """
            description: Optional[str]
            """
            A high-level description of the type of cards issued in this range. (For internal use only and not typically available in standard API requests.)
            """
            emv_auth_data: Optional[str]
            """
            Authorization response cryptogram.
            """
            exp_month: int
            """
            Two-digit number representing the card's expiration month.
            """
            exp_year: int
            """
            Four-digit number representing the card's expiration year.
            """
            fingerprint: Optional[str]
            """
            Uniquely identifies this particular card number. You can use this attribute to check whether two customers who've signed up with you are using the same card number, for example. For payment methods that tokenize card information (Apple Pay, Google Pay), the tokenized number might be provided instead of the underlying card number.

            *As of May 1, 2021, card fingerprint in India for Connect changed to allow two fingerprints for the same card---one for India and one for the rest of the world.*
            """
            funding: Optional[str]
            """
            Card funding type. Can be `credit`, `debit`, `prepaid`, or `unknown`.
            """
            generated_card: Optional[str]
            """
            ID of a card PaymentMethod generated from the card_present PaymentMethod that may be attached to a Customer for future transactions. Only present if it was possible to generate a card PaymentMethod.
            """
            iin: Optional[str]
            """
            Issuer identification number of the card. (For internal use only and not typically available in standard API requests.)
            """
            issuer: Optional[str]
            """
            The name of the card's issuing bank. (For internal use only and not typically available in standard API requests.)
            """
            last4: Optional[str]
            """
            The last four digits of the card.
            """
            location: Optional[str]
            """
            ID of the [location](https://docs.stripe.com/api/terminal/locations) that this transaction's reader is assigned to.
            """
            network: Optional[str]
            """
            Identifies which network this charge was processed on. Can be `amex`, `cartes_bancaires`, `diners`, `discover`, `eftpos_au`, `interac`, `jcb`, `link`, `mastercard`, `unionpay`, `visa`, or `unknown`.
            """
            network_transaction_id: Optional[str]
            """
            This is used by the financial networks to identify a transaction. Visa calls this the Transaction ID, Mastercard calls this the Trace ID, and American Express calls this the Acquirer Reference Data. This value will be present if it is returned by the financial network in the authorization response, and null otherwise.
            """
            preferred_locales: Optional[List[str]]
            """
            The languages that the issuing bank recommends using for localizing any customer-facing text, as read from the card. Referenced from EMV tag 5F2D, data encoded on the card's chip.
            """
            read_method: Optional[
                Literal[
                    "contact_emv",
                    "contactless_emv",
                    "contactless_magstripe_mode",
                    "magnetic_stripe_fallback",
                    "magnetic_stripe_track2",
                ]
            ]
            """
            How card details were read in this transaction.
            """
            reader: Optional[str]
            """
            ID of the [reader](https://docs.stripe.com/api/terminal/readers) this transaction was made on.
            """
            receipt: Optional[Receipt]
            """
            A collection of fields required to be displayed on receipts. Only required for EMV transactions.
            """
            _inner_class_types = {"receipt": Receipt}

        class KakaoPay(StripeObject):
            buyer_id: Optional[str]
            """
            A unique identifier for the buyer as determined by the local payment processor.
            """
            transaction_id: Optional[str]
            """
            The Kakao Pay transaction ID associated with this payment.
            """

        class Klarna(StripeObject):
            class PayerDetails(StripeObject):
                class Address(StripeObject):
                    country: Optional[str]
                    """
                    The payer address country
                    """

                address: Optional[Address]
                """
                The payer's address
                """
                _inner_class_types = {"address": Address}

            payer_details: Optional[PayerDetails]
            """
            The payer details for this transaction.
            """
            payment_method_category: Optional[str]
            """
            The Klarna payment method used for this transaction.
            Can be one of `pay_later`, `pay_now`, `pay_with_financing`, or `pay_in_installments`
            """
            preferred_locale: Optional[str]
            """
            Preferred language of the Klarna authorization page that the customer is redirected to.
            Can be one of `de-AT`, `en-AT`, `nl-BE`, `fr-BE`, `en-BE`, `de-DE`, `en-DE`, `da-DK`, `en-DK`, `es-ES`, `en-ES`, `fi-FI`, `sv-FI`, `en-FI`, `en-GB`, `en-IE`, `it-IT`, `en-IT`, `nl-NL`, `en-NL`, `nb-NO`, `en-NO`, `sv-SE`, `en-SE`, `en-US`, `es-US`, `fr-FR`, `en-FR`, `cs-CZ`, `en-CZ`, `ro-RO`, `en-RO`, `el-GR`, `en-GR`, `en-AU`, `en-NZ`, `en-CA`, `fr-CA`, `pl-PL`, `en-PL`, `pt-PT`, `en-PT`, `de-CH`, `fr-CH`, `it-CH`, or `en-CH`
            """
            _inner_class_types = {"payer_details": PayerDetails}

        class Konbini(StripeObject):
            class Store(StripeObject):
                chain: Optional[
                    Literal["familymart", "lawson", "ministop", "seicomart"]
                ]
                """
                The name of the convenience store chain where the payment was completed.
                """

            store: Optional[Store]
            """
            If the payment succeeded, this contains the details of the convenience store where the payment was completed.
            """
            _inner_class_types = {"store": Store}

        class KrCard(StripeObject):
            brand: Optional[
                Literal[
                    "bc",
                    "citi",
                    "hana",
                    "hyundai",
                    "jeju",
                    "jeonbuk",
                    "kakaobank",
                    "kbank",
                    "kdbbank",
                    "kookmin",
                    "kwangju",
                    "lotte",
                    "mg",
                    "nh",
                    "post",
                    "samsung",
                    "savingsbank",
                    "shinhan",
                    "shinhyup",
                    "suhyup",
                    "tossbank",
                    "woori",
                ]
            ]
            """
            The local credit or debit card brand.
            """
            buyer_id: Optional[str]
            """
            A unique identifier for the buyer as determined by the local payment processor.
            """
            last4: Optional[str]
            """
            The last four digits of the card. This may not be present for American Express cards.
            """
            transaction_id: Optional[str]
            """
            The Korean Card transaction ID associated with this payment.
            """

        class Link(StripeObject):
            country: Optional[str]
            """
            Two-letter ISO code representing the funding source country beneath the Link payment.
            You could use this attribute to get a sense of international fees.
            """

        class MbWay(StripeObject):
            pass

        class Mobilepay(StripeObject):
            class Card(StripeObject):
                brand: Optional[str]
                """
                Brand of the card used in the transaction
                """
                country: Optional[str]
                """
                Two-letter ISO code representing the country of the card
                """
                exp_month: Optional[int]
                """
                Two digit number representing the card's expiration month
                """
                exp_year: Optional[int]
                """
                Two digit number representing the card's expiration year
                """
                last4: Optional[str]
                """
                The last 4 digits of the card
                """

            card: Optional[Card]
            """
            Internal card details
            """
            _inner_class_types = {"card": Card}

        class Multibanco(StripeObject):
            entity: Optional[str]
            """
            Entity number associated with this Multibanco payment.
            """
            reference: Optional[str]
            """
            Reference number associated with this Multibanco payment.
            """

        class NaverPay(StripeObject):
            buyer_id: Optional[str]
            """
            A unique identifier for the buyer as determined by the local payment processor.
            """
            transaction_id: Optional[str]
            """
            The Naver Pay transaction ID associated with this payment.
            """

        class NzBankAccount(StripeObject):
            account_holder_name: Optional[str]
            """
            The name on the bank account. Only present if the account holder name is different from the name of the authorized signatory collected in the PaymentMethod's billing details.
            """
            bank_code: str
            """
            The numeric code for the bank account's bank.
            """
            bank_name: str
            """
            The name of the bank.
            """
            branch_code: str
            """
            The numeric code for the bank account's bank branch.
            """
            expected_debit_date: Optional[str]
            """
            Estimated date to debit the customer's bank account. A date string in YYYY-MM-DD format.
            """
            last4: str
            """
            Last four digits of the bank account number.
            """
            suffix: Optional[str]
            """
            The suffix of the bank account number.
            """

        class Oxxo(StripeObject):
            number: Optional[str]
            """
            OXXO reference number
            """

        class P24(StripeObject):
            bank: Optional[
                Literal[
                    "alior_bank",
                    "bank_millennium",
                    "bank_nowy_bfg_sa",
                    "bank_pekao_sa",
                    "banki_spbdzielcze",
                    "blik",
                    "bnp_paribas",
                    "boz",
                    "citi_handlowy",
                    "credit_agricole",
                    "envelobank",
                    "etransfer_pocztowy24",
                    "getin_bank",
                    "ideabank",
                    "ing",
                    "inteligo",
                    "mbank_mtransfer",
                    "nest_przelew",
                    "noble_pay",
                    "pbac_z_ipko",
                    "plus_bank",
                    "santander_przelew24",
                    "tmobile_usbugi_bankowe",
                    "toyota_bank",
                    "velobank",
                    "volkswagen_bank",
                ]
            ]
            """
            The customer's bank. Can be one of `ing`, `citi_handlowy`, `tmobile_usbugi_bankowe`, `plus_bank`, `etransfer_pocztowy24`, `banki_spbdzielcze`, `bank_nowy_bfg_sa`, `getin_bank`, `velobank`, `blik`, `noble_pay`, `ideabank`, `envelobank`, `santander_przelew24`, `nest_przelew`, `mbank_mtransfer`, `inteligo`, `pbac_z_ipko`, `bnp_paribas`, `credit_agricole`, `toyota_bank`, `bank_pekao_sa`, `volkswagen_bank`, `bank_millennium`, `alior_bank`, or `boz`.
            """
            reference: Optional[str]
            """
            Unique reference for this Przelewy24 payment.
            """
            verified_name: Optional[str]
            """
            Owner's verified full name. Values are verified or provided by Przelewy24 directly
            (if supported) at the time of authorization or settlement. They cannot be set or mutated.
            Przelewy24 rarely provides this information so the attribute is usually empty.
            """

        class PayByBank(StripeObject):
            pass

        class Payco(StripeObject):
            buyer_id: Optional[str]
            """
            A unique identifier for the buyer as determined by the local payment processor.
            """
            transaction_id: Optional[str]
            """
            The Payco transaction ID associated with this payment.
            """

        class Paynow(StripeObject):
            location: Optional[str]
            """
            ID of the [location](https://docs.stripe.com/api/terminal/locations) that this transaction's reader is assigned to.
            """
            reader: Optional[str]
            """
            ID of the [reader](https://docs.stripe.com/api/terminal/readers) this transaction was made on.
            """
            reference: Optional[str]
            """
            Reference number associated with this PayNow payment
            """

        class Paypal(StripeObject):
            class SellerProtection(StripeObject):
                dispute_categories: Optional[
                    List[Literal["fraudulent", "product_not_received"]]
                ]
                """
                An array of conditions that are covered for the transaction, if applicable.
                """
                status: Literal[
                    "eligible", "not_eligible", "partially_eligible"
                ]
                """
                Indicates whether the transaction is eligible for PayPal's seller protection.
                """

            country: Optional[str]
            """
            Two-letter ISO code representing the buyer's country. Values are provided by PayPal directly (if supported) at the time of authorization or settlement. They cannot be set or mutated.
            """
            payer_email: Optional[str]
            """
            Owner's email. Values are provided by PayPal directly
            (if supported) at the time of authorization or settlement. They cannot be set or mutated.
            """
            payer_id: Optional[str]
            """
            PayPal account PayerID. This identifier uniquely identifies the PayPal customer.
            """
            payer_name: Optional[str]
            """
            Owner's full name. Values provided by PayPal directly
            (if supported) at the time of authorization or settlement. They cannot be set or mutated.
            """
            seller_protection: Optional[SellerProtection]
            """
            The level of protection offered as defined by PayPal Seller Protection for Merchants, for this transaction.
            """
            transaction_id: Optional[str]
            """
            A unique ID generated by PayPal for this transaction.
            """
            _inner_class_types = {"seller_protection": SellerProtection}

        class Payto(StripeObject):
            bsb_number: Optional[str]
            """
            Bank-State-Branch number of the bank account.
            """
            last4: Optional[str]
            """
            Last four digits of the bank account number.
            """
            mandate: Optional[str]
            """
            ID of the mandate used to make this payment.
            """
            pay_id: Optional[str]
            """
            The PayID alias for the bank account.
            """

        class Pix(StripeObject):
            bank_transaction_id: Optional[str]
            """
            Unique transaction id generated by BCB
            """

        class Promptpay(StripeObject):
            reference: Optional[str]
            """
            Bill reference generated by PromptPay
            """

        class RevolutPay(StripeObject):
            class Funding(StripeObject):
                class Card(StripeObject):
                    brand: Optional[str]
                    """
                    Card brand. Can be `amex`, `cartes_bancaires`, `diners`, `discover`, `eftpos_au`, `jcb`, `link`, `mastercard`, `unionpay`, `visa` or `unknown`.
                    """
                    country: Optional[str]
                    """
                    Two-letter ISO code representing the country of the card. You could use this attribute to get a sense of the international breakdown of cards you've collected.
                    """
                    exp_month: Optional[int]
                    """
                    Two-digit number representing the card's expiration month.
                    """
                    exp_year: Optional[int]
                    """
                    Four-digit number representing the card's expiration year.
                    """
                    funding: Optional[str]
                    """
                    Card funding type. Can be `credit`, `debit`, `prepaid`, or `unknown`.
                    """
                    last4: Optional[str]
                    """
                    The last four digits of the card.
                    """

                card: Optional[Card]
                type: Optional[Literal["card"]]
                """
                funding type of the underlying payment method.
                """
                _inner_class_types = {"card": Card}

            funding: Optional[Funding]
            transaction_id: Optional[str]
            """
            The Revolut Pay transaction ID associated with this payment.
            """
            _inner_class_types = {"funding": Funding}

        class SamsungPay(StripeObject):
            buyer_id: Optional[str]
            """
            A unique identifier for the buyer as determined by the local payment processor.
            """
            transaction_id: Optional[str]
            """
            The Samsung Pay transaction ID associated with this payment.
            """

        class Satispay(StripeObject):
            transaction_id: Optional[str]
            """
            The Satispay transaction ID associated with this payment.
            """

        class SepaCreditTransfer(StripeObject):
            bank_name: Optional[str]
            """
            Name of the bank associated with the bank account.
            """
            bic: Optional[str]
            """
            Bank Identifier Code of the bank associated with the bank account.
            """
            iban: Optional[str]
            """
            IBAN of the bank account to transfer funds to.
            """

        class SepaDebit(StripeObject):
            bank_code: Optional[str]
            """
            Bank code of bank associated with the bank account.
            """
            branch_code: Optional[str]
            """
            Branch code of bank associated with the bank account.
            """
            country: Optional[str]
            """
            Two-letter ISO code representing the country the bank account is located in.
            """
            expected_debit_date: Optional[str]
            """
            Estimated date to debit the customer's bank account. A date string in YYYY-MM-DD format.
            """
            fingerprint: Optional[str]
            """
            Uniquely identifies this particular bank account. You can use this attribute to check whether two bank accounts are the same.
            """
            last4: Optional[str]
            """
            Last four characters of the IBAN.
            """
            mandate: Optional[str]
            """
            Find the ID of the mandate used for this payment under the [payment_method_details.sepa_debit.mandate](https://docs.stripe.com/api/charges/object#charge_object-payment_method_details-sepa_debit-mandate) property on the Charge. Use this mandate ID to [retrieve the Mandate](https://docs.stripe.com/api/mandates/retrieve).
            """

        class Sofort(StripeObject):
            bank_code: Optional[str]
            """
            Bank code of bank associated with the bank account.
            """
            bank_name: Optional[str]
            """
            Name of the bank associated with the bank account.
            """
            bic: Optional[str]
            """
            Bank Identifier Code of the bank associated with the bank account.
            """
            country: Optional[str]
            """
            Two-letter ISO code representing the country the bank account is located in.
            """
            generated_sepa_debit: Optional[ExpandableField["PaymentMethod"]]
            """
            The ID of the SEPA Direct Debit PaymentMethod which was generated by this Charge.
            """
            generated_sepa_debit_mandate: Optional[ExpandableField["Mandate"]]
            """
            The mandate for the SEPA Direct Debit PaymentMethod which was generated by this Charge.
            """
            iban_last4: Optional[str]
            """
            Last four characters of the IBAN.
            """
            preferred_language: Optional[
                Literal["de", "en", "es", "fr", "it", "nl", "pl"]
            ]
            """
            Preferred language of the SOFORT authorization page that the customer is redirected to.
            Can be one of `de`, `en`, `es`, `fr`, `it`, `nl`, or `pl`
            """
            verified_name: Optional[str]
            """
            Owner's verified full name. Values are verified or provided by SOFORT directly
            (if supported) at the time of authorization or settlement. They cannot be set or mutated.
            """

        class StripeAccount(StripeObject):
            pass

        class Swish(StripeObject):
            fingerprint: Optional[str]
            """
            Uniquely identifies the payer's Swish account. You can use this attribute to check whether two Swish transactions were paid for by the same payer
            """
            payment_reference: Optional[str]
            """
            Payer bank reference number for the payment
            """
            verified_phone_last4: Optional[str]
            """
            The last four digits of the Swish account phone number
            """

        class Twint(StripeObject):
            pass

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
            expected_debit_date: Optional[str]
            """
            Estimated date to debit the customer's bank account. A date string in YYYY-MM-DD format.
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
            payment_reference: Optional[str]
            """
            Reference number to locate ACH payments with customer's bank.
            """
            routing_number: Optional[str]
            """
            Routing number of the bank account.
            """

        class Wechat(StripeObject):
            pass

        class WechatPay(StripeObject):
            fingerprint: Optional[str]
            """
            Uniquely identifies this particular WeChat Pay account. You can use this attribute to check whether two WeChat accounts are the same.
            """
            location: Optional[str]
            """
            ID of the [location](https://docs.stripe.com/api/terminal/locations) that this transaction's reader is assigned to.
            """
            reader: Optional[str]
            """
            ID of the [reader](https://docs.stripe.com/api/terminal/readers) this transaction was made on.
            """
            transaction_id: Optional[str]
            """
            Transaction ID of this particular WeChat Pay transaction.
            """

        class Zip(StripeObject):
            pass

        ach_credit_transfer: Optional[AchCreditTransfer]
        ach_debit: Optional[AchDebit]
        acss_debit: Optional[AcssDebit]
        affirm: Optional[Affirm]
        afterpay_clearpay: Optional[AfterpayClearpay]
        alipay: Optional[Alipay]
        alma: Optional[Alma]
        amazon_pay: Optional[AmazonPay]
        au_becs_debit: Optional[AuBecsDebit]
        bacs_debit: Optional[BacsDebit]
        bancontact: Optional[Bancontact]
        billie: Optional[Billie]
        blik: Optional[Blik]
        boleto: Optional[Boleto]
        card: Optional[Card]
        card_present: Optional[CardPresent]
        cashapp: Optional[Cashapp]
        crypto: Optional[Crypto]
        customer_balance: Optional[CustomerBalance]
        eps: Optional[Eps]
        fpx: Optional[Fpx]
        giropay: Optional[Giropay]
        grabpay: Optional[Grabpay]
        ideal: Optional[Ideal]
        interac_present: Optional[InteracPresent]
        kakao_pay: Optional[KakaoPay]
        klarna: Optional[Klarna]
        konbini: Optional[Konbini]
        kr_card: Optional[KrCard]
        link: Optional[Link]
        mb_way: Optional[MbWay]
        mobilepay: Optional[Mobilepay]
        multibanco: Optional[Multibanco]
        naver_pay: Optional[NaverPay]
        nz_bank_account: Optional[NzBankAccount]
        oxxo: Optional[Oxxo]
        p24: Optional[P24]
        pay_by_bank: Optional[PayByBank]
        payco: Optional[Payco]
        paynow: Optional[Paynow]
        paypal: Optional[Paypal]
        payto: Optional[Payto]
        pix: Optional[Pix]
        promptpay: Optional[Promptpay]
        revolut_pay: Optional[RevolutPay]
        samsung_pay: Optional[SamsungPay]
        satispay: Optional[Satispay]
        sepa_credit_transfer: Optional[SepaCreditTransfer]
        sepa_debit: Optional[SepaDebit]
        sofort: Optional[Sofort]
        stripe_account: Optional[StripeAccount]
        swish: Optional[Swish]
        twint: Optional[Twint]
        type: str
        """
        The type of transaction-specific details of the payment method used in the payment. See [PaymentMethod.type](https://docs.stripe.com/api/payment_methods/object#payment_method_object-type) for the full list of possible types.
        An additional hash is included on `payment_method_details` with a name matching this value.
        It contains information specific to the payment method.
        """
        us_bank_account: Optional[UsBankAccount]
        wechat: Optional[Wechat]
        wechat_pay: Optional[WechatPay]
        zip: Optional[Zip]
        _inner_class_types = {
            "ach_credit_transfer": AchCreditTransfer,
            "ach_debit": AchDebit,
            "acss_debit": AcssDebit,
            "affirm": Affirm,
            "afterpay_clearpay": AfterpayClearpay,
            "alipay": Alipay,
            "alma": Alma,
            "amazon_pay": AmazonPay,
            "au_becs_debit": AuBecsDebit,
            "bacs_debit": BacsDebit,
            "bancontact": Bancontact,
            "billie": Billie,
            "blik": Blik,
            "boleto": Boleto,
            "card": Card,
            "card_present": CardPresent,
            "cashapp": Cashapp,
            "crypto": Crypto,
            "customer_balance": CustomerBalance,
            "eps": Eps,
            "fpx": Fpx,
            "giropay": Giropay,
            "grabpay": Grabpay,
            "ideal": Ideal,
            "interac_present": InteracPresent,
            "kakao_pay": KakaoPay,
            "klarna": Klarna,
            "konbini": Konbini,
            "kr_card": KrCard,
            "link": Link,
            "mb_way": MbWay,
            "mobilepay": Mobilepay,
            "multibanco": Multibanco,
            "naver_pay": NaverPay,
            "nz_bank_account": NzBankAccount,
            "oxxo": Oxxo,
            "p24": P24,
            "pay_by_bank": PayByBank,
            "payco": Payco,
            "paynow": Paynow,
            "paypal": Paypal,
            "payto": Payto,
            "pix": Pix,
            "promptpay": Promptpay,
            "revolut_pay": RevolutPay,
            "samsung_pay": SamsungPay,
            "satispay": Satispay,
            "sepa_credit_transfer": SepaCreditTransfer,
            "sepa_debit": SepaDebit,
            "sofort": Sofort,
            "stripe_account": StripeAccount,
            "swish": Swish,
            "twint": Twint,
            "us_bank_account": UsBankAccount,
            "wechat": Wechat,
            "wechat_pay": WechatPay,
            "zip": Zip,
        }

    class PresentmentDetails(StripeObject):
        presentment_amount: int
        """
        Amount intended to be collected by this payment, denominated in `presentment_currency`.
        """
        presentment_currency: str
        """
        Currency presented to the customer during payment.
        """

    class RadarOptions(StripeObject):
        session: Optional[str]
        """
        A [Radar Session](https://docs.stripe.com/radar/radar-session) is a snapshot of the browser metadata and device details that help Radar make more accurate predictions on your payments.
        """

    class Shipping(StripeObject):
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

        address: Optional[Address]
        carrier: Optional[str]
        """
        The delivery service that shipped a physical product, such as Fedex, UPS, USPS, etc.
        """
        name: Optional[str]
        """
        Recipient name.
        """
        phone: Optional[str]
        """
        Recipient phone (including extension).
        """
        tracking_number: Optional[str]
        """
        The tracking number for a physical product, obtained from the delivery service. If multiple tracking numbers were generated for this purchase, please separate them with commas.
        """
        _inner_class_types = {"address": Address}

    class TransferData(StripeObject):
        amount: Optional[int]
        """
        The amount transferred to the destination account, if specified. By default, the entire charge amount is transferred to the destination account.
        """
        destination: ExpandableField["Account"]
        """
        ID of an existing, connected Stripe account to transfer funds to if `transfer_data` was specified in the charge request.
        """

    amount: int
    """
    Amount intended to be collected by this payment. A positive integer representing how much to charge in the [smallest currency unit](https://docs.stripe.com/currencies#zero-decimal) (e.g., 100 cents to charge $1.00 or 100 to charge ¥100, a zero-decimal currency). The minimum amount is $0.50 US or [equivalent in charge currency](https://docs.stripe.com/currencies#minimum-and-maximum-charge-amounts). The amount value supports up to eight digits (e.g., a value of 99999999 for a USD charge of $999,999.99).
    """
    amount_captured: int
    """
    Amount in cents (or local equivalent) captured (can be less than the amount attribute on the charge if a partial capture was made).
    """
    amount_refunded: int
    """
    Amount in cents (or local equivalent) refunded (can be less than the amount attribute on the charge if a partial refund was issued).
    """
    application: Optional[ExpandableField["Application"]]
    """
    ID of the Connect application that created the charge.
    """
    application_fee: Optional[ExpandableField["ApplicationFee"]]
    """
    The application fee (if any) for the charge. [See the Connect documentation](https://docs.stripe.com/connect/direct-charges#collect-fees) for details.
    """
    application_fee_amount: Optional[int]
    """
    The amount of the application fee (if any) requested for the charge. [See the Connect documentation](https://docs.stripe.com/connect/direct-charges#collect-fees) for details.
    """
    authorization_code: Optional[str]
    """
    Authorization code on the charge.
    """
    balance_transaction: Optional[ExpandableField["BalanceTransaction"]]
    """
    ID of the balance transaction that describes the impact of this charge on your account balance (not including refunds or disputes).
    """
    billing_details: BillingDetails
    calculated_statement_descriptor: Optional[str]
    """
    The full statement descriptor that is passed to card networks, and that is displayed on your customers' credit card and bank statements. Allows you to see what the statement descriptor looks like after the static and dynamic portions are combined. This value only exists for card payments.
    """
    captured: bool
    """
    If the charge was created without capturing, this Boolean represents whether it is still uncaptured or has since been captured.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    currency: str
    """
    Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
    """
    customer: Optional[ExpandableField["Customer"]]
    """
    ID of the customer this charge is for if one exists.
    """
    description: Optional[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    disputed: bool
    """
    Whether the charge has been disputed.
    """
    failure_balance_transaction: Optional[
        ExpandableField["BalanceTransaction"]
    ]
    """
    ID of the balance transaction that describes the reversal of the balance on your account due to payment failure.
    """
    failure_code: Optional[str]
    """
    Error code explaining reason for charge failure if available (see [the errors section](https://docs.stripe.com/error-codes) for a list of codes).
    """
    failure_message: Optional[str]
    """
    Message to user further explaining reason for charge failure if available.
    """
    fraud_details: Optional[FraudDetails]
    """
    Information on fraud assessments for the charge.
    """
    id: str
    """
    Unique identifier for the object.
    """
    level3: Optional[Level3]
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["charge"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    on_behalf_of: Optional[ExpandableField["Account"]]
    """
    The account (if any) the charge was made on behalf of without triggering an automatic transfer. See the [Connect documentation](https://docs.stripe.com/connect/separate-charges-and-transfers) for details.
    """
    outcome: Optional[Outcome]
    """
    Details about whether the payment was accepted, and why. See [understanding declines](https://docs.stripe.com/declines) for details.
    """
    paid: bool
    """
    `true` if the charge succeeded, or was successfully authorized for later capture.
    """
    payment_intent: Optional[ExpandableField["PaymentIntent"]]
    """
    ID of the PaymentIntent associated with this charge, if one exists.
    """
    payment_method: Optional[str]
    """
    ID of the payment method used in this charge.
    """
    payment_method_details: Optional[PaymentMethodDetails]
    """
    Details about the payment method at the time of the transaction.
    """
    presentment_details: Optional[PresentmentDetails]
    radar_options: Optional[RadarOptions]
    """
    Options to configure Radar. See [Radar Session](https://docs.stripe.com/radar/radar-session) for more information.
    """
    receipt_email: Optional[str]
    """
    This is the email address that the receipt for this charge was sent to.
    """
    receipt_number: Optional[str]
    """
    This is the transaction number that appears on email receipts sent for this charge. This attribute will be `null` until a receipt has been sent.
    """
    receipt_url: Optional[str]
    """
    This is the URL to view the receipt for this charge. The receipt is kept up-to-date to the latest state of the charge, including any refunds. If the charge is for an Invoice, the receipt will be stylized as an Invoice receipt.
    """
    refunded: bool
    """
    Whether the charge has been fully refunded. If the charge is only partially refunded, this attribute will still be false.
    """
    refunds: Optional[ListObject["Refund"]]
    """
    A list of refunds that have been applied to the charge.
    """
    review: Optional[ExpandableField["Review"]]
    """
    ID of the review associated with this charge if one exists.
    """
    shipping: Optional[Shipping]
    """
    Shipping information for the charge.
    """
    source: Optional[Union["Account", "BankAccount", "CardResource", "Source"]]
    """
    This is a legacy field that will be removed in the future. It contains the Source, Card, or BankAccount object used for the charge. For details about the payment method used for this charge, refer to `payment_method` or `payment_method_details` instead.
    """
    source_transfer: Optional[ExpandableField["Transfer"]]
    """
    The transfer ID which created this charge. Only present if the charge came from another Stripe account. [See the Connect documentation](https://docs.stripe.com/connect/destination-charges) for details.
    """
    statement_descriptor: Optional[str]
    """
    For a non-card charge, text that appears on the customer's statement as the statement descriptor. This value overrides the account's default statement descriptor. For information about requirements, including the 22-character limit, see [the Statement Descriptor docs](https://docs.stripe.com/get-started/account/statement-descriptors).

    For a card charge, this value is ignored unless you don't specify a `statement_descriptor_suffix`, in which case this value is used as the suffix.
    """
    statement_descriptor_suffix: Optional[str]
    """
    Provides information about a card charge. Concatenated to the account's [statement descriptor prefix](https://docs.stripe.com/get-started/account/statement-descriptors#static) to form the complete statement descriptor that appears on the customer's statement. If the account has no prefix value, the suffix is concatenated to the account's statement descriptor.
    """
    status: Literal["failed", "pending", "succeeded"]
    """
    The status of the payment is either `succeeded`, `pending`, or `failed`.
    """
    transfer: Optional[ExpandableField["Transfer"]]
    """
    ID of the transfer to the `destination` account (only applicable if the charge was created using the `destination` parameter).
    """
    transfer_data: Optional[TransferData]
    """
    An optional dictionary including the account to automatically transfer to as part of a destination charge. [See the Connect documentation](https://docs.stripe.com/connect/destination-charges) for details.
    """
    transfer_group: Optional[str]
    """
    A string that identifies this transaction as part of a group. See the [Connect documentation](https://docs.stripe.com/connect/separate-charges-and-transfers#transfer-options) for details.
    """

    @classmethod
    def _cls_capture(
        cls, charge: str, **params: Unpack["ChargeCaptureParams"]
    ) -> "Charge":
        """
        Capture the payment of an existing, uncaptured charge that was created with the capture option set to false.

        Uncaptured payments expire a set number of days after they are created ([7 by default](https://docs.stripe.com/docs/charges/placing-a-hold)), after which they are marked as refunded and capture attempts will fail.

        Don't use this method to capture a PaymentIntent-initiated charge. Use [Capture a PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/capture).
        """
        return cast(
            "Charge",
            cls._static_request(
                "post",
                "/v1/charges/{charge}/capture".format(
                    charge=sanitize_id(charge)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def capture(
        charge: str, **params: Unpack["ChargeCaptureParams"]
    ) -> "Charge":
        """
        Capture the payment of an existing, uncaptured charge that was created with the capture option set to false.

        Uncaptured payments expire a set number of days after they are created ([7 by default](https://docs.stripe.com/docs/charges/placing-a-hold)), after which they are marked as refunded and capture attempts will fail.

        Don't use this method to capture a PaymentIntent-initiated charge. Use [Capture a PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/capture).
        """
        ...

    @overload
    def capture(self, **params: Unpack["ChargeCaptureParams"]) -> "Charge":
        """
        Capture the payment of an existing, uncaptured charge that was created with the capture option set to false.

        Uncaptured payments expire a set number of days after they are created ([7 by default](https://docs.stripe.com/docs/charges/placing-a-hold)), after which they are marked as refunded and capture attempts will fail.

        Don't use this method to capture a PaymentIntent-initiated charge. Use [Capture a PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/capture).
        """
        ...

    @class_method_variant("_cls_capture")
    def capture(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ChargeCaptureParams"]
    ) -> "Charge":
        """
        Capture the payment of an existing, uncaptured charge that was created with the capture option set to false.

        Uncaptured payments expire a set number of days after they are created ([7 by default](https://docs.stripe.com/docs/charges/placing-a-hold)), after which they are marked as refunded and capture attempts will fail.

        Don't use this method to capture a PaymentIntent-initiated charge. Use [Capture a PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/capture).
        """
        return cast(
            "Charge",
            self._request(
                "post",
                "/v1/charges/{charge}/capture".format(
                    charge=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_capture_async(
        cls, charge: str, **params: Unpack["ChargeCaptureParams"]
    ) -> "Charge":
        """
        Capture the payment of an existing, uncaptured charge that was created with the capture option set to false.

        Uncaptured payments expire a set number of days after they are created ([7 by default](https://docs.stripe.com/docs/charges/placing-a-hold)), after which they are marked as refunded and capture attempts will fail.

        Don't use this method to capture a PaymentIntent-initiated charge. Use [Capture a PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/capture).
        """
        return cast(
            "Charge",
            await cls._static_request_async(
                "post",
                "/v1/charges/{charge}/capture".format(
                    charge=sanitize_id(charge)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def capture_async(
        charge: str, **params: Unpack["ChargeCaptureParams"]
    ) -> "Charge":
        """
        Capture the payment of an existing, uncaptured charge that was created with the capture option set to false.

        Uncaptured payments expire a set number of days after they are created ([7 by default](https://docs.stripe.com/docs/charges/placing-a-hold)), after which they are marked as refunded and capture attempts will fail.

        Don't use this method to capture a PaymentIntent-initiated charge. Use [Capture a PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/capture).
        """
        ...

    @overload
    async def capture_async(
        self, **params: Unpack["ChargeCaptureParams"]
    ) -> "Charge":
        """
        Capture the payment of an existing, uncaptured charge that was created with the capture option set to false.

        Uncaptured payments expire a set number of days after they are created ([7 by default](https://docs.stripe.com/docs/charges/placing-a-hold)), after which they are marked as refunded and capture attempts will fail.

        Don't use this method to capture a PaymentIntent-initiated charge. Use [Capture a PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/capture).
        """
        ...

    @class_method_variant("_cls_capture_async")
    async def capture_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["ChargeCaptureParams"]
    ) -> "Charge":
        """
        Capture the payment of an existing, uncaptured charge that was created with the capture option set to false.

        Uncaptured payments expire a set number of days after they are created ([7 by default](https://docs.stripe.com/docs/charges/placing-a-hold)), after which they are marked as refunded and capture attempts will fail.

        Don't use this method to capture a PaymentIntent-initiated charge. Use [Capture a PaymentIntent](https://docs.stripe.com/docs/api/payment_intents/capture).
        """
        return cast(
            "Charge",
            await self._request_async(
                "post",
                "/v1/charges/{charge}/capture".format(
                    charge=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def create(cls, **params: Unpack["ChargeCreateParams"]) -> "Charge":
        """
        This method is no longer recommended—use the [Payment Intents API](https://docs.stripe.com/docs/api/payment_intents)
        to initiate a new payment instead. Confirmation of the PaymentIntent creates the Charge
        object used to request payment.
        """
        return cast(
            "Charge",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["ChargeCreateParams"]
    ) -> "Charge":
        """
        This method is no longer recommended—use the [Payment Intents API](https://docs.stripe.com/docs/api/payment_intents)
        to initiate a new payment instead. Confirmation of the PaymentIntent creates the Charge
        object used to request payment.
        """
        return cast(
            "Charge",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["ChargeListParams"]
    ) -> ListObject["Charge"]:
        """
        Returns a list of charges you've previously created. The charges are returned in sorted order, with the most recent charges appearing first.
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
        cls, **params: Unpack["ChargeListParams"]
    ) -> ListObject["Charge"]:
        """
        Returns a list of charges you've previously created. The charges are returned in sorted order, with the most recent charges appearing first.
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
        cls, id: str, **params: Unpack["ChargeModifyParams"]
    ) -> "Charge":
        """
        Updates the specified charge by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Charge",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls, id: str, **params: Unpack["ChargeModifyParams"]
    ) -> "Charge":
        """
        Updates the specified charge by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "Charge",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["ChargeRetrieveParams"]
    ) -> "Charge":
        """
        Retrieves the details of a charge that has previously been created. Supply the unique charge ID that was returned from your previous request, and Stripe will return the corresponding charge information. The same information is returned when creating or refunding the charge.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["ChargeRetrieveParams"]
    ) -> "Charge":
        """
        Retrieves the details of a charge that has previously been created. Supply the unique charge ID that was returned from your previous request, and Stripe will return the corresponding charge information. The same information is returned when creating or refunding the charge.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def search(
        cls, *args, **kwargs: Unpack["ChargeSearchParams"]
    ) -> SearchResultObject["Charge"]:
        """
        Search for charges you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cls._search(search_url="/v1/charges/search", *args, **kwargs)

    @classmethod
    async def search_async(
        cls, *args, **kwargs: Unpack["ChargeSearchParams"]
    ) -> SearchResultObject["Charge"]:
        """
        Search for charges you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return await cls._search_async(
            search_url="/v1/charges/search", *args, **kwargs
        )

    @classmethod
    def search_auto_paging_iter(
        cls, *args, **kwargs: Unpack["ChargeSearchParams"]
    ) -> Iterator["Charge"]:
        return cls.search(*args, **kwargs).auto_paging_iter()

    @classmethod
    async def search_auto_paging_iter_async(
        cls, *args, **kwargs: Unpack["ChargeSearchParams"]
    ) -> AsyncIterator["Charge"]:
        return (await cls.search_async(*args, **kwargs)).auto_paging_iter()

    def mark_as_fraudulent(self, idempotency_key=None) -> "Charge":
        params = {
            "fraud_details": {"user_report": "fraudulent"},
            "idempotency_key": idempotency_key,
        }
        url = self.instance_url()
        self._request_and_refresh("post", url, params)
        return self

    def mark_as_safe(self, idempotency_key=None) -> "Charge":
        params = {
            "fraud_details": {"user_report": "safe"},
            "idempotency_key": idempotency_key,
        }
        url = self.instance_url()
        self._request_and_refresh("post", url, params)
        return self

    @classmethod
    def retrieve_refund(
        cls,
        charge: str,
        refund: str,
        **params: Unpack["ChargeRetrieveRefundParams"],
    ) -> "Refund":
        """
        Retrieves the details of an existing refund.
        """
        return cast(
            "Refund",
            cls._static_request(
                "get",
                "/v1/charges/{charge}/refunds/{refund}".format(
                    charge=sanitize_id(charge), refund=sanitize_id(refund)
                ),
                params=params,
            ),
        )

    @classmethod
    async def retrieve_refund_async(
        cls,
        charge: str,
        refund: str,
        **params: Unpack["ChargeRetrieveRefundParams"],
    ) -> "Refund":
        """
        Retrieves the details of an existing refund.
        """
        return cast(
            "Refund",
            await cls._static_request_async(
                "get",
                "/v1/charges/{charge}/refunds/{refund}".format(
                    charge=sanitize_id(charge), refund=sanitize_id(refund)
                ),
                params=params,
            ),
        )

    @classmethod
    def list_refunds(
        cls, charge: str, **params: Unpack["ChargeListRefundsParams"]
    ) -> ListObject["Refund"]:
        """
        You can see a list of the refunds belonging to a specific charge. Note that the 10 most recent refunds are always available by default on the charge object. If you need more than those 10, you can use this API method and the limit and starting_after parameters to page through additional refunds.
        """
        return cast(
            ListObject["Refund"],
            cls._static_request(
                "get",
                "/v1/charges/{charge}/refunds".format(
                    charge=sanitize_id(charge)
                ),
                params=params,
            ),
        )

    @classmethod
    async def list_refunds_async(
        cls, charge: str, **params: Unpack["ChargeListRefundsParams"]
    ) -> ListObject["Refund"]:
        """
        You can see a list of the refunds belonging to a specific charge. Note that the 10 most recent refunds are always available by default on the charge object. If you need more than those 10, you can use this API method and the limit and starting_after parameters to page through additional refunds.
        """
        return cast(
            ListObject["Refund"],
            await cls._static_request_async(
                "get",
                "/v1/charges/{charge}/refunds".format(
                    charge=sanitize_id(charge)
                ),
                params=params,
            ),
        )

    _inner_class_types = {
        "billing_details": BillingDetails,
        "fraud_details": FraudDetails,
        "level3": Level3,
        "outcome": Outcome,
        "payment_method_details": PaymentMethodDetails,
        "presentment_details": PresentmentDetails,
        "radar_options": RadarOptions,
        "shipping": Shipping,
        "transfer_data": TransferData,
    }
