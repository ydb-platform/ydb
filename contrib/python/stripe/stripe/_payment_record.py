# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._api_resource import APIResource
from stripe._expandable_field import ExpandableField
from stripe._stripe_object import StripeObject
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._mandate import Mandate
    from stripe._payment_method import PaymentMethod
    from stripe.params._payment_record_report_payment_attempt_canceled_params import (
        PaymentRecordReportPaymentAttemptCanceledParams,
    )
    from stripe.params._payment_record_report_payment_attempt_failed_params import (
        PaymentRecordReportPaymentAttemptFailedParams,
    )
    from stripe.params._payment_record_report_payment_attempt_guaranteed_params import (
        PaymentRecordReportPaymentAttemptGuaranteedParams,
    )
    from stripe.params._payment_record_report_payment_attempt_informational_params import (
        PaymentRecordReportPaymentAttemptInformationalParams,
    )
    from stripe.params._payment_record_report_payment_attempt_params import (
        PaymentRecordReportPaymentAttemptParams,
    )
    from stripe.params._payment_record_report_payment_params import (
        PaymentRecordReportPaymentParams,
    )
    from stripe.params._payment_record_report_refund_params import (
        PaymentRecordReportRefundParams,
    )
    from stripe.params._payment_record_retrieve_params import (
        PaymentRecordRetrieveParams,
    )


class PaymentRecord(APIResource["PaymentRecord"]):
    """
    A Payment Record is a resource that allows you to represent payments that occur on- or off-Stripe.
    For example, you can create a Payment Record to model a payment made on a different payment processor,
    in order to mark an Invoice as paid and a Subscription as active. Payment Records consist of one or
    more Payment Attempt Records, which represent individual attempts made on a payment network.
    """

    OBJECT_NAME: ClassVar[Literal["payment_record"]] = "payment_record"

    class Amount(StripeObject):
        currency: str
        """
        Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        value: int
        """
        A positive integer representing the amount in the currency's [minor unit](https://docs.stripe.com/currencies#zero-decimal). For example, `100` can represent 1 USD or 100 JPY.
        """

    class AmountAuthorized(StripeObject):
        currency: str
        """
        Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        value: int
        """
        A positive integer representing the amount in the currency's [minor unit](https://docs.stripe.com/currencies#zero-decimal). For example, `100` can represent 1 USD or 100 JPY.
        """

    class AmountCanceled(StripeObject):
        currency: str
        """
        Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        value: int
        """
        A positive integer representing the amount in the currency's [minor unit](https://docs.stripe.com/currencies#zero-decimal). For example, `100` can represent 1 USD or 100 JPY.
        """

    class AmountFailed(StripeObject):
        currency: str
        """
        Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        value: int
        """
        A positive integer representing the amount in the currency's [minor unit](https://docs.stripe.com/currencies#zero-decimal). For example, `100` can represent 1 USD or 100 JPY.
        """

    class AmountGuaranteed(StripeObject):
        currency: str
        """
        Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        value: int
        """
        A positive integer representing the amount in the currency's [minor unit](https://docs.stripe.com/currencies#zero-decimal). For example, `100` can represent 1 USD or 100 JPY.
        """

    class AmountRefunded(StripeObject):
        currency: str
        """
        Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        value: int
        """
        A positive integer representing the amount in the currency's [minor unit](https://docs.stripe.com/currencies#zero-decimal). For example, `100` can represent 1 USD or 100 JPY.
        """

    class AmountRequested(StripeObject):
        currency: str
        """
        Three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase. Must be a [supported currency](https://stripe.com/docs/currencies).
        """
        value: int
        """
        A positive integer representing the amount in the currency's [minor unit](https://docs.stripe.com/currencies#zero-decimal). For example, `100` can represent 1 USD or 100 JPY.
        """

    class CustomerDetails(StripeObject):
        customer: Optional[str]
        """
        ID of the Stripe Customer associated with this payment.
        """
        email: Optional[str]
        """
        The customer's email address.
        """
        name: Optional[str]
        """
        The customer's name.
        """
        phone: Optional[str]
        """
        The customer's phone number.
        """

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
            ID of the location that this reader is assigned to.
            """
            reader: Optional[str]
            """
            ID of the reader this transaction was made on.
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
            Preferred language of the Bancontact authorization page that the customer is redirected to. Can be one of `en`, `de`, `fr`, or `nl`
            """
            verified_name: Optional[str]
            """
            Owner's verified full name. Values are verified or provided by Bancontact directly (if supported) at the time of authorization or settlement. They cannot be set or mutated.
            """

        class Billie(StripeObject):
            transaction_id: Optional[str]
            """
            The Billie transaction ID associated with this payment.
            """

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
            A representation of a physical address.
            """
            email: Optional[str]
            """
            The billing email associated with the method of payment.
            """
            name: Optional[str]
            """
            The billing name associated with the method of payment.
            """
            phone: Optional[str]
            """
            The billing phone number associated with the method of payment.
            """
            _inner_class_types = {"address": Address}

        class Blik(StripeObject):
            buyer_id: Optional[str]
            """
            A unique and immutable identifier assigned by BLIK to every buyer.
            """

        class Boleto(StripeObject):
            tax_id: Optional[str]
            """
            The tax ID of the customer (CPF for individuals consumers or CNPJ for businesses consumers)
            """

        class Card(StripeObject):
            class Checks(StripeObject):
                address_line1_check: Optional[
                    Literal["fail", "pass", "unavailable", "unchecked"]
                ]
                """
                If you provide a value for `address.line1`, the check result is one of `pass`, `fail`, `unavailable`, or `unchecked`.
                """
                address_postal_code_check: Optional[
                    Literal["fail", "pass", "unavailable", "unchecked"]
                ]
                """
                If you provide a address postal code, the check result is one of `pass`, `fail`, `unavailable`, or `unchecked`.
                """
                cvc_check: Optional[
                    Literal["fail", "pass", "unavailable", "unchecked"]
                ]
                """
                If you provide a CVC, the check results is one of `pass`, `fail`, `unavailable`, or `unchecked`.
                """

            class Installments(StripeObject):
                class Plan(StripeObject):
                    count: Optional[int]
                    """
                    For `fixed_count` installment plans, this is the number of installment payments your customer will make to their credit card.
                    """
                    interval: Optional[Literal["month"]]
                    """
                    For `fixed_count` installment plans, this is the interval between installment payments your customer will make to their credit card. One of `month`.
                    """
                    type: Literal["bonus", "fixed_count", "revolving"]
                    """
                    Type of installment plan, one of `fixed_count`, `revolving`, or `bonus`.
                    """

                plan: Optional[Plan]
                """
                Installment plan selected for the payment.
                """
                _inner_class_types = {"plan": Plan}

            class NetworkToken(StripeObject):
                used: bool
                """
                Indicates if Stripe used a network token, either user provided or Stripe managed when processing the transaction.
                """

            class ThreeDSecure(StripeObject):
                authentication_flow: Optional[
                    Literal["challenge", "frictionless"]
                ]
                """
                For authenticated transactions: Indicates how the issuing bank authenticated the customer.
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
                Additional information about why 3D Secure succeeded or failed, based on the `result`.
                """
                version: Optional[Literal["1.0.2", "2.1.0", "2.2.0"]]
                """
                The version of 3D Secure that was used.
                """

            class Wallet(StripeObject):
                class ApplePay(StripeObject):
                    type: str
                    """
                    Type of the apple_pay transaction, one of `apple_pay` or `apple_pay_later`.
                    """

                class GooglePay(StripeObject):
                    pass

                apple_pay: Optional[ApplePay]
                dynamic_last4: Optional[str]
                """
                (For tokenized numbers only.) The last four digits of the device account number.
                """
                google_pay: Optional[GooglePay]
                type: str
                """
                The type of the card wallet, one of `apple_pay` or `google_pay`. An additional hash is included on the Wallet subhash with a name matching this value. It contains additional information specific to the card wallet type.
                """
                _inner_class_types = {
                    "apple_pay": ApplePay,
                    "google_pay": GooglePay,
                }

            authorization_code: Optional[str]
            """
            The authorization code of the payment.
            """
            brand: Literal[
                "amex",
                "cartes_bancaires",
                "diners",
                "discover",
                "eftpos_au",
                "interac",
                "jcb",
                "link",
                "mastercard",
                "unionpay",
                "unknown",
                "visa",
            ]
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
            A high-level description of the type of cards issued in this range.
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
            funding: Literal["credit", "debit", "prepaid", "unknown"]
            """
            Card funding type. Can be `credit`, `debit`, `prepaid`, or `unknown`.
            """
            iin: Optional[str]
            """
            Issuer identification number of the card.
            """
            installments: Optional[Installments]
            """
            Installment details for this payment.
            """
            issuer: Optional[str]
            """
            The name of the card's issuing bank.
            """
            last4: str
            """
            The last four digits of the card.
            """
            moto: Optional[bool]
            """
            True if this payment was marked as MOTO and out of scope for SCA.
            """
            network: Optional[
                Literal[
                    "amex",
                    "cartes_bancaires",
                    "diners",
                    "discover",
                    "eftpos_au",
                    "interac",
                    "jcb",
                    "link",
                    "mastercard",
                    "unionpay",
                    "unknown",
                    "visa",
                ]
            ]
            """
            Identifies which network this charge was processed on. Can be `amex`, `cartes_bancaires`, `diners`, `discover`, `eftpos_au`, `interac`, `jcb`, `link`, `mastercard`, `unionpay`, `visa`, or `unknown`.
            """
            network_advice_code: Optional[str]
            """
            Advice code from the card network for the failed payment.
            """
            network_decline_code: Optional[str]
            """
            Decline code from the card network for the failed payment.
            """
            network_token: Optional[NetworkToken]
            """
            If this card has network token credentials, this contains the details of the network token credentials.
            """
            network_transaction_id: Optional[str]
            """
            This is used by the financial networks to identify a transaction. Visa calls this the Transaction ID, Mastercard calls this the Trace ID, and American Express calls this the Acquirer Reference Data. This value will be present if it is returned by the financial network in the authorization response, and null otherwise.
            """
            stored_credential_usage: Optional[
                Literal["recurring", "unscheduled"]
            ]
            """
            The transaction type that was passed for an off-session, Merchant-Initiated transaction, one of `recurring` or `unscheduled`.
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
                "installments": Installments,
                "network_token": NetworkToken,
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
            A unique and immutable identifier of payments assigned by Cash App.
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

        class Custom(StripeObject):
            display_name: str
            """
            Display name for the custom (user-defined) payment method type used to make this payment.
            """
            type: Optional[str]
            """
            The custom payment method type associated with this payment.
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
            Owner's verified full name. Values are verified or provided by Giropay directly (if supported) at the time of authorization or settlement. They cannot be set or mutated. Giropay rarely provides this information so the attribute is usually empty.
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
            The type of entity that holds the account. This can be either 'individual' or 'company'.
            """
            account_type: Optional[Literal["checking", "savings"]]
            """
            The type of the bank account. This can be either 'checking' or 'savings'.
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
            The ACH payment reference for this transaction.
            """
            routing_number: Optional[str]
            """
            The routing number for the bank account.
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
        billing_details: Optional[BillingDetails]
        """
        The billing details associated with the method of payment.
        """
        blik: Optional[Blik]
        boleto: Optional[Boleto]
        card: Optional[Card]
        """
        Details of the card used for this payment attempt.
        """
        card_present: Optional[CardPresent]
        cashapp: Optional[Cashapp]
        crypto: Optional[Crypto]
        custom: Optional[Custom]
        """
        Custom Payment Methods represent Payment Method types not modeled directly in
        the Stripe API. This resource consists of details about the custom payment method
        used for this payment attempt.
        """
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
        payment_method: Optional[str]
        """
        ID of the Stripe PaymentMethod used to make this payment.
        """
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
            "billing_details": BillingDetails,
            "blik": Blik,
            "boleto": Boleto,
            "card": Card,
            "card_present": CardPresent,
            "cashapp": Cashapp,
            "crypto": Crypto,
            "custom": Custom,
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

    class ProcessorDetails(StripeObject):
        class Custom(StripeObject):
            payment_reference: Optional[str]
            """
            An opaque string for manual reconciliation of this payment, for example a check number or a payment processor ID.
            """

        custom: Optional[Custom]
        """
        Custom processors represent payment processors not modeled directly in
        the Stripe API. This resource consists of details about the custom processor
        used for this payment attempt.
        """
        type: Literal["custom"]
        """
        The processor used for this payment attempt.
        """
        _inner_class_types = {"custom": Custom}

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
        A representation of a physical address.
        """
        name: Optional[str]
        """
        The shipping recipient's name.
        """
        phone: Optional[str]
        """
        The shipping recipient's phone number.
        """
        _inner_class_types = {"address": Address}

    amount: Amount
    """
    A representation of an amount of money, consisting of an amount and a currency.
    """
    amount_authorized: AmountAuthorized
    """
    A representation of an amount of money, consisting of an amount and a currency.
    """
    amount_canceled: AmountCanceled
    """
    A representation of an amount of money, consisting of an amount and a currency.
    """
    amount_failed: AmountFailed
    """
    A representation of an amount of money, consisting of an amount and a currency.
    """
    amount_guaranteed: AmountGuaranteed
    """
    A representation of an amount of money, consisting of an amount and a currency.
    """
    amount_refunded: AmountRefunded
    """
    A representation of an amount of money, consisting of an amount and a currency.
    """
    amount_requested: AmountRequested
    """
    A representation of an amount of money, consisting of an amount and a currency.
    """
    application: Optional[str]
    """
    ID of the Connect application that created the PaymentRecord.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    customer_details: Optional[CustomerDetails]
    """
    Customer information for this payment.
    """
    customer_presence: Optional[Literal["off_session", "on_session"]]
    """
    Indicates whether the customer was present in your checkout flow during this payment.
    """
    description: Optional[str]
    """
    An arbitrary string attached to the object. Often useful for displaying to users.
    """
    id: str
    """
    Unique identifier for the object.
    """
    latest_payment_attempt_record: Optional[str]
    """
    ID of the latest Payment Attempt Record attached to this Payment Record.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    metadata: Dict[str, str]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    object: Literal["payment_record"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    payment_method_details: Optional[PaymentMethodDetails]
    """
    Information about the Payment Method debited for this payment.
    """
    processor_details: ProcessorDetails
    """
    Processor information associated with this payment.
    """
    reported_by: Literal["self", "stripe"]
    """
    Indicates who reported the payment.
    """
    shipping_details: Optional[ShippingDetails]
    """
    Shipping information for this payment.
    """

    @classmethod
    def report_payment(
        cls, **params: Unpack["PaymentRecordReportPaymentParams"]
    ) -> "PaymentRecord":
        """
        Report a new Payment Record. You may report a Payment Record as it is
         initialized and later report updates through the other report_* methods, or report Payment
         Records in a terminal state directly, through this method.
        """
        return cast(
            "PaymentRecord",
            cls._static_request(
                "post",
                "/v1/payment_records/report_payment",
                params=params,
            ),
        )

    @classmethod
    async def report_payment_async(
        cls, **params: Unpack["PaymentRecordReportPaymentParams"]
    ) -> "PaymentRecord":
        """
        Report a new Payment Record. You may report a Payment Record as it is
         initialized and later report updates through the other report_* methods, or report Payment
         Records in a terminal state directly, through this method.
        """
        return cast(
            "PaymentRecord",
            await cls._static_request_async(
                "post",
                "/v1/payment_records/report_payment",
                params=params,
            ),
        )

    @classmethod
    def _cls_report_payment_attempt(
        cls,
        id: str,
        **params: Unpack["PaymentRecordReportPaymentAttemptParams"],
    ) -> "PaymentRecord":
        """
        Report a new payment attempt on the specified Payment Record. A new payment
         attempt can only be specified if all other payment attempts are canceled or failed.
        """
        return cast(
            "PaymentRecord",
            cls._static_request(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def report_payment_attempt(
        id: str, **params: Unpack["PaymentRecordReportPaymentAttemptParams"]
    ) -> "PaymentRecord":
        """
        Report a new payment attempt on the specified Payment Record. A new payment
         attempt can only be specified if all other payment attempts are canceled or failed.
        """
        ...

    @overload
    def report_payment_attempt(
        self, **params: Unpack["PaymentRecordReportPaymentAttemptParams"]
    ) -> "PaymentRecord":
        """
        Report a new payment attempt on the specified Payment Record. A new payment
         attempt can only be specified if all other payment attempts are canceled or failed.
        """
        ...

    @class_method_variant("_cls_report_payment_attempt")
    def report_payment_attempt(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentRecordReportPaymentAttemptParams"]
    ) -> "PaymentRecord":
        """
        Report a new payment attempt on the specified Payment Record. A new payment
         attempt can only be specified if all other payment attempts are canceled or failed.
        """
        return cast(
            "PaymentRecord",
            self._request(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_report_payment_attempt_async(
        cls,
        id: str,
        **params: Unpack["PaymentRecordReportPaymentAttemptParams"],
    ) -> "PaymentRecord":
        """
        Report a new payment attempt on the specified Payment Record. A new payment
         attempt can only be specified if all other payment attempts are canceled or failed.
        """
        return cast(
            "PaymentRecord",
            await cls._static_request_async(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def report_payment_attempt_async(
        id: str, **params: Unpack["PaymentRecordReportPaymentAttemptParams"]
    ) -> "PaymentRecord":
        """
        Report a new payment attempt on the specified Payment Record. A new payment
         attempt can only be specified if all other payment attempts are canceled or failed.
        """
        ...

    @overload
    async def report_payment_attempt_async(
        self, **params: Unpack["PaymentRecordReportPaymentAttemptParams"]
    ) -> "PaymentRecord":
        """
        Report a new payment attempt on the specified Payment Record. A new payment
         attempt can only be specified if all other payment attempts are canceled or failed.
        """
        ...

    @class_method_variant("_cls_report_payment_attempt_async")
    async def report_payment_attempt_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentRecordReportPaymentAttemptParams"]
    ) -> "PaymentRecord":
        """
        Report a new payment attempt on the specified Payment Record. A new payment
         attempt can only be specified if all other payment attempts are canceled or failed.
        """
        return cast(
            "PaymentRecord",
            await self._request_async(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_report_payment_attempt_canceled(
        cls,
        id: str,
        **params: Unpack["PaymentRecordReportPaymentAttemptCanceledParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was canceled.
        """
        return cast(
            "PaymentRecord",
            cls._static_request(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_canceled".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def report_payment_attempt_canceled(
        id: str,
        **params: Unpack["PaymentRecordReportPaymentAttemptCanceledParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was canceled.
        """
        ...

    @overload
    def report_payment_attempt_canceled(
        self,
        **params: Unpack["PaymentRecordReportPaymentAttemptCanceledParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was canceled.
        """
        ...

    @class_method_variant("_cls_report_payment_attempt_canceled")
    def report_payment_attempt_canceled(  # pyright: ignore[reportGeneralTypeIssues]
        self,
        **params: Unpack["PaymentRecordReportPaymentAttemptCanceledParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was canceled.
        """
        return cast(
            "PaymentRecord",
            self._request(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_canceled".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_report_payment_attempt_canceled_async(
        cls,
        id: str,
        **params: Unpack["PaymentRecordReportPaymentAttemptCanceledParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was canceled.
        """
        return cast(
            "PaymentRecord",
            await cls._static_request_async(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_canceled".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def report_payment_attempt_canceled_async(
        id: str,
        **params: Unpack["PaymentRecordReportPaymentAttemptCanceledParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was canceled.
        """
        ...

    @overload
    async def report_payment_attempt_canceled_async(
        self,
        **params: Unpack["PaymentRecordReportPaymentAttemptCanceledParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was canceled.
        """
        ...

    @class_method_variant("_cls_report_payment_attempt_canceled_async")
    async def report_payment_attempt_canceled_async(  # pyright: ignore[reportGeneralTypeIssues]
        self,
        **params: Unpack["PaymentRecordReportPaymentAttemptCanceledParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was canceled.
        """
        return cast(
            "PaymentRecord",
            await self._request_async(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_canceled".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_report_payment_attempt_failed(
        cls,
        id: str,
        **params: Unpack["PaymentRecordReportPaymentAttemptFailedParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         failed or errored.
        """
        return cast(
            "PaymentRecord",
            cls._static_request(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_failed".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def report_payment_attempt_failed(
        id: str,
        **params: Unpack["PaymentRecordReportPaymentAttemptFailedParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         failed or errored.
        """
        ...

    @overload
    def report_payment_attempt_failed(
        self, **params: Unpack["PaymentRecordReportPaymentAttemptFailedParams"]
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         failed or errored.
        """
        ...

    @class_method_variant("_cls_report_payment_attempt_failed")
    def report_payment_attempt_failed(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentRecordReportPaymentAttemptFailedParams"]
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         failed or errored.
        """
        return cast(
            "PaymentRecord",
            self._request(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_failed".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_report_payment_attempt_failed_async(
        cls,
        id: str,
        **params: Unpack["PaymentRecordReportPaymentAttemptFailedParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         failed or errored.
        """
        return cast(
            "PaymentRecord",
            await cls._static_request_async(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_failed".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def report_payment_attempt_failed_async(
        id: str,
        **params: Unpack["PaymentRecordReportPaymentAttemptFailedParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         failed or errored.
        """
        ...

    @overload
    async def report_payment_attempt_failed_async(
        self, **params: Unpack["PaymentRecordReportPaymentAttemptFailedParams"]
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         failed or errored.
        """
        ...

    @class_method_variant("_cls_report_payment_attempt_failed_async")
    async def report_payment_attempt_failed_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentRecordReportPaymentAttemptFailedParams"]
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         failed or errored.
        """
        return cast(
            "PaymentRecord",
            await self._request_async(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_failed".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_report_payment_attempt_guaranteed(
        cls,
        id: str,
        **params: Unpack["PaymentRecordReportPaymentAttemptGuaranteedParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was guaranteed.
        """
        return cast(
            "PaymentRecord",
            cls._static_request(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_guaranteed".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def report_payment_attempt_guaranteed(
        id: str,
        **params: Unpack["PaymentRecordReportPaymentAttemptGuaranteedParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was guaranteed.
        """
        ...

    @overload
    def report_payment_attempt_guaranteed(
        self,
        **params: Unpack["PaymentRecordReportPaymentAttemptGuaranteedParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was guaranteed.
        """
        ...

    @class_method_variant("_cls_report_payment_attempt_guaranteed")
    def report_payment_attempt_guaranteed(  # pyright: ignore[reportGeneralTypeIssues]
        self,
        **params: Unpack["PaymentRecordReportPaymentAttemptGuaranteedParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was guaranteed.
        """
        return cast(
            "PaymentRecord",
            self._request(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_guaranteed".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_report_payment_attempt_guaranteed_async(
        cls,
        id: str,
        **params: Unpack["PaymentRecordReportPaymentAttemptGuaranteedParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was guaranteed.
        """
        return cast(
            "PaymentRecord",
            await cls._static_request_async(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_guaranteed".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def report_payment_attempt_guaranteed_async(
        id: str,
        **params: Unpack["PaymentRecordReportPaymentAttemptGuaranteedParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was guaranteed.
        """
        ...

    @overload
    async def report_payment_attempt_guaranteed_async(
        self,
        **params: Unpack["PaymentRecordReportPaymentAttemptGuaranteedParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was guaranteed.
        """
        ...

    @class_method_variant("_cls_report_payment_attempt_guaranteed_async")
    async def report_payment_attempt_guaranteed_async(  # pyright: ignore[reportGeneralTypeIssues]
        self,
        **params: Unpack["PaymentRecordReportPaymentAttemptGuaranteedParams"],
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was guaranteed.
        """
        return cast(
            "PaymentRecord",
            await self._request_async(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_guaranteed".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_report_payment_attempt_informational(
        cls,
        id: str,
        **params: Unpack[
            "PaymentRecordReportPaymentAttemptInformationalParams"
        ],
    ) -> "PaymentRecord":
        """
        Report informational updates on the specified Payment Record.
        """
        return cast(
            "PaymentRecord",
            cls._static_request(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_informational".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def report_payment_attempt_informational(
        id: str,
        **params: Unpack[
            "PaymentRecordReportPaymentAttemptInformationalParams"
        ],
    ) -> "PaymentRecord":
        """
        Report informational updates on the specified Payment Record.
        """
        ...

    @overload
    def report_payment_attempt_informational(
        self,
        **params: Unpack[
            "PaymentRecordReportPaymentAttemptInformationalParams"
        ],
    ) -> "PaymentRecord":
        """
        Report informational updates on the specified Payment Record.
        """
        ...

    @class_method_variant("_cls_report_payment_attempt_informational")
    def report_payment_attempt_informational(  # pyright: ignore[reportGeneralTypeIssues]
        self,
        **params: Unpack[
            "PaymentRecordReportPaymentAttemptInformationalParams"
        ],
    ) -> "PaymentRecord":
        """
        Report informational updates on the specified Payment Record.
        """
        return cast(
            "PaymentRecord",
            self._request(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_informational".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_report_payment_attempt_informational_async(
        cls,
        id: str,
        **params: Unpack[
            "PaymentRecordReportPaymentAttemptInformationalParams"
        ],
    ) -> "PaymentRecord":
        """
        Report informational updates on the specified Payment Record.
        """
        return cast(
            "PaymentRecord",
            await cls._static_request_async(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_informational".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def report_payment_attempt_informational_async(
        id: str,
        **params: Unpack[
            "PaymentRecordReportPaymentAttemptInformationalParams"
        ],
    ) -> "PaymentRecord":
        """
        Report informational updates on the specified Payment Record.
        """
        ...

    @overload
    async def report_payment_attempt_informational_async(
        self,
        **params: Unpack[
            "PaymentRecordReportPaymentAttemptInformationalParams"
        ],
    ) -> "PaymentRecord":
        """
        Report informational updates on the specified Payment Record.
        """
        ...

    @class_method_variant("_cls_report_payment_attempt_informational_async")
    async def report_payment_attempt_informational_async(  # pyright: ignore[reportGeneralTypeIssues]
        self,
        **params: Unpack[
            "PaymentRecordReportPaymentAttemptInformationalParams"
        ],
    ) -> "PaymentRecord":
        """
        Report informational updates on the specified Payment Record.
        """
        return cast(
            "PaymentRecord",
            await self._request_async(
                "post",
                "/v1/payment_records/{id}/report_payment_attempt_informational".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_report_refund(
        cls, id: str, **params: Unpack["PaymentRecordReportRefundParams"]
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was refunded.
        """
        return cast(
            "PaymentRecord",
            cls._static_request(
                "post",
                "/v1/payment_records/{id}/report_refund".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def report_refund(
        id: str, **params: Unpack["PaymentRecordReportRefundParams"]
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was refunded.
        """
        ...

    @overload
    def report_refund(
        self, **params: Unpack["PaymentRecordReportRefundParams"]
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was refunded.
        """
        ...

    @class_method_variant("_cls_report_refund")
    def report_refund(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentRecordReportRefundParams"]
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was refunded.
        """
        return cast(
            "PaymentRecord",
            self._request(
                "post",
                "/v1/payment_records/{id}/report_refund".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_report_refund_async(
        cls, id: str, **params: Unpack["PaymentRecordReportRefundParams"]
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was refunded.
        """
        return cast(
            "PaymentRecord",
            await cls._static_request_async(
                "post",
                "/v1/payment_records/{id}/report_refund".format(
                    id=sanitize_id(id)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def report_refund_async(
        id: str, **params: Unpack["PaymentRecordReportRefundParams"]
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was refunded.
        """
        ...

    @overload
    async def report_refund_async(
        self, **params: Unpack["PaymentRecordReportRefundParams"]
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was refunded.
        """
        ...

    @class_method_variant("_cls_report_refund_async")
    async def report_refund_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["PaymentRecordReportRefundParams"]
    ) -> "PaymentRecord":
        """
        Report that the most recent payment attempt on the specified Payment Record
         was refunded.
        """
        return cast(
            "PaymentRecord",
            await self._request_async(
                "post",
                "/v1/payment_records/{id}/report_refund".format(
                    id=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["PaymentRecordRetrieveParams"]
    ) -> "PaymentRecord":
        """
        Retrieves a Payment Record with the given ID
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["PaymentRecordRetrieveParams"]
    ) -> "PaymentRecord":
        """
        Retrieves a Payment Record with the given ID
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "amount": Amount,
        "amount_authorized": AmountAuthorized,
        "amount_canceled": AmountCanceled,
        "amount_failed": AmountFailed,
        "amount_guaranteed": AmountGuaranteed,
        "amount_refunded": AmountRefunded,
        "amount_requested": AmountRequested,
        "customer_details": CustomerDetails,
        "payment_method_details": PaymentMethodDetails,
        "processor_details": ProcessorDetails,
        "shipping_details": ShippingDetails,
    }
