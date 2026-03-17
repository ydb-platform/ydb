# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import Dict, List
from typing_extensions import Literal, NotRequired, TypedDict


class PaymentMethodCreateParams(RequestOptions):
    acss_debit: NotRequired["PaymentMethodCreateParamsAcssDebit"]
    """
    If this is an `acss_debit` PaymentMethod, this hash contains details about the ACSS Debit payment method.
    """
    affirm: NotRequired["PaymentMethodCreateParamsAffirm"]
    """
    If this is an `affirm` PaymentMethod, this hash contains details about the Affirm payment method.
    """
    afterpay_clearpay: NotRequired["PaymentMethodCreateParamsAfterpayClearpay"]
    """
    If this is an `AfterpayClearpay` PaymentMethod, this hash contains details about the AfterpayClearpay payment method.
    """
    alipay: NotRequired["PaymentMethodCreateParamsAlipay"]
    """
    If this is an `Alipay` PaymentMethod, this hash contains details about the Alipay payment method.
    """
    allow_redisplay: NotRequired[Literal["always", "limited", "unspecified"]]
    """
    This field indicates whether this payment method can be shown again to its customer in a checkout flow. Stripe products such as Checkout and Elements use this field to determine whether a payment method can be shown as a saved payment method in a checkout flow. The field defaults to `unspecified`.
    """
    alma: NotRequired["PaymentMethodCreateParamsAlma"]
    """
    If this is a Alma PaymentMethod, this hash contains details about the Alma payment method.
    """
    amazon_pay: NotRequired["PaymentMethodCreateParamsAmazonPay"]
    """
    If this is a AmazonPay PaymentMethod, this hash contains details about the AmazonPay payment method.
    """
    au_becs_debit: NotRequired["PaymentMethodCreateParamsAuBecsDebit"]
    """
    If this is an `au_becs_debit` PaymentMethod, this hash contains details about the bank account.
    """
    bacs_debit: NotRequired["PaymentMethodCreateParamsBacsDebit"]
    """
    If this is a `bacs_debit` PaymentMethod, this hash contains details about the Bacs Direct Debit bank account.
    """
    bancontact: NotRequired["PaymentMethodCreateParamsBancontact"]
    """
    If this is a `bancontact` PaymentMethod, this hash contains details about the Bancontact payment method.
    """
    billie: NotRequired["PaymentMethodCreateParamsBillie"]
    """
    If this is a `billie` PaymentMethod, this hash contains details about the Billie payment method.
    """
    billing_details: NotRequired["PaymentMethodCreateParamsBillingDetails"]
    """
    Billing information associated with the PaymentMethod that may be used or required by particular types of payment methods.
    """
    blik: NotRequired["PaymentMethodCreateParamsBlik"]
    """
    If this is a `blik` PaymentMethod, this hash contains details about the BLIK payment method.
    """
    boleto: NotRequired["PaymentMethodCreateParamsBoleto"]
    """
    If this is a `boleto` PaymentMethod, this hash contains details about the Boleto payment method.
    """
    card: NotRequired["PaymentMethodCreateParamsCard"]
    """
    If this is a `card` PaymentMethod, this hash contains the user's card details. For backwards compatibility, you can alternatively provide a Stripe token (e.g., for Apple Pay, Amex Express Checkout, or legacy Checkout) into the card hash with format `card: {token: "tok_visa"}`. When providing a card number, you must meet the requirements for [PCI compliance](https://stripe.com/docs/security#validating-pci-compliance). We strongly recommend using Stripe.js instead of interacting with this API directly.
    """
    cashapp: NotRequired["PaymentMethodCreateParamsCashapp"]
    """
    If this is a `cashapp` PaymentMethod, this hash contains details about the Cash App Pay payment method.
    """
    crypto: NotRequired["PaymentMethodCreateParamsCrypto"]
    """
    If this is a Crypto PaymentMethod, this hash contains details about the Crypto payment method.
    """
    custom: NotRequired["PaymentMethodCreateParamsCustom"]
    """
    If this is a `custom` PaymentMethod, this hash contains details about the Custom payment method.
    """
    customer: NotRequired[str]
    """
    The `Customer` to whom the original PaymentMethod is attached.
    """
    customer_balance: NotRequired["PaymentMethodCreateParamsCustomerBalance"]
    """
    If this is a `customer_balance` PaymentMethod, this hash contains details about the CustomerBalance payment method.
    """
    eps: NotRequired["PaymentMethodCreateParamsEps"]
    """
    If this is an `eps` PaymentMethod, this hash contains details about the EPS payment method.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    fpx: NotRequired["PaymentMethodCreateParamsFpx"]
    """
    If this is an `fpx` PaymentMethod, this hash contains details about the FPX payment method.
    """
    giropay: NotRequired["PaymentMethodCreateParamsGiropay"]
    """
    If this is a `giropay` PaymentMethod, this hash contains details about the Giropay payment method.
    """
    grabpay: NotRequired["PaymentMethodCreateParamsGrabpay"]
    """
    If this is a `grabpay` PaymentMethod, this hash contains details about the GrabPay payment method.
    """
    ideal: NotRequired["PaymentMethodCreateParamsIdeal"]
    """
    If this is an `ideal` PaymentMethod, this hash contains details about the iDEAL payment method.
    """
    interac_present: NotRequired["PaymentMethodCreateParamsInteracPresent"]
    """
    If this is an `interac_present` PaymentMethod, this hash contains details about the Interac Present payment method.
    """
    kakao_pay: NotRequired["PaymentMethodCreateParamsKakaoPay"]
    """
    If this is a `kakao_pay` PaymentMethod, this hash contains details about the Kakao Pay payment method.
    """
    klarna: NotRequired["PaymentMethodCreateParamsKlarna"]
    """
    If this is a `klarna` PaymentMethod, this hash contains details about the Klarna payment method.
    """
    konbini: NotRequired["PaymentMethodCreateParamsKonbini"]
    """
    If this is a `konbini` PaymentMethod, this hash contains details about the Konbini payment method.
    """
    kr_card: NotRequired["PaymentMethodCreateParamsKrCard"]
    """
    If this is a `kr_card` PaymentMethod, this hash contains details about the Korean Card payment method.
    """
    link: NotRequired["PaymentMethodCreateParamsLink"]
    """
    If this is an `Link` PaymentMethod, this hash contains details about the Link payment method.
    """
    mb_way: NotRequired["PaymentMethodCreateParamsMbWay"]
    """
    If this is a MB WAY PaymentMethod, this hash contains details about the MB WAY payment method.
    """
    metadata: NotRequired[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format. Individual keys can be unset by posting an empty value to them. All keys can be unset by posting an empty value to `metadata`.
    """
    mobilepay: NotRequired["PaymentMethodCreateParamsMobilepay"]
    """
    If this is a `mobilepay` PaymentMethod, this hash contains details about the MobilePay payment method.
    """
    multibanco: NotRequired["PaymentMethodCreateParamsMultibanco"]
    """
    If this is a `multibanco` PaymentMethod, this hash contains details about the Multibanco payment method.
    """
    naver_pay: NotRequired["PaymentMethodCreateParamsNaverPay"]
    """
    If this is a `naver_pay` PaymentMethod, this hash contains details about the Naver Pay payment method.
    """
    nz_bank_account: NotRequired["PaymentMethodCreateParamsNzBankAccount"]
    """
    If this is an nz_bank_account PaymentMethod, this hash contains details about the nz_bank_account payment method.
    """
    oxxo: NotRequired["PaymentMethodCreateParamsOxxo"]
    """
    If this is an `oxxo` PaymentMethod, this hash contains details about the OXXO payment method.
    """
    p24: NotRequired["PaymentMethodCreateParamsP24"]
    """
    If this is a `p24` PaymentMethod, this hash contains details about the P24 payment method.
    """
    pay_by_bank: NotRequired["PaymentMethodCreateParamsPayByBank"]
    """
    If this is a `pay_by_bank` PaymentMethod, this hash contains details about the PayByBank payment method.
    """
    payco: NotRequired["PaymentMethodCreateParamsPayco"]
    """
    If this is a `payco` PaymentMethod, this hash contains details about the PAYCO payment method.
    """
    payment_method: NotRequired[str]
    """
    The PaymentMethod to share.
    """
    paynow: NotRequired["PaymentMethodCreateParamsPaynow"]
    """
    If this is a `paynow` PaymentMethod, this hash contains details about the PayNow payment method.
    """
    paypal: NotRequired["PaymentMethodCreateParamsPaypal"]
    """
    If this is a `paypal` PaymentMethod, this hash contains details about the PayPal payment method.
    """
    payto: NotRequired["PaymentMethodCreateParamsPayto"]
    """
    If this is a `payto` PaymentMethod, this hash contains details about the PayTo payment method.
    """
    pix: NotRequired["PaymentMethodCreateParamsPix"]
    """
    If this is a `pix` PaymentMethod, this hash contains details about the Pix payment method.
    """
    promptpay: NotRequired["PaymentMethodCreateParamsPromptpay"]
    """
    If this is a `promptpay` PaymentMethod, this hash contains details about the PromptPay payment method.
    """
    radar_options: NotRequired["PaymentMethodCreateParamsRadarOptions"]
    """
    Options to configure Radar. See [Radar Session](https://docs.stripe.com/radar/radar-session) for more information.
    """
    revolut_pay: NotRequired["PaymentMethodCreateParamsRevolutPay"]
    """
    If this is a `revolut_pay` PaymentMethod, this hash contains details about the Revolut Pay payment method.
    """
    samsung_pay: NotRequired["PaymentMethodCreateParamsSamsungPay"]
    """
    If this is a `samsung_pay` PaymentMethod, this hash contains details about the SamsungPay payment method.
    """
    satispay: NotRequired["PaymentMethodCreateParamsSatispay"]
    """
    If this is a `satispay` PaymentMethod, this hash contains details about the Satispay payment method.
    """
    sepa_debit: NotRequired["PaymentMethodCreateParamsSepaDebit"]
    """
    If this is a `sepa_debit` PaymentMethod, this hash contains details about the SEPA debit bank account.
    """
    sofort: NotRequired["PaymentMethodCreateParamsSofort"]
    """
    If this is a `sofort` PaymentMethod, this hash contains details about the SOFORT payment method.
    """
    swish: NotRequired["PaymentMethodCreateParamsSwish"]
    """
    If this is a `swish` PaymentMethod, this hash contains details about the Swish payment method.
    """
    twint: NotRequired["PaymentMethodCreateParamsTwint"]
    """
    If this is a TWINT PaymentMethod, this hash contains details about the TWINT payment method.
    """
    type: NotRequired[
        Literal[
            "acss_debit",
            "affirm",
            "afterpay_clearpay",
            "alipay",
            "alma",
            "amazon_pay",
            "au_becs_debit",
            "bacs_debit",
            "bancontact",
            "billie",
            "blik",
            "boleto",
            "card",
            "cashapp",
            "crypto",
            "custom",
            "customer_balance",
            "eps",
            "fpx",
            "giropay",
            "grabpay",
            "ideal",
            "kakao_pay",
            "klarna",
            "konbini",
            "kr_card",
            "link",
            "mb_way",
            "mobilepay",
            "multibanco",
            "naver_pay",
            "nz_bank_account",
            "oxxo",
            "p24",
            "pay_by_bank",
            "payco",
            "paynow",
            "paypal",
            "payto",
            "pix",
            "promptpay",
            "revolut_pay",
            "samsung_pay",
            "satispay",
            "sepa_debit",
            "sofort",
            "swish",
            "twint",
            "us_bank_account",
            "wechat_pay",
            "zip",
        ]
    ]
    """
    The type of the PaymentMethod. An additional hash is included on the PaymentMethod with a name matching this value. It contains additional information specific to the PaymentMethod type.
    """
    us_bank_account: NotRequired["PaymentMethodCreateParamsUsBankAccount"]
    """
    If this is an `us_bank_account` PaymentMethod, this hash contains details about the US bank account payment method.
    """
    wechat_pay: NotRequired["PaymentMethodCreateParamsWechatPay"]
    """
    If this is an `wechat_pay` PaymentMethod, this hash contains details about the wechat_pay payment method.
    """
    zip: NotRequired["PaymentMethodCreateParamsZip"]
    """
    If this is a `zip` PaymentMethod, this hash contains details about the Zip payment method.
    """


class PaymentMethodCreateParamsAcssDebit(TypedDict):
    account_number: str
    """
    Customer's bank account number.
    """
    institution_number: str
    """
    Institution number of the customer's bank.
    """
    transit_number: str
    """
    Transit number of the customer's bank.
    """


class PaymentMethodCreateParamsAffirm(TypedDict):
    pass


class PaymentMethodCreateParamsAfterpayClearpay(TypedDict):
    pass


class PaymentMethodCreateParamsAlipay(TypedDict):
    pass


class PaymentMethodCreateParamsAlma(TypedDict):
    pass


class PaymentMethodCreateParamsAmazonPay(TypedDict):
    pass


class PaymentMethodCreateParamsAuBecsDebit(TypedDict):
    account_number: str
    """
    The account number for the bank account.
    """
    bsb_number: str
    """
    Bank-State-Branch number of the bank account.
    """


class PaymentMethodCreateParamsBacsDebit(TypedDict):
    account_number: NotRequired[str]
    """
    Account number of the bank account that the funds will be debited from.
    """
    sort_code: NotRequired[str]
    """
    Sort code of the bank account. (e.g., `10-20-30`)
    """


class PaymentMethodCreateParamsBancontact(TypedDict):
    pass


class PaymentMethodCreateParamsBillie(TypedDict):
    pass


class PaymentMethodCreateParamsBillingDetails(TypedDict):
    address: NotRequired[
        "Literal['']|PaymentMethodCreateParamsBillingDetailsAddress"
    ]
    """
    Billing address.
    """
    email: NotRequired["Literal['']|str"]
    """
    Email address.
    """
    name: NotRequired["Literal['']|str"]
    """
    Full name.
    """
    phone: NotRequired["Literal['']|str"]
    """
    Billing phone number (including extension).
    """
    tax_id: NotRequired[str]
    """
    Taxpayer identification number. Used only for transactions between LATAM buyers and non-LATAM sellers.
    """


class PaymentMethodCreateParamsBillingDetailsAddress(TypedDict):
    city: NotRequired[str]
    """
    City, district, suburb, town, or village.
    """
    country: NotRequired[str]
    """
    Two-letter country code ([ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)).
    """
    line1: NotRequired[str]
    """
    Address line 1, such as the street, PO Box, or company name.
    """
    line2: NotRequired[str]
    """
    Address line 2, such as the apartment, suite, unit, or building.
    """
    postal_code: NotRequired[str]
    """
    ZIP or postal code.
    """
    state: NotRequired[str]
    """
    State, county, province, or region ([ISO 3166-2](https://en.wikipedia.org/wiki/ISO_3166-2)).
    """


class PaymentMethodCreateParamsBlik(TypedDict):
    pass


class PaymentMethodCreateParamsBoleto(TypedDict):
    tax_id: str
    """
    The tax ID of the customer (CPF for individual consumers or CNPJ for businesses consumers)
    """


class PaymentMethodCreateParamsCard(TypedDict):
    cvc: NotRequired[str]
    """
    The card's CVC. It is highly recommended to always include this value.
    """
    exp_month: NotRequired[int]
    """
    Two-digit number representing the card's expiration month.
    """
    exp_year: NotRequired[int]
    """
    Four-digit number representing the card's expiration year.
    """
    networks: NotRequired["PaymentMethodCreateParamsCardNetworks"]
    """
    Contains information about card networks used to process the payment.
    """
    number: NotRequired[str]
    """
    The card number, as a string without any separators.
    """
    token: NotRequired[str]
    """
    For backwards compatibility, you can alternatively provide a Stripe token (e.g., for Apple Pay, Amex Express Checkout, or legacy Checkout) into the card hash with format card: {token: "tok_visa"}.
    """


class PaymentMethodCreateParamsCardNetworks(TypedDict):
    preferred: NotRequired[Literal["cartes_bancaires", "mastercard", "visa"]]
    """
    The customer's preferred card network for co-branded cards. Supports `cartes_bancaires`, `mastercard`, or `visa`. Selection of a network that does not apply to the card will be stored as `invalid_preference` on the card.
    """


class PaymentMethodCreateParamsCashapp(TypedDict):
    pass


class PaymentMethodCreateParamsCrypto(TypedDict):
    pass


class PaymentMethodCreateParamsCustom(TypedDict):
    type: str
    """
    ID of the Dashboard-only CustomPaymentMethodType. This field is used by Stripe products' internal code to support CPMs.
    """


class PaymentMethodCreateParamsCustomerBalance(TypedDict):
    pass


class PaymentMethodCreateParamsEps(TypedDict):
    bank: NotRequired[
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
    The customer's bank.
    """


class PaymentMethodCreateParamsFpx(TypedDict):
    account_holder_type: NotRequired[Literal["company", "individual"]]
    """
    Account holder type for FPX transaction
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
    The customer's bank.
    """


class PaymentMethodCreateParamsGiropay(TypedDict):
    pass


class PaymentMethodCreateParamsGrabpay(TypedDict):
    pass


class PaymentMethodCreateParamsIdeal(TypedDict):
    bank: NotRequired[
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
    The customer's bank. Only use this parameter for existing customers. Don't use it for new customers.
    """


class PaymentMethodCreateParamsInteracPresent(TypedDict):
    pass


class PaymentMethodCreateParamsKakaoPay(TypedDict):
    pass


class PaymentMethodCreateParamsKlarna(TypedDict):
    dob: NotRequired["PaymentMethodCreateParamsKlarnaDob"]
    """
    Customer's date of birth
    """


class PaymentMethodCreateParamsKlarnaDob(TypedDict):
    day: int
    """
    The day of birth, between 1 and 31.
    """
    month: int
    """
    The month of birth, between 1 and 12.
    """
    year: int
    """
    The four-digit year of birth.
    """


class PaymentMethodCreateParamsKonbini(TypedDict):
    pass


class PaymentMethodCreateParamsKrCard(TypedDict):
    pass


class PaymentMethodCreateParamsLink(TypedDict):
    pass


class PaymentMethodCreateParamsMbWay(TypedDict):
    pass


class PaymentMethodCreateParamsMobilepay(TypedDict):
    pass


class PaymentMethodCreateParamsMultibanco(TypedDict):
    pass


class PaymentMethodCreateParamsNaverPay(TypedDict):
    funding: NotRequired[Literal["card", "points"]]
    """
    Whether to use Naver Pay points or a card to fund this transaction. If not provided, this defaults to `card`.
    """


class PaymentMethodCreateParamsNzBankAccount(TypedDict):
    account_holder_name: NotRequired[str]
    """
    The name on the bank account. Only required if the account holder name is different from the name of the authorized signatory collected in the PaymentMethod's billing details.
    """
    account_number: str
    """
    The account number for the bank account.
    """
    bank_code: str
    """
    The numeric code for the bank account's bank.
    """
    branch_code: str
    """
    The numeric code for the bank account's bank branch.
    """
    reference: NotRequired[str]
    suffix: str
    """
    The suffix of the bank account number.
    """


class PaymentMethodCreateParamsOxxo(TypedDict):
    pass


class PaymentMethodCreateParamsP24(TypedDict):
    bank: NotRequired[
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
    The customer's bank.
    """


class PaymentMethodCreateParamsPayByBank(TypedDict):
    pass


class PaymentMethodCreateParamsPayco(TypedDict):
    pass


class PaymentMethodCreateParamsPaynow(TypedDict):
    pass


class PaymentMethodCreateParamsPaypal(TypedDict):
    pass


class PaymentMethodCreateParamsPayto(TypedDict):
    account_number: NotRequired[str]
    """
    The account number for the bank account.
    """
    bsb_number: NotRequired[str]
    """
    Bank-State-Branch number of the bank account.
    """
    pay_id: NotRequired[str]
    """
    The PayID alias for the bank account.
    """


class PaymentMethodCreateParamsPix(TypedDict):
    pass


class PaymentMethodCreateParamsPromptpay(TypedDict):
    pass


class PaymentMethodCreateParamsRadarOptions(TypedDict):
    session: NotRequired[str]
    """
    A [Radar Session](https://docs.stripe.com/radar/radar-session) is a snapshot of the browser metadata and device details that help Radar make more accurate predictions on your payments.
    """


class PaymentMethodCreateParamsRevolutPay(TypedDict):
    pass


class PaymentMethodCreateParamsSamsungPay(TypedDict):
    pass


class PaymentMethodCreateParamsSatispay(TypedDict):
    pass


class PaymentMethodCreateParamsSepaDebit(TypedDict):
    iban: str
    """
    IBAN of the bank account.
    """


class PaymentMethodCreateParamsSofort(TypedDict):
    country: Literal["AT", "BE", "DE", "ES", "IT", "NL"]
    """
    Two-letter ISO code representing the country the bank account is located in.
    """


class PaymentMethodCreateParamsSwish(TypedDict):
    pass


class PaymentMethodCreateParamsTwint(TypedDict):
    pass


class PaymentMethodCreateParamsUsBankAccount(TypedDict):
    account_holder_type: NotRequired[Literal["company", "individual"]]
    """
    Account holder type: individual or company.
    """
    account_number: NotRequired[str]
    """
    Account number of the bank account.
    """
    account_type: NotRequired[Literal["checking", "savings"]]
    """
    Account type: checkings or savings. Defaults to checking if omitted.
    """
    financial_connections_account: NotRequired[str]
    """
    The ID of a Financial Connections Account to use as a payment method.
    """
    routing_number: NotRequired[str]
    """
    Routing number of the bank account.
    """


class PaymentMethodCreateParamsWechatPay(TypedDict):
    pass


class PaymentMethodCreateParamsZip(TypedDict):
    pass
