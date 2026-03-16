# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._request_options import RequestOptions
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class PaymentMethodConfigurationModifyParams(RequestOptions):
    acss_debit: NotRequired["PaymentMethodConfigurationModifyParamsAcssDebit"]
    """
    Canadian pre-authorized debit payments, check this [page](https://docs.stripe.com/payments/acss-debit) for more details like country availability.
    """
    active: NotRequired[bool]
    """
    Whether the configuration can be used for new payments.
    """
    affirm: NotRequired["PaymentMethodConfigurationModifyParamsAffirm"]
    """
    [Affirm](https://www.affirm.com/) gives your customers a way to split purchases over a series of payments. Depending on the purchase, they can pay with four interest-free payments (Split Pay) or pay over a longer term (Installments), which might include interest. Check this [page](https://docs.stripe.com/payments/affirm) for more details like country availability.
    """
    afterpay_clearpay: NotRequired[
        "PaymentMethodConfigurationModifyParamsAfterpayClearpay"
    ]
    """
    Afterpay gives your customers a way to pay for purchases in installments, check this [page](https://docs.stripe.com/payments/afterpay-clearpay) for more details like country availability. Afterpay is particularly popular among businesses selling fashion, beauty, and sports products.
    """
    alipay: NotRequired["PaymentMethodConfigurationModifyParamsAlipay"]
    """
    Alipay is a digital wallet in China that has more than a billion active users worldwide. Alipay users can pay on the web or on a mobile device using login credentials or their Alipay app. Alipay has a low dispute rate and reduces fraud by authenticating payments using the customer's login credentials. Check this [page](https://docs.stripe.com/payments/alipay) for more details.
    """
    alma: NotRequired["PaymentMethodConfigurationModifyParamsAlma"]
    """
    Alma is a Buy Now, Pay Later payment method that offers customers the ability to pay in 2, 3, or 4 installments.
    """
    amazon_pay: NotRequired["PaymentMethodConfigurationModifyParamsAmazonPay"]
    """
    Amazon Pay is a wallet payment method that lets your customers check out the same way as on Amazon.
    """
    apple_pay: NotRequired["PaymentMethodConfigurationModifyParamsApplePay"]
    """
    Stripe users can accept [Apple Pay](https://stripe.com/payments/apple-pay) in iOS applications in iOS 9 and later, and on the web in Safari starting with iOS 10 or macOS Sierra. There are no additional fees to process Apple Pay payments, and the [pricing](https://stripe.com/pricing) is the same as other card transactions. Check this [page](https://docs.stripe.com/apple-pay) for more details.
    """
    apple_pay_later: NotRequired[
        "PaymentMethodConfigurationModifyParamsApplePayLater"
    ]
    """
    Apple Pay Later, a payment method for customers to buy now and pay later, gives your customers a way to split purchases into four installments across six weeks.
    """
    au_becs_debit: NotRequired[
        "PaymentMethodConfigurationModifyParamsAuBecsDebit"
    ]
    """
    Stripe users in Australia can accept Bulk Electronic Clearing System (BECS) direct debit payments from customers with an Australian bank account. Check this [page](https://docs.stripe.com/payments/au-becs-debit) for more details.
    """
    bacs_debit: NotRequired["PaymentMethodConfigurationModifyParamsBacsDebit"]
    """
    Stripe users in the UK can accept Bacs Direct Debit payments from customers with a UK bank account, check this [page](https://docs.stripe.com/payments/payment-methods/bacs-debit) for more details.
    """
    bancontact: NotRequired["PaymentMethodConfigurationModifyParamsBancontact"]
    """
    Bancontact is the most popular online payment method in Belgium, with over 15 million cards in circulation. [Customers](https://docs.stripe.com/api/customers) use a Bancontact card or mobile app linked to a Belgian bank account to make online payments that are secure, guaranteed, and confirmed immediately. Check this [page](https://docs.stripe.com/payments/bancontact) for more details.
    """
    billie: NotRequired["PaymentMethodConfigurationModifyParamsBillie"]
    """
    Billie is a [single-use](https://docs.stripe.com/payments/payment-methods#usage) payment method that offers businesses Pay by Invoice where they offer payment terms ranging from 7-120 days. Customers are redirected from your website or app, authorize the payment with Billie, then return to your website or app. You get [immediate notification](https://docs.stripe.com/payments/payment-methods#payment-notification) of whether the payment succeeded or failed.
    """
    blik: NotRequired["PaymentMethodConfigurationModifyParamsBlik"]
    """
    BLIK is a [single use](https://docs.stripe.com/payments/payment-methods#usage) payment method that requires customers to authenticate their payments. When customers want to pay online using BLIK, they request a six-digit code from their banking application and enter it into the payment collection form. Check this [page](https://docs.stripe.com/payments/blik) for more details.
    """
    boleto: NotRequired["PaymentMethodConfigurationModifyParamsBoleto"]
    """
    Boleto is an official (regulated by the Central Bank of Brazil) payment method in Brazil. Check this [page](https://docs.stripe.com/payments/boleto) for more details.
    """
    card: NotRequired["PaymentMethodConfigurationModifyParamsCard"]
    """
    Cards are a popular way for consumers and businesses to pay online or in person. Stripe supports global and local card networks.
    """
    cartes_bancaires: NotRequired[
        "PaymentMethodConfigurationModifyParamsCartesBancaires"
    ]
    """
    Cartes Bancaires is France's local card network. More than 95% of these cards are co-branded with either Visa or Mastercard, meaning you can process these cards over either Cartes Bancaires or the Visa or Mastercard networks. Check this [page](https://docs.stripe.com/payments/cartes-bancaires) for more details.
    """
    cashapp: NotRequired["PaymentMethodConfigurationModifyParamsCashapp"]
    """
    Cash App is a popular consumer app in the US that allows customers to bank, invest, send, and receive money using their digital wallet. Check this [page](https://docs.stripe.com/payments/cash-app-pay) for more details.
    """
    crypto: NotRequired["PaymentMethodConfigurationModifyParamsCrypto"]
    """
    [Stablecoin payments](https://docs.stripe.com/payments/stablecoin-payments) enable customers to pay in stablecoins like USDC from 100s of wallets including Phantom and Metamask.
    """
    customer_balance: NotRequired[
        "PaymentMethodConfigurationModifyParamsCustomerBalance"
    ]
    """
    Uses a customer's [cash balance](https://docs.stripe.com/payments/customer-balance) for the payment. The cash balance can be funded via a bank transfer. Check this [page](https://docs.stripe.com/payments/bank-transfers) for more details.
    """
    eps: NotRequired["PaymentMethodConfigurationModifyParamsEps"]
    """
    EPS is an Austria-based payment method that allows customers to complete transactions online using their bank credentials. EPS is supported by all Austrian banks and is accepted by over 80% of Austrian online retailers. Check this [page](https://docs.stripe.com/payments/eps) for more details.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    fpx: NotRequired["PaymentMethodConfigurationModifyParamsFpx"]
    """
    Financial Process Exchange (FPX) is a Malaysia-based payment method that allows customers to complete transactions online using their bank credentials. Bank Negara Malaysia (BNM), the Central Bank of Malaysia, and eleven other major Malaysian financial institutions are members of the PayNet Group, which owns and operates FPX. It is one of the most popular online payment methods in Malaysia, with nearly 90 million transactions in 2018 according to BNM. Check this [page](https://docs.stripe.com/payments/fpx) for more details.
    """
    fr_meal_voucher_conecs: NotRequired[
        "PaymentMethodConfigurationModifyParamsFrMealVoucherConecs"
    ]
    """
    Meal vouchers in France, or “titres-restaurant”, is a local benefits program commonly offered by employers for their employees to purchase prepared food and beverages on working days. Check this [page](https://docs.stripe.com/payments/meal-vouchers/fr-meal-vouchers) for more details.
    """
    giropay: NotRequired["PaymentMethodConfigurationModifyParamsGiropay"]
    """
    giropay is a German payment method based on online banking, introduced in 2006. It allows customers to complete transactions online using their online banking environment, with funds debited from their bank account. Depending on their bank, customers confirm payments on giropay using a second factor of authentication or a PIN. giropay accounts for 10% of online checkouts in Germany. Check this [page](https://docs.stripe.com/payments/giropay) for more details.
    """
    google_pay: NotRequired["PaymentMethodConfigurationModifyParamsGooglePay"]
    """
    Google Pay allows customers to make payments in your app or website using any credit or debit card saved to their Google Account, including those from Google Play, YouTube, Chrome, or an Android device. Use the Google Pay API to request any credit or debit card stored in your customer's Google account. Check this [page](https://docs.stripe.com/google-pay) for more details.
    """
    grabpay: NotRequired["PaymentMethodConfigurationModifyParamsGrabpay"]
    """
    GrabPay is a payment method developed by [Grab](https://www.grab.com/sg/consumer/finance/pay/). GrabPay is a digital wallet - customers maintain a balance in their wallets that they pay out with. Check this [page](https://docs.stripe.com/payments/grabpay) for more details.
    """
    ideal: NotRequired["PaymentMethodConfigurationModifyParamsIdeal"]
    """
    iDEAL is a Netherlands-based payment method that allows customers to complete transactions online using their bank credentials. All major Dutch banks are members of Currence, the scheme that operates iDEAL, making it the most popular online payment method in the Netherlands with a share of online transactions close to 55%. Check this [page](https://docs.stripe.com/payments/ideal) for more details.
    """
    jcb: NotRequired["PaymentMethodConfigurationModifyParamsJcb"]
    """
    JCB is a credit card company based in Japan. JCB is currently available in Japan to businesses approved by JCB, and available to all businesses in Australia, Canada, Hong Kong, Japan, New Zealand, Singapore, Switzerland, United Kingdom, United States, and all countries in the European Economic Area except Iceland. Check this [page](https://support.stripe.com/questions/accepting-japan-credit-bureau-%28jcb%29-payments) for more details.
    """
    kakao_pay: NotRequired["PaymentMethodConfigurationModifyParamsKakaoPay"]
    """
    Kakao Pay is a popular local wallet available in South Korea.
    """
    klarna: NotRequired["PaymentMethodConfigurationModifyParamsKlarna"]
    """
    Klarna gives customers a range of [payment options](https://docs.stripe.com/payments/klarna#payment-options) during checkout. Available payment options vary depending on the customer's billing address and the transaction amount. These payment options make it convenient for customers to purchase items in all price ranges. Check this [page](https://docs.stripe.com/payments/klarna) for more details.
    """
    konbini: NotRequired["PaymentMethodConfigurationModifyParamsKonbini"]
    """
    Konbini allows customers in Japan to pay for bills and online purchases at convenience stores with cash. Check this [page](https://docs.stripe.com/payments/konbini) for more details.
    """
    kr_card: NotRequired["PaymentMethodConfigurationModifyParamsKrCard"]
    """
    Korean cards let users pay using locally issued cards from South Korea.
    """
    link: NotRequired["PaymentMethodConfigurationModifyParamsLink"]
    """
    [Link](https://docs.stripe.com/payments/link) is a payment method network. With Link, users save their payment details once, then reuse that information to pay with one click for any business on the network.
    """
    mb_way: NotRequired["PaymentMethodConfigurationModifyParamsMbWay"]
    """
    MB WAY is the most popular wallet in Portugal. After entering their phone number in your checkout, customers approve the payment directly in their MB WAY app. Check this [page](https://docs.stripe.com/payments/mb-way) for more details.
    """
    mobilepay: NotRequired["PaymentMethodConfigurationModifyParamsMobilepay"]
    """
    MobilePay is a [single-use](https://docs.stripe.com/payments/payment-methods#usage) card wallet payment method used in Denmark and Finland. It allows customers to [authenticate and approve](https://docs.stripe.com/payments/payment-methods#customer-actions) payments using the MobilePay app. Check this [page](https://docs.stripe.com/payments/mobilepay) for more details.
    """
    multibanco: NotRequired["PaymentMethodConfigurationModifyParamsMultibanco"]
    """
    Stripe users in Europe and the United States can accept Multibanco payments from customers in Portugal using [Sources](https://stripe.com/docs/sources)—a single integration path for creating payments using any supported method.
    """
    name: NotRequired[str]
    """
    Configuration name.
    """
    naver_pay: NotRequired["PaymentMethodConfigurationModifyParamsNaverPay"]
    """
    Naver Pay is a popular local wallet available in South Korea.
    """
    nz_bank_account: NotRequired[
        "PaymentMethodConfigurationModifyParamsNzBankAccount"
    ]
    """
    Stripe users in New Zealand can accept Bulk Electronic Clearing System (BECS) direct debit payments from customers with a New Zeland bank account. Check this [page](https://docs.stripe.com/payments/nz-bank-account) for more details.
    """
    oxxo: NotRequired["PaymentMethodConfigurationModifyParamsOxxo"]
    """
    OXXO is a Mexican chain of convenience stores with thousands of locations across Latin America and represents nearly 20% of online transactions in Mexico. OXXO allows customers to pay bills and online purchases in-store with cash. Check this [page](https://docs.stripe.com/payments/oxxo) for more details.
    """
    p24: NotRequired["PaymentMethodConfigurationModifyParamsP24"]
    """
    Przelewy24 is a Poland-based payment method aggregator that allows customers to complete transactions online using bank transfers and other methods. Bank transfers account for 30% of online payments in Poland and Przelewy24 provides a way for customers to pay with over 165 banks. Check this [page](https://docs.stripe.com/payments/p24) for more details.
    """
    pay_by_bank: NotRequired["PaymentMethodConfigurationModifyParamsPayByBank"]
    """
    Pay by bank is a redirect payment method backed by bank transfers. A customer is redirected to their bank to authorize a bank transfer for a given amount. This removes a lot of the error risks inherent in waiting for the customer to initiate a transfer themselves, and is less expensive than card payments.
    """
    payco: NotRequired["PaymentMethodConfigurationModifyParamsPayco"]
    """
    PAYCO is a [single-use](https://docs.stripe.com/payments/payment-methods#usage local wallet available in South Korea.
    """
    paynow: NotRequired["PaymentMethodConfigurationModifyParamsPaynow"]
    """
    PayNow is a Singapore-based payment method that allows customers to make a payment using their preferred app from participating banks and participating non-bank financial institutions. Check this [page](https://docs.stripe.com/payments/paynow) for more details.
    """
    paypal: NotRequired["PaymentMethodConfigurationModifyParamsPaypal"]
    """
    PayPal, a digital wallet popular with customers in Europe, allows your customers worldwide to pay using their PayPal account. Check this [page](https://docs.stripe.com/payments/paypal) for more details.
    """
    payto: NotRequired["PaymentMethodConfigurationModifyParamsPayto"]
    """
    PayTo is a [real-time](https://docs.stripe.com/payments/real-time) payment method that enables customers in Australia to pay by providing their bank account details. Customers must accept a mandate authorizing you to debit their account. Check this [page](https://docs.stripe.com/payments/payto) for more details.
    """
    pix: NotRequired["PaymentMethodConfigurationModifyParamsPix"]
    """
    Pix is a payment method popular in Brazil. When paying with Pix, customers authenticate and approve payments by scanning a QR code in their preferred banking app. Check this [page](https://docs.stripe.com/payments/pix) for more details.
    """
    promptpay: NotRequired["PaymentMethodConfigurationModifyParamsPromptpay"]
    """
    PromptPay is a Thailand-based payment method that allows customers to make a payment using their preferred app from participating banks. Check this [page](https://docs.stripe.com/payments/promptpay) for more details.
    """
    revolut_pay: NotRequired[
        "PaymentMethodConfigurationModifyParamsRevolutPay"
    ]
    """
    Revolut Pay, developed by Revolut, a global finance app, is a digital wallet payment method. Revolut Pay uses the customer's stored balance or cards to fund the payment, and offers the option for non-Revolut customers to save their details after their first purchase.
    """
    samsung_pay: NotRequired[
        "PaymentMethodConfigurationModifyParamsSamsungPay"
    ]
    """
    Samsung Pay is a [single-use](https://docs.stripe.com/payments/payment-methods#usage local wallet available in South Korea.
    """
    satispay: NotRequired["PaymentMethodConfigurationModifyParamsSatispay"]
    """
    Satispay is a [single-use](https://docs.stripe.com/payments/payment-methods#usage) payment method where customers are required to [authenticate](https://docs.stripe.com/payments/payment-methods#customer-actions) their payment. Customers pay by being redirected from your website or app, authorizing the payment with Satispay, then returning to your website or app. You get [immediate notification](https://docs.stripe.com/payments/payment-methods#payment-notification) of whether the payment succeeded or failed.
    """
    sepa_debit: NotRequired["PaymentMethodConfigurationModifyParamsSepaDebit"]
    """
    The [Single Euro Payments Area (SEPA)](https://en.wikipedia.org/wiki/Single_Euro_Payments_Area) is an initiative of the European Union to simplify payments within and across member countries. SEPA established and enforced banking standards to allow for the direct debiting of every EUR-denominated bank account within the SEPA region, check this [page](https://docs.stripe.com/payments/sepa-debit) for more details.
    """
    sofort: NotRequired["PaymentMethodConfigurationModifyParamsSofort"]
    """
    Stripe users in Europe and the United States can use the [Payment Intents API](https://stripe.com/docs/payments/payment-intents)—a single integration path for creating payments using any supported method—to accept [Sofort](https://www.sofort.com/) payments from customers. Check this [page](https://docs.stripe.com/payments/sofort) for more details.
    """
    swish: NotRequired["PaymentMethodConfigurationModifyParamsSwish"]
    """
    Swish is a [real-time](https://docs.stripe.com/payments/real-time) payment method popular in Sweden. It allows customers to [authenticate and approve](https://docs.stripe.com/payments/payment-methods#customer-actions) payments using the Swish mobile app and the Swedish BankID mobile app. Check this [page](https://docs.stripe.com/payments/swish) for more details.
    """
    twint: NotRequired["PaymentMethodConfigurationModifyParamsTwint"]
    """
    Twint is a payment method popular in Switzerland. It allows customers to pay using their mobile phone. Check this [page](https://docs.stripe.com/payments/twint) for more details.
    """
    us_bank_account: NotRequired[
        "PaymentMethodConfigurationModifyParamsUsBankAccount"
    ]
    """
    Stripe users in the United States can accept ACH direct debit payments from customers with a US bank account using the Automated Clearing House (ACH) payments system operated by Nacha. Check this [page](https://docs.stripe.com/payments/ach-direct-debit) for more details.
    """
    wechat_pay: NotRequired["PaymentMethodConfigurationModifyParamsWechatPay"]
    """
    WeChat, owned by Tencent, is China's leading mobile app with over 1 billion monthly active users. Chinese consumers can use WeChat Pay to pay for goods and services inside of businesses' apps and websites. WeChat Pay users buy most frequently in gaming, e-commerce, travel, online education, and food/nutrition. Check this [page](https://docs.stripe.com/payments/wechat-pay) for more details.
    """
    zip: NotRequired["PaymentMethodConfigurationModifyParamsZip"]
    """
    Zip gives your customers a way to split purchases over a series of payments. Check this [page](https://docs.stripe.com/payments/zip) for more details like country availability.
    """


class PaymentMethodConfigurationModifyParamsAcssDebit(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsAcssDebitDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsAcssDebitDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsAffirm(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsAffirmDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsAffirmDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsAfterpayClearpay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsAfterpayClearpayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsAfterpayClearpayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsAlipay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsAlipayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsAlipayDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsAlma(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsAlmaDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsAlmaDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsAmazonPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsAmazonPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsAmazonPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsApplePay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsApplePayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsApplePayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsApplePayLater(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsApplePayLaterDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsApplePayLaterDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsAuBecsDebit(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsAuBecsDebitDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsAuBecsDebitDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsBacsDebit(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsBacsDebitDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsBacsDebitDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsBancontact(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsBancontactDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsBancontactDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsBillie(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsBillieDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsBillieDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsBlik(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsBlikDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsBlikDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsBoleto(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsBoletoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsBoletoDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsCard(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsCardDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsCardDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsCartesBancaires(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsCartesBancairesDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsCartesBancairesDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsCashapp(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsCashappDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsCashappDisplayPreference(
    TypedDict
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsCrypto(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsCryptoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsCryptoDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsCustomerBalance(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsCustomerBalanceDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsCustomerBalanceDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsEps(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsEpsDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsEpsDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsFpx(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsFpxDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsFpxDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsFrMealVoucherConecs(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsFrMealVoucherConecsDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsFrMealVoucherConecsDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsGiropay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsGiropayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsGiropayDisplayPreference(
    TypedDict
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsGooglePay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsGooglePayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsGooglePayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsGrabpay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsGrabpayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsGrabpayDisplayPreference(
    TypedDict
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsIdeal(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsIdealDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsIdealDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsJcb(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsJcbDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsJcbDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsKakaoPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsKakaoPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsKakaoPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsKlarna(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsKlarnaDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsKlarnaDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsKonbini(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsKonbiniDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsKonbiniDisplayPreference(
    TypedDict
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsKrCard(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsKrCardDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsKrCardDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsLink(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsLinkDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsLinkDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsMbWay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsMbWayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsMbWayDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsMobilepay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsMobilepayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsMobilepayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsMultibanco(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsMultibancoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsMultibancoDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsNaverPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsNaverPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsNaverPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsNzBankAccount(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsNzBankAccountDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsNzBankAccountDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsOxxo(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsOxxoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsOxxoDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsP24(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsP24DisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsP24DisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsPayByBank(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsPayByBankDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsPayByBankDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsPayco(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsPaycoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsPaycoDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsPaynow(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsPaynowDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsPaynowDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsPaypal(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsPaypalDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsPaypalDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsPayto(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsPaytoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsPaytoDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsPix(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsPixDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsPixDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsPromptpay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsPromptpayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsPromptpayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsRevolutPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsRevolutPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsRevolutPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsSamsungPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsSamsungPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsSamsungPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsSatispay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsSatispayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsSatispayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsSepaDebit(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsSepaDebitDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsSepaDebitDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsSofort(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsSofortDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsSofortDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsSwish(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsSwishDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsSwishDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsTwint(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsTwintDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsTwintDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsUsBankAccount(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsUsBankAccountDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsUsBankAccountDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsWechatPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsWechatPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsWechatPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationModifyParamsZip(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationModifyParamsZipDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationModifyParamsZipDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """
