# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from typing import List
from typing_extensions import Literal, NotRequired, TypedDict


class PaymentMethodConfigurationUpdateParams(TypedDict):
    acss_debit: NotRequired["PaymentMethodConfigurationUpdateParamsAcssDebit"]
    """
    Canadian pre-authorized debit payments, check this [page](https://docs.stripe.com/payments/acss-debit) for more details like country availability.
    """
    active: NotRequired[bool]
    """
    Whether the configuration can be used for new payments.
    """
    affirm: NotRequired["PaymentMethodConfigurationUpdateParamsAffirm"]
    """
    [Affirm](https://www.affirm.com/) gives your customers a way to split purchases over a series of payments. Depending on the purchase, they can pay with four interest-free payments (Split Pay) or pay over a longer term (Installments), which might include interest. Check this [page](https://docs.stripe.com/payments/affirm) for more details like country availability.
    """
    afterpay_clearpay: NotRequired[
        "PaymentMethodConfigurationUpdateParamsAfterpayClearpay"
    ]
    """
    Afterpay gives your customers a way to pay for purchases in installments, check this [page](https://docs.stripe.com/payments/afterpay-clearpay) for more details like country availability. Afterpay is particularly popular among businesses selling fashion, beauty, and sports products.
    """
    alipay: NotRequired["PaymentMethodConfigurationUpdateParamsAlipay"]
    """
    Alipay is a digital wallet in China that has more than a billion active users worldwide. Alipay users can pay on the web or on a mobile device using login credentials or their Alipay app. Alipay has a low dispute rate and reduces fraud by authenticating payments using the customer's login credentials. Check this [page](https://docs.stripe.com/payments/alipay) for more details.
    """
    alma: NotRequired["PaymentMethodConfigurationUpdateParamsAlma"]
    """
    Alma is a Buy Now, Pay Later payment method that offers customers the ability to pay in 2, 3, or 4 installments.
    """
    amazon_pay: NotRequired["PaymentMethodConfigurationUpdateParamsAmazonPay"]
    """
    Amazon Pay is a wallet payment method that lets your customers check out the same way as on Amazon.
    """
    apple_pay: NotRequired["PaymentMethodConfigurationUpdateParamsApplePay"]
    """
    Stripe users can accept [Apple Pay](https://stripe.com/payments/apple-pay) in iOS applications in iOS 9 and later, and on the web in Safari starting with iOS 10 or macOS Sierra. There are no additional fees to process Apple Pay payments, and the [pricing](https://stripe.com/pricing) is the same as other card transactions. Check this [page](https://docs.stripe.com/apple-pay) for more details.
    """
    apple_pay_later: NotRequired[
        "PaymentMethodConfigurationUpdateParamsApplePayLater"
    ]
    """
    Apple Pay Later, a payment method for customers to buy now and pay later, gives your customers a way to split purchases into four installments across six weeks.
    """
    au_becs_debit: NotRequired[
        "PaymentMethodConfigurationUpdateParamsAuBecsDebit"
    ]
    """
    Stripe users in Australia can accept Bulk Electronic Clearing System (BECS) direct debit payments from customers with an Australian bank account. Check this [page](https://docs.stripe.com/payments/au-becs-debit) for more details.
    """
    bacs_debit: NotRequired["PaymentMethodConfigurationUpdateParamsBacsDebit"]
    """
    Stripe users in the UK can accept Bacs Direct Debit payments from customers with a UK bank account, check this [page](https://docs.stripe.com/payments/payment-methods/bacs-debit) for more details.
    """
    bancontact: NotRequired["PaymentMethodConfigurationUpdateParamsBancontact"]
    """
    Bancontact is the most popular online payment method in Belgium, with over 15 million cards in circulation. [Customers](https://docs.stripe.com/api/customers) use a Bancontact card or mobile app linked to a Belgian bank account to make online payments that are secure, guaranteed, and confirmed immediately. Check this [page](https://docs.stripe.com/payments/bancontact) for more details.
    """
    billie: NotRequired["PaymentMethodConfigurationUpdateParamsBillie"]
    """
    Billie is a [single-use](https://docs.stripe.com/payments/payment-methods#usage) payment method that offers businesses Pay by Invoice where they offer payment terms ranging from 7-120 days. Customers are redirected from your website or app, authorize the payment with Billie, then return to your website or app. You get [immediate notification](https://docs.stripe.com/payments/payment-methods#payment-notification) of whether the payment succeeded or failed.
    """
    blik: NotRequired["PaymentMethodConfigurationUpdateParamsBlik"]
    """
    BLIK is a [single use](https://docs.stripe.com/payments/payment-methods#usage) payment method that requires customers to authenticate their payments. When customers want to pay online using BLIK, they request a six-digit code from their banking application and enter it into the payment collection form. Check this [page](https://docs.stripe.com/payments/blik) for more details.
    """
    boleto: NotRequired["PaymentMethodConfigurationUpdateParamsBoleto"]
    """
    Boleto is an official (regulated by the Central Bank of Brazil) payment method in Brazil. Check this [page](https://docs.stripe.com/payments/boleto) for more details.
    """
    card: NotRequired["PaymentMethodConfigurationUpdateParamsCard"]
    """
    Cards are a popular way for consumers and businesses to pay online or in person. Stripe supports global and local card networks.
    """
    cartes_bancaires: NotRequired[
        "PaymentMethodConfigurationUpdateParamsCartesBancaires"
    ]
    """
    Cartes Bancaires is France's local card network. More than 95% of these cards are co-branded with either Visa or Mastercard, meaning you can process these cards over either Cartes Bancaires or the Visa or Mastercard networks. Check this [page](https://docs.stripe.com/payments/cartes-bancaires) for more details.
    """
    cashapp: NotRequired["PaymentMethodConfigurationUpdateParamsCashapp"]
    """
    Cash App is a popular consumer app in the US that allows customers to bank, invest, send, and receive money using their digital wallet. Check this [page](https://docs.stripe.com/payments/cash-app-pay) for more details.
    """
    crypto: NotRequired["PaymentMethodConfigurationUpdateParamsCrypto"]
    """
    [Stablecoin payments](https://docs.stripe.com/payments/stablecoin-payments) enable customers to pay in stablecoins like USDC from 100s of wallets including Phantom and Metamask.
    """
    customer_balance: NotRequired[
        "PaymentMethodConfigurationUpdateParamsCustomerBalance"
    ]
    """
    Uses a customer's [cash balance](https://docs.stripe.com/payments/customer-balance) for the payment. The cash balance can be funded via a bank transfer. Check this [page](https://docs.stripe.com/payments/bank-transfers) for more details.
    """
    eps: NotRequired["PaymentMethodConfigurationUpdateParamsEps"]
    """
    EPS is an Austria-based payment method that allows customers to complete transactions online using their bank credentials. EPS is supported by all Austrian banks and is accepted by over 80% of Austrian online retailers. Check this [page](https://docs.stripe.com/payments/eps) for more details.
    """
    expand: NotRequired[List[str]]
    """
    Specifies which fields in the response should be expanded.
    """
    fpx: NotRequired["PaymentMethodConfigurationUpdateParamsFpx"]
    """
    Financial Process Exchange (FPX) is a Malaysia-based payment method that allows customers to complete transactions online using their bank credentials. Bank Negara Malaysia (BNM), the Central Bank of Malaysia, and eleven other major Malaysian financial institutions are members of the PayNet Group, which owns and operates FPX. It is one of the most popular online payment methods in Malaysia, with nearly 90 million transactions in 2018 according to BNM. Check this [page](https://docs.stripe.com/payments/fpx) for more details.
    """
    fr_meal_voucher_conecs: NotRequired[
        "PaymentMethodConfigurationUpdateParamsFrMealVoucherConecs"
    ]
    """
    Meal vouchers in France, or “titres-restaurant”, is a local benefits program commonly offered by employers for their employees to purchase prepared food and beverages on working days. Check this [page](https://docs.stripe.com/payments/meal-vouchers/fr-meal-vouchers) for more details.
    """
    giropay: NotRequired["PaymentMethodConfigurationUpdateParamsGiropay"]
    """
    giropay is a German payment method based on online banking, introduced in 2006. It allows customers to complete transactions online using their online banking environment, with funds debited from their bank account. Depending on their bank, customers confirm payments on giropay using a second factor of authentication or a PIN. giropay accounts for 10% of online checkouts in Germany. Check this [page](https://docs.stripe.com/payments/giropay) for more details.
    """
    google_pay: NotRequired["PaymentMethodConfigurationUpdateParamsGooglePay"]
    """
    Google Pay allows customers to make payments in your app or website using any credit or debit card saved to their Google Account, including those from Google Play, YouTube, Chrome, or an Android device. Use the Google Pay API to request any credit or debit card stored in your customer's Google account. Check this [page](https://docs.stripe.com/google-pay) for more details.
    """
    grabpay: NotRequired["PaymentMethodConfigurationUpdateParamsGrabpay"]
    """
    GrabPay is a payment method developed by [Grab](https://www.grab.com/sg/consumer/finance/pay/). GrabPay is a digital wallet - customers maintain a balance in their wallets that they pay out with. Check this [page](https://docs.stripe.com/payments/grabpay) for more details.
    """
    ideal: NotRequired["PaymentMethodConfigurationUpdateParamsIdeal"]
    """
    iDEAL is a Netherlands-based payment method that allows customers to complete transactions online using their bank credentials. All major Dutch banks are members of Currence, the scheme that operates iDEAL, making it the most popular online payment method in the Netherlands with a share of online transactions close to 55%. Check this [page](https://docs.stripe.com/payments/ideal) for more details.
    """
    jcb: NotRequired["PaymentMethodConfigurationUpdateParamsJcb"]
    """
    JCB is a credit card company based in Japan. JCB is currently available in Japan to businesses approved by JCB, and available to all businesses in Australia, Canada, Hong Kong, Japan, New Zealand, Singapore, Switzerland, United Kingdom, United States, and all countries in the European Economic Area except Iceland. Check this [page](https://support.stripe.com/questions/accepting-japan-credit-bureau-%28jcb%29-payments) for more details.
    """
    kakao_pay: NotRequired["PaymentMethodConfigurationUpdateParamsKakaoPay"]
    """
    Kakao Pay is a popular local wallet available in South Korea.
    """
    klarna: NotRequired["PaymentMethodConfigurationUpdateParamsKlarna"]
    """
    Klarna gives customers a range of [payment options](https://docs.stripe.com/payments/klarna#payment-options) during checkout. Available payment options vary depending on the customer's billing address and the transaction amount. These payment options make it convenient for customers to purchase items in all price ranges. Check this [page](https://docs.stripe.com/payments/klarna) for more details.
    """
    konbini: NotRequired["PaymentMethodConfigurationUpdateParamsKonbini"]
    """
    Konbini allows customers in Japan to pay for bills and online purchases at convenience stores with cash. Check this [page](https://docs.stripe.com/payments/konbini) for more details.
    """
    kr_card: NotRequired["PaymentMethodConfigurationUpdateParamsKrCard"]
    """
    Korean cards let users pay using locally issued cards from South Korea.
    """
    link: NotRequired["PaymentMethodConfigurationUpdateParamsLink"]
    """
    [Link](https://docs.stripe.com/payments/link) is a payment method network. With Link, users save their payment details once, then reuse that information to pay with one click for any business on the network.
    """
    mb_way: NotRequired["PaymentMethodConfigurationUpdateParamsMbWay"]
    """
    MB WAY is the most popular wallet in Portugal. After entering their phone number in your checkout, customers approve the payment directly in their MB WAY app. Check this [page](https://docs.stripe.com/payments/mb-way) for more details.
    """
    mobilepay: NotRequired["PaymentMethodConfigurationUpdateParamsMobilepay"]
    """
    MobilePay is a [single-use](https://docs.stripe.com/payments/payment-methods#usage) card wallet payment method used in Denmark and Finland. It allows customers to [authenticate and approve](https://docs.stripe.com/payments/payment-methods#customer-actions) payments using the MobilePay app. Check this [page](https://docs.stripe.com/payments/mobilepay) for more details.
    """
    multibanco: NotRequired["PaymentMethodConfigurationUpdateParamsMultibanco"]
    """
    Stripe users in Europe and the United States can accept Multibanco payments from customers in Portugal using [Sources](https://stripe.com/docs/sources)—a single integration path for creating payments using any supported method.
    """
    name: NotRequired[str]
    """
    Configuration name.
    """
    naver_pay: NotRequired["PaymentMethodConfigurationUpdateParamsNaverPay"]
    """
    Naver Pay is a popular local wallet available in South Korea.
    """
    nz_bank_account: NotRequired[
        "PaymentMethodConfigurationUpdateParamsNzBankAccount"
    ]
    """
    Stripe users in New Zealand can accept Bulk Electronic Clearing System (BECS) direct debit payments from customers with a New Zeland bank account. Check this [page](https://docs.stripe.com/payments/nz-bank-account) for more details.
    """
    oxxo: NotRequired["PaymentMethodConfigurationUpdateParamsOxxo"]
    """
    OXXO is a Mexican chain of convenience stores with thousands of locations across Latin America and represents nearly 20% of online transactions in Mexico. OXXO allows customers to pay bills and online purchases in-store with cash. Check this [page](https://docs.stripe.com/payments/oxxo) for more details.
    """
    p24: NotRequired["PaymentMethodConfigurationUpdateParamsP24"]
    """
    Przelewy24 is a Poland-based payment method aggregator that allows customers to complete transactions online using bank transfers and other methods. Bank transfers account for 30% of online payments in Poland and Przelewy24 provides a way for customers to pay with over 165 banks. Check this [page](https://docs.stripe.com/payments/p24) for more details.
    """
    pay_by_bank: NotRequired["PaymentMethodConfigurationUpdateParamsPayByBank"]
    """
    Pay by bank is a redirect payment method backed by bank transfers. A customer is redirected to their bank to authorize a bank transfer for a given amount. This removes a lot of the error risks inherent in waiting for the customer to initiate a transfer themselves, and is less expensive than card payments.
    """
    payco: NotRequired["PaymentMethodConfigurationUpdateParamsPayco"]
    """
    PAYCO is a [single-use](https://docs.stripe.com/payments/payment-methods#usage local wallet available in South Korea.
    """
    paynow: NotRequired["PaymentMethodConfigurationUpdateParamsPaynow"]
    """
    PayNow is a Singapore-based payment method that allows customers to make a payment using their preferred app from participating banks and participating non-bank financial institutions. Check this [page](https://docs.stripe.com/payments/paynow) for more details.
    """
    paypal: NotRequired["PaymentMethodConfigurationUpdateParamsPaypal"]
    """
    PayPal, a digital wallet popular with customers in Europe, allows your customers worldwide to pay using their PayPal account. Check this [page](https://docs.stripe.com/payments/paypal) for more details.
    """
    payto: NotRequired["PaymentMethodConfigurationUpdateParamsPayto"]
    """
    PayTo is a [real-time](https://docs.stripe.com/payments/real-time) payment method that enables customers in Australia to pay by providing their bank account details. Customers must accept a mandate authorizing you to debit their account. Check this [page](https://docs.stripe.com/payments/payto) for more details.
    """
    pix: NotRequired["PaymentMethodConfigurationUpdateParamsPix"]
    """
    Pix is a payment method popular in Brazil. When paying with Pix, customers authenticate and approve payments by scanning a QR code in their preferred banking app. Check this [page](https://docs.stripe.com/payments/pix) for more details.
    """
    promptpay: NotRequired["PaymentMethodConfigurationUpdateParamsPromptpay"]
    """
    PromptPay is a Thailand-based payment method that allows customers to make a payment using their preferred app from participating banks. Check this [page](https://docs.stripe.com/payments/promptpay) for more details.
    """
    revolut_pay: NotRequired[
        "PaymentMethodConfigurationUpdateParamsRevolutPay"
    ]
    """
    Revolut Pay, developed by Revolut, a global finance app, is a digital wallet payment method. Revolut Pay uses the customer's stored balance or cards to fund the payment, and offers the option for non-Revolut customers to save their details after their first purchase.
    """
    samsung_pay: NotRequired[
        "PaymentMethodConfigurationUpdateParamsSamsungPay"
    ]
    """
    Samsung Pay is a [single-use](https://docs.stripe.com/payments/payment-methods#usage local wallet available in South Korea.
    """
    satispay: NotRequired["PaymentMethodConfigurationUpdateParamsSatispay"]
    """
    Satispay is a [single-use](https://docs.stripe.com/payments/payment-methods#usage) payment method where customers are required to [authenticate](https://docs.stripe.com/payments/payment-methods#customer-actions) their payment. Customers pay by being redirected from your website or app, authorizing the payment with Satispay, then returning to your website or app. You get [immediate notification](https://docs.stripe.com/payments/payment-methods#payment-notification) of whether the payment succeeded or failed.
    """
    sepa_debit: NotRequired["PaymentMethodConfigurationUpdateParamsSepaDebit"]
    """
    The [Single Euro Payments Area (SEPA)](https://en.wikipedia.org/wiki/Single_Euro_Payments_Area) is an initiative of the European Union to simplify payments within and across member countries. SEPA established and enforced banking standards to allow for the direct debiting of every EUR-denominated bank account within the SEPA region, check this [page](https://docs.stripe.com/payments/sepa-debit) for more details.
    """
    sofort: NotRequired["PaymentMethodConfigurationUpdateParamsSofort"]
    """
    Stripe users in Europe and the United States can use the [Payment Intents API](https://stripe.com/docs/payments/payment-intents)—a single integration path for creating payments using any supported method—to accept [Sofort](https://www.sofort.com/) payments from customers. Check this [page](https://docs.stripe.com/payments/sofort) for more details.
    """
    swish: NotRequired["PaymentMethodConfigurationUpdateParamsSwish"]
    """
    Swish is a [real-time](https://docs.stripe.com/payments/real-time) payment method popular in Sweden. It allows customers to [authenticate and approve](https://docs.stripe.com/payments/payment-methods#customer-actions) payments using the Swish mobile app and the Swedish BankID mobile app. Check this [page](https://docs.stripe.com/payments/swish) for more details.
    """
    twint: NotRequired["PaymentMethodConfigurationUpdateParamsTwint"]
    """
    Twint is a payment method popular in Switzerland. It allows customers to pay using their mobile phone. Check this [page](https://docs.stripe.com/payments/twint) for more details.
    """
    us_bank_account: NotRequired[
        "PaymentMethodConfigurationUpdateParamsUsBankAccount"
    ]
    """
    Stripe users in the United States can accept ACH direct debit payments from customers with a US bank account using the Automated Clearing House (ACH) payments system operated by Nacha. Check this [page](https://docs.stripe.com/payments/ach-direct-debit) for more details.
    """
    wechat_pay: NotRequired["PaymentMethodConfigurationUpdateParamsWechatPay"]
    """
    WeChat, owned by Tencent, is China's leading mobile app with over 1 billion monthly active users. Chinese consumers can use WeChat Pay to pay for goods and services inside of businesses' apps and websites. WeChat Pay users buy most frequently in gaming, e-commerce, travel, online education, and food/nutrition. Check this [page](https://docs.stripe.com/payments/wechat-pay) for more details.
    """
    zip: NotRequired["PaymentMethodConfigurationUpdateParamsZip"]
    """
    Zip gives your customers a way to split purchases over a series of payments. Check this [page](https://docs.stripe.com/payments/zip) for more details like country availability.
    """


class PaymentMethodConfigurationUpdateParamsAcssDebit(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsAcssDebitDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsAcssDebitDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsAffirm(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsAffirmDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsAffirmDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsAfterpayClearpay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsAfterpayClearpayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsAfterpayClearpayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsAlipay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsAlipayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsAlipayDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsAlma(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsAlmaDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsAlmaDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsAmazonPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsAmazonPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsAmazonPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsApplePay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsApplePayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsApplePayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsApplePayLater(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsApplePayLaterDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsApplePayLaterDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsAuBecsDebit(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsAuBecsDebitDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsAuBecsDebitDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsBacsDebit(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsBacsDebitDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsBacsDebitDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsBancontact(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsBancontactDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsBancontactDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsBillie(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsBillieDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsBillieDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsBlik(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsBlikDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsBlikDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsBoleto(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsBoletoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsBoletoDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsCard(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsCardDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsCardDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsCartesBancaires(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsCartesBancairesDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsCartesBancairesDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsCashapp(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsCashappDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsCashappDisplayPreference(
    TypedDict
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsCrypto(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsCryptoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsCryptoDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsCustomerBalance(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsCustomerBalanceDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsCustomerBalanceDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsEps(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsEpsDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsEpsDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsFpx(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsFpxDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsFpxDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsFrMealVoucherConecs(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsFrMealVoucherConecsDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsFrMealVoucherConecsDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsGiropay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsGiropayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsGiropayDisplayPreference(
    TypedDict
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsGooglePay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsGooglePayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsGooglePayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsGrabpay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsGrabpayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsGrabpayDisplayPreference(
    TypedDict
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsIdeal(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsIdealDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsIdealDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsJcb(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsJcbDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsJcbDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsKakaoPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsKakaoPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsKakaoPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsKlarna(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsKlarnaDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsKlarnaDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsKonbini(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsKonbiniDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsKonbiniDisplayPreference(
    TypedDict
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsKrCard(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsKrCardDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsKrCardDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsLink(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsLinkDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsLinkDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsMbWay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsMbWayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsMbWayDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsMobilepay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsMobilepayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsMobilepayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsMultibanco(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsMultibancoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsMultibancoDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsNaverPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsNaverPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsNaverPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsNzBankAccount(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsNzBankAccountDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsNzBankAccountDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsOxxo(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsOxxoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsOxxoDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsP24(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsP24DisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsP24DisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsPayByBank(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsPayByBankDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsPayByBankDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsPayco(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsPaycoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsPaycoDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsPaynow(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsPaynowDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsPaynowDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsPaypal(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsPaypalDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsPaypalDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsPayto(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsPaytoDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsPaytoDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsPix(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsPixDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsPixDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsPromptpay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsPromptpayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsPromptpayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsRevolutPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsRevolutPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsRevolutPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsSamsungPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsSamsungPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsSamsungPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsSatispay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsSatispayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsSatispayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsSepaDebit(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsSepaDebitDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsSepaDebitDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsSofort(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsSofortDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsSofortDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsSwish(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsSwishDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsSwishDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsTwint(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsTwintDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsTwintDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsUsBankAccount(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsUsBankAccountDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsUsBankAccountDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsWechatPay(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsWechatPayDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsWechatPayDisplayPreference(
    TypedDict,
):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """


class PaymentMethodConfigurationUpdateParamsZip(TypedDict):
    display_preference: NotRequired[
        "PaymentMethodConfigurationUpdateParamsZipDisplayPreference"
    ]
    """
    Whether or not the payment method should be displayed.
    """


class PaymentMethodConfigurationUpdateParamsZipDisplayPreference(TypedDict):
    preference: NotRequired[Literal["none", "off", "on"]]
    """
    The account's preference for whether or not to display this payment method.
    """
