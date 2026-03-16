# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import sanitize_id
from typing import ClassVar, Optional, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe.params._payment_method_configuration_create_params import (
        PaymentMethodConfigurationCreateParams,
    )
    from stripe.params._payment_method_configuration_list_params import (
        PaymentMethodConfigurationListParams,
    )
    from stripe.params._payment_method_configuration_modify_params import (
        PaymentMethodConfigurationModifyParams,
    )
    from stripe.params._payment_method_configuration_retrieve_params import (
        PaymentMethodConfigurationRetrieveParams,
    )


class PaymentMethodConfiguration(
    CreateableAPIResource["PaymentMethodConfiguration"],
    ListableAPIResource["PaymentMethodConfiguration"],
    UpdateableAPIResource["PaymentMethodConfiguration"],
):
    """
    PaymentMethodConfigurations control which payment methods are displayed to your customers when you don't explicitly specify payment method types. You can have multiple configurations with different sets of payment methods for different scenarios.

    There are two types of PaymentMethodConfigurations. Which is used depends on the [charge type](https://docs.stripe.com/connect/charges):

    **Direct** configurations apply to payments created on your account, including Connect destination charges, Connect separate charges and transfers, and payments not involving Connect.

    **Child** configurations apply to payments created on your connected accounts using direct charges, and charges with the on_behalf_of parameter.

    Child configurations have a `parent` that sets default values and controls which settings connected accounts may override. You can specify a parent ID at payment time, and Stripe will automatically resolve the connected account's associated child configuration. Parent configurations are [managed in the dashboard](https://dashboard.stripe.com/settings/payment_methods/connected_accounts) and are not available in this API.

    Related guides:
    - [Payment Method Configurations API](https://docs.stripe.com/connect/payment-method-configurations)
    - [Multiple configurations on dynamic payment methods](https://docs.stripe.com/payments/multiple-payment-method-configs)
    - [Multiple configurations for your Connect accounts](https://docs.stripe.com/connect/multiple-payment-method-configurations)
    """

    OBJECT_NAME: ClassVar[Literal["payment_method_configuration"]] = (
        "payment_method_configuration"
    )

    class AcssDebit(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Affirm(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class AfterpayClearpay(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Alipay(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Alma(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class AmazonPay(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class ApplePay(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class AuBecsDebit(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class BacsDebit(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Bancontact(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Billie(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Blik(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Boleto(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Card(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class CartesBancaires(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Cashapp(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Crypto(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class CustomerBalance(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Eps(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Fpx(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Giropay(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class GooglePay(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Grabpay(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Ideal(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Jcb(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class KakaoPay(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Klarna(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Konbini(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class KrCard(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Link(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class MbWay(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Mobilepay(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Multibanco(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class NaverPay(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class NzBankAccount(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Oxxo(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class P24(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class PayByBank(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Payco(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Paynow(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Paypal(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Payto(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Pix(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Promptpay(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class RevolutPay(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class SamsungPay(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Satispay(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class SepaDebit(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Sofort(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Swish(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Twint(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class UsBankAccount(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class WechatPay(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    class Zip(StripeObject):
        class DisplayPreference(StripeObject):
            overridable: Optional[bool]
            """
            For child configs, whether or not the account's preference will be observed. If `false`, the parent configuration's default is used.
            """
            preference: Literal["none", "off", "on"]
            """
            The account's display preference.
            """
            value: Literal["off", "on"]
            """
            The effective display preference value.
            """

        available: bool
        """
        Whether this payment method may be offered at checkout. True if `display_preference` is `on` and the payment method's capability is active.
        """
        display_preference: DisplayPreference
        _inner_class_types = {"display_preference": DisplayPreference}

    acss_debit: Optional[AcssDebit]
    active: bool
    """
    Whether the configuration can be used for new payments.
    """
    affirm: Optional[Affirm]
    afterpay_clearpay: Optional[AfterpayClearpay]
    alipay: Optional[Alipay]
    alma: Optional[Alma]
    amazon_pay: Optional[AmazonPay]
    apple_pay: Optional[ApplePay]
    application: Optional[str]
    """
    For child configs, the Connect application associated with the configuration.
    """
    au_becs_debit: Optional[AuBecsDebit]
    bacs_debit: Optional[BacsDebit]
    bancontact: Optional[Bancontact]
    billie: Optional[Billie]
    blik: Optional[Blik]
    boleto: Optional[Boleto]
    card: Optional[Card]
    cartes_bancaires: Optional[CartesBancaires]
    cashapp: Optional[Cashapp]
    crypto: Optional[Crypto]
    customer_balance: Optional[CustomerBalance]
    eps: Optional[Eps]
    fpx: Optional[Fpx]
    giropay: Optional[Giropay]
    google_pay: Optional[GooglePay]
    grabpay: Optional[Grabpay]
    id: str
    """
    Unique identifier for the object.
    """
    ideal: Optional[Ideal]
    is_default: bool
    """
    The default configuration is used whenever a payment method configuration is not specified.
    """
    jcb: Optional[Jcb]
    kakao_pay: Optional[KakaoPay]
    klarna: Optional[Klarna]
    konbini: Optional[Konbini]
    kr_card: Optional[KrCard]
    link: Optional[Link]
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    mb_way: Optional[MbWay]
    mobilepay: Optional[Mobilepay]
    multibanco: Optional[Multibanco]
    name: str
    """
    The configuration's name.
    """
    naver_pay: Optional[NaverPay]
    nz_bank_account: Optional[NzBankAccount]
    object: Literal["payment_method_configuration"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    oxxo: Optional[Oxxo]
    p24: Optional[P24]
    parent: Optional[str]
    """
    For child configs, the configuration's parent configuration.
    """
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
    sepa_debit: Optional[SepaDebit]
    sofort: Optional[Sofort]
    swish: Optional[Swish]
    twint: Optional[Twint]
    us_bank_account: Optional[UsBankAccount]
    wechat_pay: Optional[WechatPay]
    zip: Optional[Zip]

    @classmethod
    def create(
        cls, **params: Unpack["PaymentMethodConfigurationCreateParams"]
    ) -> "PaymentMethodConfiguration":
        """
        Creates a payment method configuration
        """
        return cast(
            "PaymentMethodConfiguration",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["PaymentMethodConfigurationCreateParams"]
    ) -> "PaymentMethodConfiguration":
        """
        Creates a payment method configuration
        """
        return cast(
            "PaymentMethodConfiguration",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["PaymentMethodConfigurationListParams"]
    ) -> ListObject["PaymentMethodConfiguration"]:
        """
        List payment method configurations
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
        cls, **params: Unpack["PaymentMethodConfigurationListParams"]
    ) -> ListObject["PaymentMethodConfiguration"]:
        """
        List payment method configurations
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
        cls,
        id: str,
        **params: Unpack["PaymentMethodConfigurationModifyParams"],
    ) -> "PaymentMethodConfiguration":
        """
        Update payment method configuration
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "PaymentMethodConfiguration",
            cls._static_request(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    async def modify_async(
        cls,
        id: str,
        **params: Unpack["PaymentMethodConfigurationModifyParams"],
    ) -> "PaymentMethodConfiguration":
        """
        Update payment method configuration
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(id))
        return cast(
            "PaymentMethodConfiguration",
            await cls._static_request_async(
                "post",
                url,
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls,
        id: str,
        **params: Unpack["PaymentMethodConfigurationRetrieveParams"],
    ) -> "PaymentMethodConfiguration":
        """
        Retrieve payment method configuration
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls,
        id: str,
        **params: Unpack["PaymentMethodConfigurationRetrieveParams"],
    ) -> "PaymentMethodConfiguration":
        """
        Retrieve payment method configuration
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    _inner_class_types = {
        "acss_debit": AcssDebit,
        "affirm": Affirm,
        "afterpay_clearpay": AfterpayClearpay,
        "alipay": Alipay,
        "alma": Alma,
        "amazon_pay": AmazonPay,
        "apple_pay": ApplePay,
        "au_becs_debit": AuBecsDebit,
        "bacs_debit": BacsDebit,
        "bancontact": Bancontact,
        "billie": Billie,
        "blik": Blik,
        "boleto": Boleto,
        "card": Card,
        "cartes_bancaires": CartesBancaires,
        "cashapp": Cashapp,
        "crypto": Crypto,
        "customer_balance": CustomerBalance,
        "eps": Eps,
        "fpx": Fpx,
        "giropay": Giropay,
        "google_pay": GooglePay,
        "grabpay": Grabpay,
        "ideal": Ideal,
        "jcb": Jcb,
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
        "sepa_debit": SepaDebit,
        "sofort": Sofort,
        "swish": Swish,
        "twint": Twint,
        "us_bank_account": UsBankAccount,
        "wechat_pay": WechatPay,
        "zip": Zip,
    }
