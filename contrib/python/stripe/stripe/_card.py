# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._account import Account
from stripe._customer import Customer
from stripe._deletable_api_resource import DeletableAPIResource
from stripe._error import InvalidRequestError
from stripe._expandable_field import ExpandableField
from stripe._stripe_object import StripeObject
from stripe._updateable_api_resource import UpdateableAPIResource
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, Union, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._bank_account import BankAccount
    from stripe.params._card_delete_params import CardDeleteParams


class Card(DeletableAPIResource["Card"], UpdateableAPIResource["Card"]):
    """
    You can store multiple cards on a customer in order to charge the customer
    later. You can also store multiple debit cards on a recipient in order to
    transfer to those cards later.

    Related guide: [Card payments with Sources](https://docs.stripe.com/sources/cards)
    """

    OBJECT_NAME: ClassVar[Literal["card"]] = "card"

    class Networks(StripeObject):
        preferred: Optional[str]
        """
        The preferred network for co-branded cards. Can be `cartes_bancaires`, `mastercard`, `visa` or `invalid_preference` if requested network is not valid for the card.
        """

    account: Optional[ExpandableField["Account"]]
    address_city: Optional[str]
    """
    City/District/Suburb/Town/Village.
    """
    address_country: Optional[str]
    """
    Billing address country, if provided when creating card.
    """
    address_line1: Optional[str]
    """
    Address line 1 (Street address/PO Box/Company name).
    """
    address_line1_check: Optional[str]
    """
    If `address_line1` was provided, results of the check: `pass`, `fail`, `unavailable`, or `unchecked`.
    """
    address_line2: Optional[str]
    """
    Address line 2 (Apartment/Suite/Unit/Building).
    """
    address_state: Optional[str]
    """
    State/County/Province/Region.
    """
    address_zip: Optional[str]
    """
    ZIP or postal code.
    """
    address_zip_check: Optional[str]
    """
    If `address_zip` was provided, results of the check: `pass`, `fail`, `unavailable`, or `unchecked`.
    """
    allow_redisplay: Optional[Literal["always", "limited", "unspecified"]]
    """
    This field indicates whether this payment method can be shown again to its customer in a checkout flow. Stripe products such as Checkout and Elements use this field to determine whether a payment method can be shown as a saved payment method in a checkout flow. The field defaults to “unspecified”.
    """
    available_payout_methods: Optional[List[Literal["instant", "standard"]]]
    """
    A set of available payout methods for this card. Only values from this set should be passed as the `method` when creating a payout.
    """
    brand: str
    """
    Card brand. Can be `American Express`, `Cartes Bancaires`, `Diners Club`, `Discover`, `Eftpos Australia`, `Girocard`, `JCB`, `MasterCard`, `UnionPay`, `Visa`, or `Unknown`.
    """
    country: Optional[str]
    """
    Two-letter ISO code representing the country of the card. You could use this attribute to get a sense of the international breakdown of cards you've collected.
    """
    currency: Optional[str]
    """
    Three-letter [ISO code for currency](https://www.iso.org/iso-4217-currency-codes.html) in lowercase. Must be a [supported currency](https://docs.stripe.com/currencies). Only applicable on accounts (not customers or recipients). The card can be used as a transfer destination for funds in this currency. This property is only available when returned as an [External Account](https://docs.stripe.com/api/external_account_cards/object) where [controller.is_controller](https://docs.stripe.com/api/accounts/object#account_object-controller-is_controller) is `true`.
    """
    customer: Optional[ExpandableField["Customer"]]
    """
    The customer that this card belongs to. This attribute will not be in the card object if the card belongs to an account or recipient instead.
    """
    cvc_check: Optional[str]
    """
    If a CVC was provided, results of the check: `pass`, `fail`, `unavailable`, or `unchecked`. A result of unchecked indicates that CVC was provided but hasn't been checked yet. Checks are typically performed when attaching a card to a Customer object, or when creating a charge. For more details, see [Check if a card is valid without a charge](https://support.stripe.com/questions/check-if-a-card-is-valid-without-a-charge).
    """
    default_for_currency: Optional[bool]
    """
    Whether this card is the default external account for its currency. This property is only available for accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is `application`, which includes Custom accounts.
    """
    deleted: Optional[Literal[True]]
    """
    Always true for a deleted object
    """
    description: Optional[str]
    """
    A high-level description of the type of cards issued in this range. (For internal use only and not typically available in standard API requests.)
    """
    dynamic_last4: Optional[str]
    """
    (For tokenized numbers only.) The last four digits of the device account number.
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
    funding: str
    """
    Card funding type. Can be `credit`, `debit`, `prepaid`, or `unknown`.
    """
    id: str
    """
    Unique identifier for the object.
    """
    iin: Optional[str]
    """
    Issuer identification number of the card. (For internal use only and not typically available in standard API requests.)
    """
    issuer: Optional[str]
    """
    The name of the card's issuing bank. (For internal use only and not typically available in standard API requests.)
    """
    last4: str
    """
    The last four digits of the card.
    """
    metadata: Optional[Dict[str, str]]
    """
    Set of [key-value pairs](https://docs.stripe.com/api/metadata) that you can attach to an object. This can be useful for storing additional information about the object in a structured format.
    """
    name: Optional[str]
    """
    Cardholder name.
    """
    networks: Optional[Networks]
    object: Literal["card"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    regulated_status: Optional[Literal["regulated", "unregulated"]]
    """
    Status of a card based on the card issuer.
    """
    status: Optional[str]
    """
    For external accounts that are cards, possible values are `new` and `errored`. If a payout fails, the status is set to `errored` and [scheduled payouts](https://stripe.com/docs/payouts#payout-schedule) are stopped until account details are updated.
    """
    tokenization_method: Optional[str]
    """
    If the card number is tokenized, this is the method that was used. Can be `android_pay` (includes Google Pay), `apple_pay`, `masterpass`, `visa_checkout`, or null.
    """

    @classmethod
    def _cls_delete(
        cls, sid: str, **params: Unpack["CardDeleteParams"]
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            Union["BankAccount", "Card"],
            cls._static_request(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    def delete(
        sid: str, **params: Unpack["CardDeleteParams"]
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        ...

    @overload
    def delete(
        self, **params: Unpack["CardDeleteParams"]
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        ...

    @class_method_variant("_cls_delete")
    def delete(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["CardDeleteParams"]
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        return self._request_and_refresh(
            "delete",
            self.instance_url(),
            params=params,
        )

    @classmethod
    async def _cls_delete_async(
        cls, sid: str, **params: Unpack["CardDeleteParams"]
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        url = "%s/%s" % (cls.class_url(), sanitize_id(sid))
        return cast(
            Union["BankAccount", "Card"],
            await cls._static_request_async(
                "delete",
                url,
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def delete_async(
        sid: str, **params: Unpack["CardDeleteParams"]
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        ...

    @overload
    async def delete_async(
        self, **params: Unpack["CardDeleteParams"]
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        ...

    @class_method_variant("_cls_delete_async")
    async def delete_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["CardDeleteParams"]
    ) -> Union["BankAccount", "Card"]:
        """
        Delete a specified external account for a given account.
        """
        return await self._request_and_refresh_async(
            "delete",
            self.instance_url(),
            params=params,
        )

    def instance_url(self):
        token = self.id
        extn = sanitize_id(token)
        if hasattr(self, "customer"):
            customer = self.customer

            base = Customer.class_url()
            assert customer is not None
            if isinstance(customer, Customer):
                customer = customer.id
            owner_extn = sanitize_id(customer)
            class_base = "sources"

        elif hasattr(self, "account"):
            account = self.account

            base = Account.class_url()
            assert account is not None
            if isinstance(account, Account):
                account = account.id
            owner_extn = sanitize_id(account)
            class_base = "external_accounts"

        else:
            raise InvalidRequestError(
                "Could not determine whether card_id %s is "
                "attached to a customer, or "
                "account." % token,
                "id",
            )

        return "%s/%s/%s/%s" % (base, owner_extn, class_base, extn)

    @classmethod
    def modify(cls, sid, **params):
        raise NotImplementedError(
            "Can't modify a card without a customer or account ID. "
            "Use stripe.Customer.modify_source('customer_id', 'card_id', ...) "
            "(see https://stripe.com/docs/api/cards/update) or "
            "stripe.Account.modify_external_account('account_id', 'card_id', ...) "
            "(see https://stripe.com/docs/api/external_account_cards/update)."
        )

    @classmethod
    def retrieve(cls, id, **params):
        raise NotImplementedError(
            "Can't retrieve a card without a customer or account ID. "
            "Use stripe.Customer.retrieve_source('customer_id', 'card_id') "
            "(see https://stripe.com/docs/api/cards/retrieve) or "
            "stripe.Account.retrieve_external_account('account_id', 'card_id') "
            "(see https://stripe.com/docs/api/external_account_cards/retrieve)."
        )

    _inner_class_types = {"networks": Networks}
