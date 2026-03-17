# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._createable_api_resource import CreateableAPIResource
from typing import ClassVar, Optional, cast
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._bank_account import BankAccount
    from stripe._card import Card
    from stripe.params._token_create_params import TokenCreateParams
    from stripe.params._token_retrieve_params import TokenRetrieveParams


class Token(CreateableAPIResource["Token"]):
    """
    Tokenization is the process Stripe uses to collect sensitive card or bank
    account details, or personally identifiable information (PII), directly from
    your customers in a secure manner. A token representing this information is
    returned to your server to use. Use our
    [recommended payments integrations](https://docs.stripe.com/payments) to perform this process
    on the client-side. This guarantees that no sensitive card data touches your server,
    and allows your integration to operate in a PCI-compliant way.

    If you can't use client-side tokenization, you can also create tokens using
    the API with either your publishable or secret API key. If
    your integration uses this method, you're responsible for any PCI compliance
    that it might require, and you must keep your secret API key safe. Unlike with
    client-side tokenization, your customer's information isn't sent directly to
    Stripe, so we can't determine how it's handled or stored.

    You can't store or use tokens more than once. To store card or bank account
    information for later use, create [Customer](https://docs.stripe.com/api#customers)
    objects or [External accounts](https://docs.stripe.com/api#external_accounts).
    [Radar](https://docs.stripe.com/radar), our integrated solution for automatic fraud protection,
    performs best with integrations that use client-side tokenization.
    """

    OBJECT_NAME: ClassVar[Literal["token"]] = "token"
    bank_account: Optional["BankAccount"]
    """
    These bank accounts are payment methods on `Customer` objects.

    On the other hand [External Accounts](https://docs.stripe.com/api#external_accounts) are transfer
    destinations on `Account` objects for connected accounts.
    They can be bank accounts or debit cards as well, and are documented in the links above.

    Related guide: [Bank debits and transfers](https://docs.stripe.com/payments/bank-debits-transfers)
    """
    card: Optional["Card"]
    """
    You can store multiple cards on a customer in order to charge the customer
    later. You can also store multiple debit cards on a recipient in order to
    transfer to those cards later.

    Related guide: [Card payments with Sources](https://docs.stripe.com/sources/cards)
    """
    client_ip: Optional[str]
    """
    IP address of the client that generates the token.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    id: str
    """
    Unique identifier for the object.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["token"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    type: str
    """
    Type of the token: `account`, `bank_account`, `card`, or `pii`.
    """
    used: bool
    """
    Determines if you have already used this token (you can only use tokens once).
    """

    @classmethod
    def create(cls, **params: Unpack["TokenCreateParams"]) -> "Token":
        """
        Creates a single-use token that represents a bank account's details.
        You can use this token with any v1 API method in place of a bank account dictionary. You can only use this token once. To do so, attach it to a [connected account](https://docs.stripe.com/api#accounts) where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is application, which includes Custom accounts.
        """
        return cast(
            "Token",
            cls._static_request(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    async def create_async(
        cls, **params: Unpack["TokenCreateParams"]
    ) -> "Token":
        """
        Creates a single-use token that represents a bank account's details.
        You can use this token with any v1 API method in place of a bank account dictionary. You can only use this token once. To do so, attach it to a [connected account](https://docs.stripe.com/api#accounts) where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection) is application, which includes Custom accounts.
        """
        return cast(
            "Token",
            await cls._static_request_async(
                "post",
                cls.class_url(),
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["TokenRetrieveParams"]
    ) -> "Token":
        """
        Retrieves the token with the given ID.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["TokenRetrieveParams"]
    ) -> "Token":
        """
        Retrieves the token with the given ID.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance
