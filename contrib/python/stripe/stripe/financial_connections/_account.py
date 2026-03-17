# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._expandable_field import ExpandableField
from stripe._list_object import ListObject
from stripe._listable_api_resource import ListableAPIResource
from stripe._stripe_object import StripeObject
from stripe._util import class_method_variant, sanitize_id
from typing import ClassVar, Dict, List, Optional, cast, overload
from typing_extensions import Literal, Unpack, TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._account import Account as AccountResource
    from stripe._customer import Customer
    from stripe.financial_connections._account_owner import AccountOwner
    from stripe.financial_connections._account_ownership import (
        AccountOwnership,
    )
    from stripe.params.financial_connections._account_disconnect_params import (
        AccountDisconnectParams,
    )
    from stripe.params.financial_connections._account_list_owners_params import (
        AccountListOwnersParams,
    )
    from stripe.params.financial_connections._account_list_params import (
        AccountListParams,
    )
    from stripe.params.financial_connections._account_refresh_account_params import (
        AccountRefreshAccountParams,
    )
    from stripe.params.financial_connections._account_retrieve_params import (
        AccountRetrieveParams,
    )
    from stripe.params.financial_connections._account_subscribe_params import (
        AccountSubscribeParams,
    )
    from stripe.params.financial_connections._account_unsubscribe_params import (
        AccountUnsubscribeParams,
    )


class Account(ListableAPIResource["Account"]):
    """
    A Financial Connections Account represents an account that exists outside of Stripe, to which you have been granted some degree of access.
    """

    OBJECT_NAME: ClassVar[Literal["financial_connections.account"]] = (
        "financial_connections.account"
    )

    class AccountHolder(StripeObject):
        account: Optional[ExpandableField["AccountResource"]]
        """
        The ID of the Stripe account that this account belongs to. Only available when `account_holder.type` is `account`.
        """
        customer: Optional[ExpandableField["Customer"]]
        """
        The ID for an Account representing a customer that this account belongs to. Only available when `account_holder.type` is `customer`.
        """
        customer_account: Optional[str]
        type: Literal["account", "customer"]
        """
        Type of account holder that this account belongs to.
        """

    class AccountNumber(StripeObject):
        expected_expiry_date: Optional[int]
        """
        When the account number is expected to expire, if applicable.
        """
        identifier_type: Literal["account_number", "tokenized_account_number"]
        """
        The type of account number associated with the account.
        """
        status: Literal["deactivated", "transactable"]
        """
        Whether the account number is currently active and usable for transactions.
        """
        supported_networks: List[Literal["ach"]]
        """
        The payment networks that the account number can be used for.
        """

    class Balance(StripeObject):
        class Cash(StripeObject):
            available: Optional[Dict[str, int]]
            """
            The funds available to the account holder. Typically this is the current balance after subtracting any outbound pending transactions and adding any inbound pending transactions.

            Each key is a three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase.

            Each value is a integer amount. A positive amount indicates money owed to the account holder. A negative amount indicates money owed by the account holder.
            """

        class Credit(StripeObject):
            used: Optional[Dict[str, int]]
            """
            The credit that has been used by the account holder.

            Each key is a three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase.

            Each value is a integer amount. A positive amount indicates money owed to the account holder. A negative amount indicates money owed by the account holder.
            """

        as_of: int
        """
        The time that the external institution calculated this balance. Measured in seconds since the Unix epoch.
        """
        cash: Optional[Cash]
        credit: Optional[Credit]
        current: Dict[str, int]
        """
        The balances owed to (or by) the account holder, before subtracting any outbound pending transactions or adding any inbound pending transactions.

        Each key is a three-letter [ISO currency code](https://www.iso.org/iso-4217-currency-codes.html), in lowercase.

        Each value is a integer amount. A positive amount indicates money owed to the account holder. A negative amount indicates money owed by the account holder.
        """
        type: Literal["cash", "credit"]
        """
        The `type` of the balance. An additional hash is included on the balance with a name matching this value.
        """
        _inner_class_types = {"cash": Cash, "credit": Credit}

    class BalanceRefresh(StripeObject):
        last_attempted_at: int
        """
        The time at which the last refresh attempt was initiated. Measured in seconds since the Unix epoch.
        """
        next_refresh_available_at: Optional[int]
        """
        Time at which the next balance refresh can be initiated. This value will be `null` when `status` is `pending`. Measured in seconds since the Unix epoch.
        """
        status: Literal["failed", "pending", "succeeded"]
        """
        The status of the last refresh attempt.
        """

    class OwnershipRefresh(StripeObject):
        last_attempted_at: int
        """
        The time at which the last refresh attempt was initiated. Measured in seconds since the Unix epoch.
        """
        next_refresh_available_at: Optional[int]
        """
        Time at which the next ownership refresh can be initiated. This value will be `null` when `status` is `pending`. Measured in seconds since the Unix epoch.
        """
        status: Literal["failed", "pending", "succeeded"]
        """
        The status of the last refresh attempt.
        """

    class TransactionRefresh(StripeObject):
        id: str
        """
        Unique identifier for the object.
        """
        last_attempted_at: int
        """
        The time at which the last refresh attempt was initiated. Measured in seconds since the Unix epoch.
        """
        next_refresh_available_at: Optional[int]
        """
        Time at which the next transaction refresh can be initiated. This value will be `null` when `status` is `pending`. Measured in seconds since the Unix epoch.
        """
        status: Literal["failed", "pending", "succeeded"]
        """
        The status of the last refresh attempt.
        """

    account_holder: Optional[AccountHolder]
    """
    The account holder that this account belongs to.
    """
    account_numbers: Optional[List[AccountNumber]]
    """
    Details about the account numbers.
    """
    balance: Optional[Balance]
    """
    The most recent information about the account's balance.
    """
    balance_refresh: Optional[BalanceRefresh]
    """
    The state of the most recent attempt to refresh the account balance.
    """
    category: Literal["cash", "credit", "investment", "other"]
    """
    The type of the account. Account category is further divided in `subcategory`.
    """
    created: int
    """
    Time at which the object was created. Measured in seconds since the Unix epoch.
    """
    display_name: Optional[str]
    """
    A human-readable name that has been assigned to this account, either by the account holder or by the institution.
    """
    id: str
    """
    Unique identifier for the object.
    """
    institution_name: str
    """
    The name of the institution that holds this account.
    """
    last4: Optional[str]
    """
    The last 4 digits of the account number. If present, this will be 4 numeric characters.
    """
    livemode: bool
    """
    Has the value `true` if the object exists in live mode or the value `false` if the object exists in test mode.
    """
    object: Literal["financial_connections.account"]
    """
    String representing the object's type. Objects of the same type share the same value.
    """
    ownership: Optional[ExpandableField["AccountOwnership"]]
    """
    The most recent information about the account's owners.
    """
    ownership_refresh: Optional[OwnershipRefresh]
    """
    The state of the most recent attempt to refresh the account owners.
    """
    permissions: Optional[
        List[
            Literal["balances", "ownership", "payment_method", "transactions"]
        ]
    ]
    """
    The list of permissions granted by this account.
    """
    status: Literal["active", "disconnected", "inactive"]
    """
    The status of the link to the account.
    """
    subcategory: Literal[
        "checking",
        "credit_card",
        "line_of_credit",
        "mortgage",
        "other",
        "savings",
    ]
    """
    If `category` is `cash`, one of:

     - `checking`
     - `savings`
     - `other`

    If `category` is `credit`, one of:

     - `mortgage`
     - `line_of_credit`
     - `credit_card`
     - `other`

    If `category` is `investment` or `other`, this will be `other`.
    """
    subscriptions: Optional[List[Literal["transactions"]]]
    """
    The list of data refresh subscriptions requested on this account.
    """
    supported_payment_method_types: List[Literal["link", "us_bank_account"]]
    """
    The [PaymentMethod type](https://docs.stripe.com/api/payment_methods/object#payment_method_object-type)(s) that can be created from this account.
    """
    transaction_refresh: Optional[TransactionRefresh]
    """
    The state of the most recent attempt to refresh the account transactions.
    """

    @classmethod
    def _cls_disconnect(
        cls, account: str, **params: Unpack["AccountDisconnectParams"]
    ) -> "Account":
        """
        Disables your access to a Financial Connections Account. You will no longer be able to access data associated with the account (e.g. balances, transactions).
        """
        return cast(
            "Account",
            cls._static_request(
                "post",
                "/v1/financial_connections/accounts/{account}/disconnect".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def disconnect(
        account: str, **params: Unpack["AccountDisconnectParams"]
    ) -> "Account":
        """
        Disables your access to a Financial Connections Account. You will no longer be able to access data associated with the account (e.g. balances, transactions).
        """
        ...

    @overload
    def disconnect(
        self, **params: Unpack["AccountDisconnectParams"]
    ) -> "Account":
        """
        Disables your access to a Financial Connections Account. You will no longer be able to access data associated with the account (e.g. balances, transactions).
        """
        ...

    @class_method_variant("_cls_disconnect")
    def disconnect(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AccountDisconnectParams"]
    ) -> "Account":
        """
        Disables your access to a Financial Connections Account. You will no longer be able to access data associated with the account (e.g. balances, transactions).
        """
        return cast(
            "Account",
            self._request(
                "post",
                "/v1/financial_connections/accounts/{account}/disconnect".format(
                    account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_disconnect_async(
        cls, account: str, **params: Unpack["AccountDisconnectParams"]
    ) -> "Account":
        """
        Disables your access to a Financial Connections Account. You will no longer be able to access data associated with the account (e.g. balances, transactions).
        """
        return cast(
            "Account",
            await cls._static_request_async(
                "post",
                "/v1/financial_connections/accounts/{account}/disconnect".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def disconnect_async(
        account: str, **params: Unpack["AccountDisconnectParams"]
    ) -> "Account":
        """
        Disables your access to a Financial Connections Account. You will no longer be able to access data associated with the account (e.g. balances, transactions).
        """
        ...

    @overload
    async def disconnect_async(
        self, **params: Unpack["AccountDisconnectParams"]
    ) -> "Account":
        """
        Disables your access to a Financial Connections Account. You will no longer be able to access data associated with the account (e.g. balances, transactions).
        """
        ...

    @class_method_variant("_cls_disconnect_async")
    async def disconnect_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AccountDisconnectParams"]
    ) -> "Account":
        """
        Disables your access to a Financial Connections Account. You will no longer be able to access data associated with the account (e.g. balances, transactions).
        """
        return cast(
            "Account",
            await self._request_async(
                "post",
                "/v1/financial_connections/accounts/{account}/disconnect".format(
                    account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def list(
        cls, **params: Unpack["AccountListParams"]
    ) -> ListObject["Account"]:
        """
        Returns a list of Financial Connections Account objects.
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
        cls, **params: Unpack["AccountListParams"]
    ) -> ListObject["Account"]:
        """
        Returns a list of Financial Connections Account objects.
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
    def _cls_list_owners(
        cls, account: str, **params: Unpack["AccountListOwnersParams"]
    ) -> ListObject["AccountOwner"]:
        """
        Lists all owners for a given Account
        """
        return cast(
            ListObject["AccountOwner"],
            cls._static_request(
                "get",
                "/v1/financial_connections/accounts/{account}/owners".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def list_owners(
        account: str, **params: Unpack["AccountListOwnersParams"]
    ) -> ListObject["AccountOwner"]:
        """
        Lists all owners for a given Account
        """
        ...

    @overload
    def list_owners(
        self, **params: Unpack["AccountListOwnersParams"]
    ) -> ListObject["AccountOwner"]:
        """
        Lists all owners for a given Account
        """
        ...

    @class_method_variant("_cls_list_owners")
    def list_owners(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AccountListOwnersParams"]
    ) -> ListObject["AccountOwner"]:
        """
        Lists all owners for a given Account
        """
        return cast(
            ListObject["AccountOwner"],
            self._request(
                "get",
                "/v1/financial_connections/accounts/{account}/owners".format(
                    account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_list_owners_async(
        cls, account: str, **params: Unpack["AccountListOwnersParams"]
    ) -> ListObject["AccountOwner"]:
        """
        Lists all owners for a given Account
        """
        return cast(
            ListObject["AccountOwner"],
            await cls._static_request_async(
                "get",
                "/v1/financial_connections/accounts/{account}/owners".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def list_owners_async(
        account: str, **params: Unpack["AccountListOwnersParams"]
    ) -> ListObject["AccountOwner"]:
        """
        Lists all owners for a given Account
        """
        ...

    @overload
    async def list_owners_async(
        self, **params: Unpack["AccountListOwnersParams"]
    ) -> ListObject["AccountOwner"]:
        """
        Lists all owners for a given Account
        """
        ...

    @class_method_variant("_cls_list_owners_async")
    async def list_owners_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AccountListOwnersParams"]
    ) -> ListObject["AccountOwner"]:
        """
        Lists all owners for a given Account
        """
        return cast(
            ListObject["AccountOwner"],
            await self._request_async(
                "get",
                "/v1/financial_connections/accounts/{account}/owners".format(
                    account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_refresh_account(
        cls, account: str, **params: Unpack["AccountRefreshAccountParams"]
    ) -> "Account":
        """
        Refreshes the data associated with a Financial Connections Account.
        """
        return cast(
            "Account",
            cls._static_request(
                "post",
                "/v1/financial_connections/accounts/{account}/refresh".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def refresh_account(
        account: str, **params: Unpack["AccountRefreshAccountParams"]
    ) -> "Account":
        """
        Refreshes the data associated with a Financial Connections Account.
        """
        ...

    @overload
    def refresh_account(
        self, **params: Unpack["AccountRefreshAccountParams"]
    ) -> "Account":
        """
        Refreshes the data associated with a Financial Connections Account.
        """
        ...

    @class_method_variant("_cls_refresh_account")
    def refresh_account(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AccountRefreshAccountParams"]
    ) -> "Account":
        """
        Refreshes the data associated with a Financial Connections Account.
        """
        return cast(
            "Account",
            self._request(
                "post",
                "/v1/financial_connections/accounts/{account}/refresh".format(
                    account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_refresh_account_async(
        cls, account: str, **params: Unpack["AccountRefreshAccountParams"]
    ) -> "Account":
        """
        Refreshes the data associated with a Financial Connections Account.
        """
        return cast(
            "Account",
            await cls._static_request_async(
                "post",
                "/v1/financial_connections/accounts/{account}/refresh".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def refresh_account_async(
        account: str, **params: Unpack["AccountRefreshAccountParams"]
    ) -> "Account":
        """
        Refreshes the data associated with a Financial Connections Account.
        """
        ...

    @overload
    async def refresh_account_async(
        self, **params: Unpack["AccountRefreshAccountParams"]
    ) -> "Account":
        """
        Refreshes the data associated with a Financial Connections Account.
        """
        ...

    @class_method_variant("_cls_refresh_account_async")
    async def refresh_account_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AccountRefreshAccountParams"]
    ) -> "Account":
        """
        Refreshes the data associated with a Financial Connections Account.
        """
        return cast(
            "Account",
            await self._request_async(
                "post",
                "/v1/financial_connections/accounts/{account}/refresh".format(
                    account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def retrieve(
        cls, id: str, **params: Unpack["AccountRetrieveParams"]
    ) -> "Account":
        """
        Retrieves the details of an Financial Connections Account.
        """
        instance = cls(id, **params)
        instance.refresh()
        return instance

    @classmethod
    async def retrieve_async(
        cls, id: str, **params: Unpack["AccountRetrieveParams"]
    ) -> "Account":
        """
        Retrieves the details of an Financial Connections Account.
        """
        instance = cls(id, **params)
        await instance.refresh_async()
        return instance

    @classmethod
    def _cls_subscribe(
        cls, account: str, **params: Unpack["AccountSubscribeParams"]
    ) -> "Account":
        """
        Subscribes to periodic refreshes of data associated with a Financial Connections Account. When the account status is active, data is typically refreshed once a day.
        """
        return cast(
            "Account",
            cls._static_request(
                "post",
                "/v1/financial_connections/accounts/{account}/subscribe".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def subscribe(
        account: str, **params: Unpack["AccountSubscribeParams"]
    ) -> "Account":
        """
        Subscribes to periodic refreshes of data associated with a Financial Connections Account. When the account status is active, data is typically refreshed once a day.
        """
        ...

    @overload
    def subscribe(
        self, **params: Unpack["AccountSubscribeParams"]
    ) -> "Account":
        """
        Subscribes to periodic refreshes of data associated with a Financial Connections Account. When the account status is active, data is typically refreshed once a day.
        """
        ...

    @class_method_variant("_cls_subscribe")
    def subscribe(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AccountSubscribeParams"]
    ) -> "Account":
        """
        Subscribes to periodic refreshes of data associated with a Financial Connections Account. When the account status is active, data is typically refreshed once a day.
        """
        return cast(
            "Account",
            self._request(
                "post",
                "/v1/financial_connections/accounts/{account}/subscribe".format(
                    account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_subscribe_async(
        cls, account: str, **params: Unpack["AccountSubscribeParams"]
    ) -> "Account":
        """
        Subscribes to periodic refreshes of data associated with a Financial Connections Account. When the account status is active, data is typically refreshed once a day.
        """
        return cast(
            "Account",
            await cls._static_request_async(
                "post",
                "/v1/financial_connections/accounts/{account}/subscribe".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def subscribe_async(
        account: str, **params: Unpack["AccountSubscribeParams"]
    ) -> "Account":
        """
        Subscribes to periodic refreshes of data associated with a Financial Connections Account. When the account status is active, data is typically refreshed once a day.
        """
        ...

    @overload
    async def subscribe_async(
        self, **params: Unpack["AccountSubscribeParams"]
    ) -> "Account":
        """
        Subscribes to periodic refreshes of data associated with a Financial Connections Account. When the account status is active, data is typically refreshed once a day.
        """
        ...

    @class_method_variant("_cls_subscribe_async")
    async def subscribe_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AccountSubscribeParams"]
    ) -> "Account":
        """
        Subscribes to periodic refreshes of data associated with a Financial Connections Account. When the account status is active, data is typically refreshed once a day.
        """
        return cast(
            "Account",
            await self._request_async(
                "post",
                "/v1/financial_connections/accounts/{account}/subscribe".format(
                    account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    def _cls_unsubscribe(
        cls, account: str, **params: Unpack["AccountUnsubscribeParams"]
    ) -> "Account":
        """
        Unsubscribes from periodic refreshes of data associated with a Financial Connections Account.
        """
        return cast(
            "Account",
            cls._static_request(
                "post",
                "/v1/financial_connections/accounts/{account}/unsubscribe".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    def unsubscribe(
        account: str, **params: Unpack["AccountUnsubscribeParams"]
    ) -> "Account":
        """
        Unsubscribes from periodic refreshes of data associated with a Financial Connections Account.
        """
        ...

    @overload
    def unsubscribe(
        self, **params: Unpack["AccountUnsubscribeParams"]
    ) -> "Account":
        """
        Unsubscribes from periodic refreshes of data associated with a Financial Connections Account.
        """
        ...

    @class_method_variant("_cls_unsubscribe")
    def unsubscribe(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AccountUnsubscribeParams"]
    ) -> "Account":
        """
        Unsubscribes from periodic refreshes of data associated with a Financial Connections Account.
        """
        return cast(
            "Account",
            self._request(
                "post",
                "/v1/financial_connections/accounts/{account}/unsubscribe".format(
                    account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    @classmethod
    async def _cls_unsubscribe_async(
        cls, account: str, **params: Unpack["AccountUnsubscribeParams"]
    ) -> "Account":
        """
        Unsubscribes from periodic refreshes of data associated with a Financial Connections Account.
        """
        return cast(
            "Account",
            await cls._static_request_async(
                "post",
                "/v1/financial_connections/accounts/{account}/unsubscribe".format(
                    account=sanitize_id(account)
                ),
                params=params,
            ),
        )

    @overload
    @staticmethod
    async def unsubscribe_async(
        account: str, **params: Unpack["AccountUnsubscribeParams"]
    ) -> "Account":
        """
        Unsubscribes from periodic refreshes of data associated with a Financial Connections Account.
        """
        ...

    @overload
    async def unsubscribe_async(
        self, **params: Unpack["AccountUnsubscribeParams"]
    ) -> "Account":
        """
        Unsubscribes from periodic refreshes of data associated with a Financial Connections Account.
        """
        ...

    @class_method_variant("_cls_unsubscribe_async")
    async def unsubscribe_async(  # pyright: ignore[reportGeneralTypeIssues]
        self, **params: Unpack["AccountUnsubscribeParams"]
    ) -> "Account":
        """
        Unsubscribes from periodic refreshes of data associated with a Financial Connections Account.
        """
        return cast(
            "Account",
            await self._request_async(
                "post",
                "/v1/financial_connections/accounts/{account}/unsubscribe".format(
                    account=sanitize_id(self.get("id"))
                ),
                params=params,
            ),
        )

    _inner_class_types = {
        "account_holder": AccountHolder,
        "account_numbers": AccountNumber,
        "balance": Balance,
        "balance_refresh": BalanceRefresh,
        "ownership_refresh": OwnershipRefresh,
        "transaction_refresh": TransactionRefresh,
    }
