# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.financial_connections._account import Account
    from stripe.financial_connections._account_owner_service import (
        AccountOwnerService,
    )
    from stripe.params.financial_connections._account_disconnect_params import (
        AccountDisconnectParams,
    )
    from stripe.params.financial_connections._account_list_params import (
        AccountListParams,
    )
    from stripe.params.financial_connections._account_refresh_params import (
        AccountRefreshParams,
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

_subservices = {
    "owners": [
        "stripe.financial_connections._account_owner_service",
        "AccountOwnerService",
    ],
}


class AccountService(StripeService):
    owners: "AccountOwnerService"

    def __init__(self, requestor):
        super().__init__(requestor)

    def __getattr__(self, name):
        try:
            import_from, service = _subservices[name]
            service_class = getattr(
                import_module(import_from),
                service,
            )
            setattr(
                self,
                name,
                service_class(self._requestor),
            )
            return getattr(self, name)
        except KeyError:
            raise AttributeError()

    def list(
        self,
        params: Optional["AccountListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Account]":
        """
        Returns a list of Financial Connections Account objects.
        """
        return cast(
            "ListObject[Account]",
            self._request(
                "get",
                "/v1/financial_connections/accounts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["AccountListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Account]":
        """
        Returns a list of Financial Connections Account objects.
        """
        return cast(
            "ListObject[Account]",
            await self._request_async(
                "get",
                "/v1/financial_connections/accounts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        account: str,
        params: Optional["AccountRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Retrieves the details of an Financial Connections Account.
        """
        return cast(
            "Account",
            self._request(
                "get",
                "/v1/financial_connections/accounts/{account}".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        account: str,
        params: Optional["AccountRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Retrieves the details of an Financial Connections Account.
        """
        return cast(
            "Account",
            await self._request_async(
                "get",
                "/v1/financial_connections/accounts/{account}".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def disconnect(
        self,
        account: str,
        params: Optional["AccountDisconnectParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Disables your access to a Financial Connections Account. You will no longer be able to access data associated with the account (e.g. balances, transactions).
        """
        return cast(
            "Account",
            self._request(
                "post",
                "/v1/financial_connections/accounts/{account}/disconnect".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def disconnect_async(
        self,
        account: str,
        params: Optional["AccountDisconnectParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Disables your access to a Financial Connections Account. You will no longer be able to access data associated with the account (e.g. balances, transactions).
        """
        return cast(
            "Account",
            await self._request_async(
                "post",
                "/v1/financial_connections/accounts/{account}/disconnect".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def refresh(
        self,
        account: str,
        params: "AccountRefreshParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Refreshes the data associated with a Financial Connections Account.
        """
        return cast(
            "Account",
            self._request(
                "post",
                "/v1/financial_connections/accounts/{account}/refresh".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def refresh_async(
        self,
        account: str,
        params: "AccountRefreshParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Refreshes the data associated with a Financial Connections Account.
        """
        return cast(
            "Account",
            await self._request_async(
                "post",
                "/v1/financial_connections/accounts/{account}/refresh".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def subscribe(
        self,
        account: str,
        params: "AccountSubscribeParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Subscribes to periodic refreshes of data associated with a Financial Connections Account. When the account status is active, data is typically refreshed once a day.
        """
        return cast(
            "Account",
            self._request(
                "post",
                "/v1/financial_connections/accounts/{account}/subscribe".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def subscribe_async(
        self,
        account: str,
        params: "AccountSubscribeParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Subscribes to periodic refreshes of data associated with a Financial Connections Account. When the account status is active, data is typically refreshed once a day.
        """
        return cast(
            "Account",
            await self._request_async(
                "post",
                "/v1/financial_connections/accounts/{account}/subscribe".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def unsubscribe(
        self,
        account: str,
        params: "AccountUnsubscribeParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Unsubscribes from periodic refreshes of data associated with a Financial Connections Account.
        """
        return cast(
            "Account",
            self._request(
                "post",
                "/v1/financial_connections/accounts/{account}/unsubscribe".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def unsubscribe_async(
        self,
        account: str,
        params: "AccountUnsubscribeParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Unsubscribes from periodic refreshes of data associated with a Financial Connections Account.
        """
        return cast(
            "Account",
            await self._request_async(
                "post",
                "/v1/financial_connections/accounts/{account}/unsubscribe".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
