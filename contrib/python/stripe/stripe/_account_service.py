# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._account import Account
    from stripe._account_capability_service import AccountCapabilityService
    from stripe._account_external_account_service import (
        AccountExternalAccountService,
    )
    from stripe._account_login_link_service import AccountLoginLinkService
    from stripe._account_person_service import AccountPersonService
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._account_create_params import AccountCreateParams
    from stripe.params._account_delete_params import AccountDeleteParams
    from stripe.params._account_list_params import AccountListParams
    from stripe.params._account_reject_params import AccountRejectParams
    from stripe.params._account_retrieve_current_params import (
        AccountRetrieveCurrentParams,
    )
    from stripe.params._account_retrieve_params import AccountRetrieveParams
    from stripe.params._account_update_params import AccountUpdateParams

_subservices = {
    "capabilities": [
        "stripe._account_capability_service",
        "AccountCapabilityService",
    ],
    "external_accounts": [
        "stripe._account_external_account_service",
        "AccountExternalAccountService",
    ],
    "login_links": [
        "stripe._account_login_link_service",
        "AccountLoginLinkService",
    ],
    "persons": ["stripe._account_person_service", "AccountPersonService"],
}


class AccountService(StripeService):
    capabilities: "AccountCapabilityService"
    external_accounts: "AccountExternalAccountService"
    login_links: "AccountLoginLinkService"
    persons: "AccountPersonService"

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

    def delete(
        self,
        account: str,
        params: Optional["AccountDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can delete accounts you manage.

        Test-mode accounts can be deleted at any time.

        Live-mode accounts that have access to the standard dashboard and Stripe is responsible for negative account balances cannot be deleted, which includes Standard accounts. All other Live-mode accounts, can be deleted when all [balances](https://docs.stripe.com/api/balance/balance_object) are zero.

        If you want to delete your own account, use the [account information tab in your account settings](https://dashboard.stripe.com/settings/account) instead.
        """
        return cast(
            "Account",
            self._request(
                "delete",
                "/v1/accounts/{account}".format(account=sanitize_id(account)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        account: str,
        params: Optional["AccountDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can delete accounts you manage.

        Test-mode accounts can be deleted at any time.

        Live-mode accounts that have access to the standard dashboard and Stripe is responsible for negative account balances cannot be deleted, which includes Standard accounts. All other Live-mode accounts, can be deleted when all [balances](https://docs.stripe.com/api/balance/balance_object) are zero.

        If you want to delete your own account, use the [account information tab in your account settings](https://dashboard.stripe.com/settings/account) instead.
        """
        return cast(
            "Account",
            await self._request_async(
                "delete",
                "/v1/accounts/{account}".format(account=sanitize_id(account)),
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
        Retrieves the details of an account.
        """
        return cast(
            "Account",
            self._request(
                "get",
                "/v1/accounts/{account}".format(account=sanitize_id(account)),
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
        Retrieves the details of an account.
        """
        return cast(
            "Account",
            await self._request_async(
                "get",
                "/v1/accounts/{account}".format(account=sanitize_id(account)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        account: str,
        params: Optional["AccountUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Updates a [connected account](https://docs.stripe.com/connect/accounts) by setting the values of the parameters passed. Any parameters not provided are
        left unchanged.

        For accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection)
        is application, which includes Custom accounts, you can update any information on the account.

        For accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection)
        is stripe, which includes Standard and Express accounts, you can update all information until you create
        an [Account Link or <a href="/api/account_sessions">Account Session](https://docs.stripe.com/api/account_links) to start Connect onboarding,
        after which some properties can no longer be updated.

        To update your own account, use the [Dashboard](https://dashboard.stripe.com/settings/account). Refer to our
        [Connect](https://docs.stripe.com/docs/connect/updating-accounts) documentation to learn more about updating accounts.
        """
        return cast(
            "Account",
            self._request(
                "post",
                "/v1/accounts/{account}".format(account=sanitize_id(account)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        account: str,
        params: Optional["AccountUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Updates a [connected account](https://docs.stripe.com/connect/accounts) by setting the values of the parameters passed. Any parameters not provided are
        left unchanged.

        For accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection)
        is application, which includes Custom accounts, you can update any information on the account.

        For accounts where [controller.requirement_collection](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection)
        is stripe, which includes Standard and Express accounts, you can update all information until you create
        an [Account Link or <a href="/api/account_sessions">Account Session](https://docs.stripe.com/api/account_links) to start Connect onboarding,
        after which some properties can no longer be updated.

        To update your own account, use the [Dashboard](https://dashboard.stripe.com/settings/account). Refer to our
        [Connect](https://docs.stripe.com/docs/connect/updating-accounts) documentation to learn more about updating accounts.
        """
        return cast(
            "Account",
            await self._request_async(
                "post",
                "/v1/accounts/{account}".format(account=sanitize_id(account)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve_current(
        self,
        params: Optional["AccountRetrieveCurrentParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Retrieves the details of an account.
        """
        return cast(
            "Account",
            self._request(
                "get",
                "/v1/account",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_current_async(
        self,
        params: Optional["AccountRetrieveCurrentParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Retrieves the details of an account.
        """
        return cast(
            "Account",
            await self._request_async(
                "get",
                "/v1/account",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: Optional["AccountListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Account]":
        """
        Returns a list of accounts connected to your platform via [Connect](https://docs.stripe.com/docs/connect). If you're not a platform, the list is empty.
        """
        return cast(
            "ListObject[Account]",
            self._request(
                "get",
                "/v1/accounts",
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
        Returns a list of accounts connected to your platform via [Connect](https://docs.stripe.com/docs/connect). If you're not a platform, the list is empty.
        """
        return cast(
            "ListObject[Account]",
            await self._request_async(
                "get",
                "/v1/accounts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["AccountCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/docs/connect), you can create Stripe accounts for your users.
        To do this, you'll first need to [register your platform](https://dashboard.stripe.com/account/applications/settings).

        If you've already collected information for your connected accounts, you [can prefill that information](https://docs.stripe.com/docs/connect/best-practices#onboarding) when
        creating the account. Connect Onboarding won't ask for the prefilled information during account onboarding.
        You can prefill any information on the account.
        """
        return cast(
            "Account",
            self._request(
                "post",
                "/v1/accounts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["AccountCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/docs/connect), you can create Stripe accounts for your users.
        To do this, you'll first need to [register your platform](https://dashboard.stripe.com/account/applications/settings).

        If you've already collected information for your connected accounts, you [can prefill that information](https://docs.stripe.com/docs/connect/best-practices#onboarding) when
        creating the account. Connect Onboarding won't ask for the prefilled information during account onboarding.
        You can prefill any information on the account.
        """
        return cast(
            "Account",
            await self._request_async(
                "post",
                "/v1/accounts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def reject(
        self,
        account: str,
        params: "AccountRejectParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can reject accounts that you have flagged as suspicious.

        Only accounts where your platform is liable for negative account balances, which includes Custom and Express accounts, can be rejected. Test-mode accounts can be rejected at any time. Live-mode accounts can only be rejected after all balances are zero.
        """
        return cast(
            "Account",
            self._request(
                "post",
                "/v1/accounts/{account}/reject".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def reject_async(
        self,
        account: str,
        params: "AccountRejectParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        With [Connect](https://docs.stripe.com/connect), you can reject accounts that you have flagged as suspicious.

        Only accounts where your platform is liable for negative account balances, which includes Custom and Express accounts, can be rejected. Test-mode accounts can be rejected at any time. Live-mode accounts can only be rejected after all balances are zero.
        """
        return cast(
            "Account",
            await self._request_async(
                "post",
                "/v1/accounts/{account}/reject".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
