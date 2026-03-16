# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._bank_account import BankAccount
    from stripe._card import Card
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._account_external_account_create_params import (
        AccountExternalAccountCreateParams,
    )
    from stripe.params._account_external_account_delete_params import (
        AccountExternalAccountDeleteParams,
    )
    from stripe.params._account_external_account_list_params import (
        AccountExternalAccountListParams,
    )
    from stripe.params._account_external_account_retrieve_params import (
        AccountExternalAccountRetrieveParams,
    )
    from stripe.params._account_external_account_update_params import (
        AccountExternalAccountUpdateParams,
    )
    from typing import Union


class AccountExternalAccountService(StripeService):
    def delete(
        self,
        account: str,
        id: str,
        params: Optional["AccountExternalAccountDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Union[BankAccount, Card]":
        """
        Delete a specified external account for a given account.
        """
        return cast(
            "Union[BankAccount, Card]",
            self._request(
                "delete",
                "/v1/accounts/{account}/external_accounts/{id}".format(
                    account=sanitize_id(account),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        account: str,
        id: str,
        params: Optional["AccountExternalAccountDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Union[BankAccount, Card]":
        """
        Delete a specified external account for a given account.
        """
        return cast(
            "Union[BankAccount, Card]",
            await self._request_async(
                "delete",
                "/v1/accounts/{account}/external_accounts/{id}".format(
                    account=sanitize_id(account),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        account: str,
        id: str,
        params: Optional["AccountExternalAccountRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Union[BankAccount, Card]":
        """
        Retrieve a specified external account for a given account.
        """
        return cast(
            "Union[BankAccount, Card]",
            self._request(
                "get",
                "/v1/accounts/{account}/external_accounts/{id}".format(
                    account=sanitize_id(account),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        account: str,
        id: str,
        params: Optional["AccountExternalAccountRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Union[BankAccount, Card]":
        """
        Retrieve a specified external account for a given account.
        """
        return cast(
            "Union[BankAccount, Card]",
            await self._request_async(
                "get",
                "/v1/accounts/{account}/external_accounts/{id}".format(
                    account=sanitize_id(account),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        account: str,
        id: str,
        params: Optional["AccountExternalAccountUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Union[BankAccount, Card]":
        """
        Updates the metadata, account holder name, account holder type of a bank account belonging to
        a connected account and optionally sets it as the default for its currency. Other bank account
        details are not editable by design.

        You can only update bank accounts when [account.controller.requirement_collection is application, which includes <a href="/connect/custom-accounts">Custom accounts](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection).

        You can re-enable a disabled bank account by performing an update call without providing any
        arguments or changes.
        """
        return cast(
            "Union[BankAccount, Card]",
            self._request(
                "post",
                "/v1/accounts/{account}/external_accounts/{id}".format(
                    account=sanitize_id(account),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        account: str,
        id: str,
        params: Optional["AccountExternalAccountUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Union[BankAccount, Card]":
        """
        Updates the metadata, account holder name, account holder type of a bank account belonging to
        a connected account and optionally sets it as the default for its currency. Other bank account
        details are not editable by design.

        You can only update bank accounts when [account.controller.requirement_collection is application, which includes <a href="/connect/custom-accounts">Custom accounts](https://docs.stripe.com/api/accounts/object#account_object-controller-requirement_collection).

        You can re-enable a disabled bank account by performing an update call without providing any
        arguments or changes.
        """
        return cast(
            "Union[BankAccount, Card]",
            await self._request_async(
                "post",
                "/v1/accounts/{account}/external_accounts/{id}".format(
                    account=sanitize_id(account),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        account: str,
        params: Optional["AccountExternalAccountListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Union[BankAccount, Card]]":
        """
        List external accounts for an account.
        """
        return cast(
            "ListObject[Union[BankAccount, Card]]",
            self._request(
                "get",
                "/v1/accounts/{account}/external_accounts".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        account: str,
        params: Optional["AccountExternalAccountListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Union[BankAccount, Card]]":
        """
        List external accounts for an account.
        """
        return cast(
            "ListObject[Union[BankAccount, Card]]",
            await self._request_async(
                "get",
                "/v1/accounts/{account}/external_accounts".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        account: str,
        params: "AccountExternalAccountCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Union[BankAccount, Card]":
        """
        Create an external account for a given account.
        """
        return cast(
            "Union[BankAccount, Card]",
            self._request(
                "post",
                "/v1/accounts/{account}/external_accounts".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        account: str,
        params: "AccountExternalAccountCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Union[BankAccount, Card]":
        """
        Create an external account for a given account.
        """
        return cast(
            "Union[BankAccount, Card]",
            await self._request_async(
                "post",
                "/v1/accounts/{account}/external_accounts".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
