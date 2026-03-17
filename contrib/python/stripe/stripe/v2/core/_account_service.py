# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.v2.core._account_close_params import AccountCloseParams
    from stripe.params.v2.core._account_create_params import (
        AccountCreateParams,
    )
    from stripe.params.v2.core._account_list_params import AccountListParams
    from stripe.params.v2.core._account_retrieve_params import (
        AccountRetrieveParams,
    )
    from stripe.params.v2.core._account_update_params import (
        AccountUpdateParams,
    )
    from stripe.v2._list_object import ListObject
    from stripe.v2.core._account import Account
    from stripe.v2.core.accounts._person_service import PersonService
    from stripe.v2.core.accounts._person_token_service import (
        PersonTokenService,
    )

_subservices = {
    "persons": ["stripe.v2.core.accounts._person_service", "PersonService"],
    "person_tokens": [
        "stripe.v2.core.accounts._person_token_service",
        "PersonTokenService",
    ],
}


class AccountService(StripeService):
    persons: "PersonService"
    person_tokens: "PersonTokenService"

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
        Returns a list of Accounts.
        """
        return cast(
            "ListObject[Account]",
            self._request(
                "get",
                "/v2/core/accounts",
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
        Returns a list of Accounts.
        """
        return cast(
            "ListObject[Account]",
            await self._request_async(
                "get",
                "/v2/core/accounts",
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
        An Account is a representation of a company, individual or other entity that a user interacts with. Accounts contain identifying information about the entity, and configurations that store the features an account has access to. An account can be configured as any or all of the following configurations: Customer, Merchant and/or Recipient.
        """
        return cast(
            "Account",
            self._request(
                "post",
                "/v2/core/accounts",
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
        An Account is a representation of a company, individual or other entity that a user interacts with. Accounts contain identifying information about the entity, and configurations that store the features an account has access to. An account can be configured as any or all of the following configurations: Customer, Merchant and/or Recipient.
        """
        return cast(
            "Account",
            await self._request_async(
                "post",
                "/v2/core/accounts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["AccountRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Retrieves the details of an Account.
        """
        return cast(
            "Account",
            self._request(
                "get",
                "/v2/core/accounts/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["AccountRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Retrieves the details of an Account.
        """
        return cast(
            "Account",
            await self._request_async(
                "get",
                "/v2/core/accounts/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        id: str,
        params: Optional["AccountUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Updates the details of an Account.
        """
        return cast(
            "Account",
            self._request(
                "post",
                "/v2/core/accounts/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        id: str,
        params: Optional["AccountUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Updates the details of an Account.
        """
        return cast(
            "Account",
            await self._request_async(
                "post",
                "/v2/core/accounts/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def close(
        self,
        id: str,
        params: Optional["AccountCloseParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Removes access to the Account and its associated resources. Closed Accounts can no longer be operated on, but limited information can still be retrieved through the API in order to be able to track their history.
        """
        return cast(
            "Account",
            self._request(
                "post",
                "/v2/core/accounts/{id}/close".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def close_async(
        self,
        id: str,
        params: Optional["AccountCloseParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Account":
        """
        Removes access to the Account and its associated resources. Closed Accounts can no longer be operated on, but limited information can still be retrieved through the API in order to be able to track their history.
        """
        return cast(
            "Account",
            await self._request_async(
                "post",
                "/v2/core/accounts/{id}/close".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
