# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.v2.core.accounts._person_create_params import (
        PersonCreateParams,
    )
    from stripe.params.v2.core.accounts._person_delete_params import (
        PersonDeleteParams,
    )
    from stripe.params.v2.core.accounts._person_list_params import (
        PersonListParams,
    )
    from stripe.params.v2.core.accounts._person_retrieve_params import (
        PersonRetrieveParams,
    )
    from stripe.params.v2.core.accounts._person_update_params import (
        PersonUpdateParams,
    )
    from stripe.v2._deleted_object import DeletedObject
    from stripe.v2._list_object import ListObject
    from stripe.v2.core._account_person import AccountPerson


class PersonService(StripeService):
    def list(
        self,
        account_id: str,
        params: Optional["PersonListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[AccountPerson]":
        """
        Returns a paginated list of Persons associated with an Account.
        """
        return cast(
            "ListObject[AccountPerson]",
            self._request(
                "get",
                "/v2/core/accounts/{account_id}/persons".format(
                    account_id=sanitize_id(account_id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        account_id: str,
        params: Optional["PersonListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[AccountPerson]":
        """
        Returns a paginated list of Persons associated with an Account.
        """
        return cast(
            "ListObject[AccountPerson]",
            await self._request_async(
                "get",
                "/v2/core/accounts/{account_id}/persons".format(
                    account_id=sanitize_id(account_id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        account_id: str,
        params: Optional["PersonCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "AccountPerson":
        """
        Create a Person. Adds an individual to an Account's identity. You can set relationship attributes and identity information at creation.
        """
        return cast(
            "AccountPerson",
            self._request(
                "post",
                "/v2/core/accounts/{account_id}/persons".format(
                    account_id=sanitize_id(account_id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        account_id: str,
        params: Optional["PersonCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "AccountPerson":
        """
        Create a Person. Adds an individual to an Account's identity. You can set relationship attributes and identity information at creation.
        """
        return cast(
            "AccountPerson",
            await self._request_async(
                "post",
                "/v2/core/accounts/{account_id}/persons".format(
                    account_id=sanitize_id(account_id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def delete(
        self,
        account_id: str,
        id: str,
        params: Optional["PersonDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "DeletedObject":
        """
        Delete a Person associated with an Account.
        """
        return cast(
            "DeletedObject",
            self._request(
                "delete",
                "/v2/core/accounts/{account_id}/persons/{id}".format(
                    account_id=sanitize_id(account_id),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        account_id: str,
        id: str,
        params: Optional["PersonDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "DeletedObject":
        """
        Delete a Person associated with an Account.
        """
        return cast(
            "DeletedObject",
            await self._request_async(
                "delete",
                "/v2/core/accounts/{account_id}/persons/{id}".format(
                    account_id=sanitize_id(account_id),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        account_id: str,
        id: str,
        params: Optional["PersonRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "AccountPerson":
        """
        Retrieves a Person associated with an Account.
        """
        return cast(
            "AccountPerson",
            self._request(
                "get",
                "/v2/core/accounts/{account_id}/persons/{id}".format(
                    account_id=sanitize_id(account_id),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        account_id: str,
        id: str,
        params: Optional["PersonRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "AccountPerson":
        """
        Retrieves a Person associated with an Account.
        """
        return cast(
            "AccountPerson",
            await self._request_async(
                "get",
                "/v2/core/accounts/{account_id}/persons/{id}".format(
                    account_id=sanitize_id(account_id),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        account_id: str,
        id: str,
        params: Optional["PersonUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "AccountPerson":
        """
        Updates a Person associated with an Account.
        """
        return cast(
            "AccountPerson",
            self._request(
                "post",
                "/v2/core/accounts/{account_id}/persons/{id}".format(
                    account_id=sanitize_id(account_id),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        account_id: str,
        id: str,
        params: Optional["PersonUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "AccountPerson":
        """
        Updates a Person associated with an Account.
        """
        return cast(
            "AccountPerson",
            await self._request_async(
                "post",
                "/v2/core/accounts/{account_id}/persons/{id}".format(
                    account_id=sanitize_id(account_id),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
