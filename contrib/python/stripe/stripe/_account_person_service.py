# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._person import Person
    from stripe._request_options import RequestOptions
    from stripe.params._account_person_create_params import (
        AccountPersonCreateParams,
    )
    from stripe.params._account_person_delete_params import (
        AccountPersonDeleteParams,
    )
    from stripe.params._account_person_list_params import (
        AccountPersonListParams,
    )
    from stripe.params._account_person_retrieve_params import (
        AccountPersonRetrieveParams,
    )
    from stripe.params._account_person_update_params import (
        AccountPersonUpdateParams,
    )


class AccountPersonService(StripeService):
    def delete(
        self,
        account: str,
        person: str,
        params: Optional["AccountPersonDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Person":
        """
        Deletes an existing person's relationship to the account's legal entity. Any person with a relationship for an account can be deleted through the API, except if the person is the account_opener. If your integration is using the executive parameter, you cannot delete the only verified executive on file.
        """
        return cast(
            "Person",
            self._request(
                "delete",
                "/v1/accounts/{account}/persons/{person}".format(
                    account=sanitize_id(account),
                    person=sanitize_id(person),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        account: str,
        person: str,
        params: Optional["AccountPersonDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Person":
        """
        Deletes an existing person's relationship to the account's legal entity. Any person with a relationship for an account can be deleted through the API, except if the person is the account_opener. If your integration is using the executive parameter, you cannot delete the only verified executive on file.
        """
        return cast(
            "Person",
            await self._request_async(
                "delete",
                "/v1/accounts/{account}/persons/{person}".format(
                    account=sanitize_id(account),
                    person=sanitize_id(person),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        account: str,
        person: str,
        params: Optional["AccountPersonRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Person":
        """
        Retrieves an existing person.
        """
        return cast(
            "Person",
            self._request(
                "get",
                "/v1/accounts/{account}/persons/{person}".format(
                    account=sanitize_id(account),
                    person=sanitize_id(person),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        account: str,
        person: str,
        params: Optional["AccountPersonRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Person":
        """
        Retrieves an existing person.
        """
        return cast(
            "Person",
            await self._request_async(
                "get",
                "/v1/accounts/{account}/persons/{person}".format(
                    account=sanitize_id(account),
                    person=sanitize_id(person),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        account: str,
        person: str,
        params: Optional["AccountPersonUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Person":
        """
        Updates an existing person.
        """
        return cast(
            "Person",
            self._request(
                "post",
                "/v1/accounts/{account}/persons/{person}".format(
                    account=sanitize_id(account),
                    person=sanitize_id(person),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        account: str,
        person: str,
        params: Optional["AccountPersonUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Person":
        """
        Updates an existing person.
        """
        return cast(
            "Person",
            await self._request_async(
                "post",
                "/v1/accounts/{account}/persons/{person}".format(
                    account=sanitize_id(account),
                    person=sanitize_id(person),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        account: str,
        params: Optional["AccountPersonListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Person]":
        """
        Returns a list of people associated with the account's legal entity. The people are returned sorted by creation date, with the most recent people appearing first.
        """
        return cast(
            "ListObject[Person]",
            self._request(
                "get",
                "/v1/accounts/{account}/persons".format(
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
        params: Optional["AccountPersonListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Person]":
        """
        Returns a list of people associated with the account's legal entity. The people are returned sorted by creation date, with the most recent people appearing first.
        """
        return cast(
            "ListObject[Person]",
            await self._request_async(
                "get",
                "/v1/accounts/{account}/persons".format(
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
        params: Optional["AccountPersonCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Person":
        """
        Creates a new person.
        """
        return cast(
            "Person",
            self._request(
                "post",
                "/v1/accounts/{account}/persons".format(
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
        params: Optional["AccountPersonCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Person":
        """
        Creates a new person.
        """
        return cast(
            "Person",
            await self._request_async(
                "post",
                "/v1/accounts/{account}/persons".format(
                    account=sanitize_id(account),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
