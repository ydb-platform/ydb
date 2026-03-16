# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.v2.core.accounts._person_token_create_params import (
        PersonTokenCreateParams,
    )
    from stripe.params.v2.core.accounts._person_token_retrieve_params import (
        PersonTokenRetrieveParams,
    )
    from stripe.v2.core._account_person_token import AccountPersonToken


class PersonTokenService(StripeService):
    def create(
        self,
        account_id: str,
        params: Optional["PersonTokenCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "AccountPersonToken":
        """
        Creates a Person Token associated with an Account.
        """
        return cast(
            "AccountPersonToken",
            self._request(
                "post",
                "/v2/core/accounts/{account_id}/person_tokens".format(
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
        params: Optional["PersonTokenCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "AccountPersonToken":
        """
        Creates a Person Token associated with an Account.
        """
        return cast(
            "AccountPersonToken",
            await self._request_async(
                "post",
                "/v2/core/accounts/{account_id}/person_tokens".format(
                    account_id=sanitize_id(account_id),
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
        params: Optional["PersonTokenRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "AccountPersonToken":
        """
        Retrieves a Person Token associated with an Account.
        """
        return cast(
            "AccountPersonToken",
            self._request(
                "get",
                "/v2/core/accounts/{account_id}/person_tokens/{id}".format(
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
        params: Optional["PersonTokenRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "AccountPersonToken":
        """
        Retrieves a Person Token associated with an Account.
        """
        return cast(
            "AccountPersonToken",
            await self._request_async(
                "get",
                "/v2/core/accounts/{account_id}/person_tokens/{id}".format(
                    account_id=sanitize_id(account_id),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
