# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._account import Account
    from stripe._bank_account import BankAccount
    from stripe._card import Card
    from stripe._request_options import RequestOptions
    from stripe._source import Source
    from stripe._source_transaction_service import SourceTransactionService
    from stripe.params._source_create_params import SourceCreateParams
    from stripe.params._source_detach_params import SourceDetachParams
    from stripe.params._source_retrieve_params import SourceRetrieveParams
    from stripe.params._source_update_params import SourceUpdateParams
    from stripe.params._source_verify_params import SourceVerifyParams
    from typing import Union

_subservices = {
    "transactions": [
        "stripe._source_transaction_service",
        "SourceTransactionService",
    ],
}


class SourceService(StripeService):
    transactions: "SourceTransactionService"

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

    def detach(
        self,
        customer: str,
        id: str,
        params: Optional["SourceDetachParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Union[Account, BankAccount, Card, Source]":
        """
        Delete a specified source for a given customer.
        """
        return cast(
            "Union[Account, BankAccount, Card, Source]",
            self._request(
                "delete",
                "/v1/customers/{customer}/sources/{id}".format(
                    customer=sanitize_id(customer),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def detach_async(
        self,
        customer: str,
        id: str,
        params: Optional["SourceDetachParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Union[Account, BankAccount, Card, Source]":
        """
        Delete a specified source for a given customer.
        """
        return cast(
            "Union[Account, BankAccount, Card, Source]",
            await self._request_async(
                "delete",
                "/v1/customers/{customer}/sources/{id}".format(
                    customer=sanitize_id(customer),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        source: str,
        params: Optional["SourceRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Source":
        """
        Retrieves an existing source object. Supply the unique source ID from a source creation request and Stripe will return the corresponding up-to-date source object information.
        """
        return cast(
            "Source",
            self._request(
                "get",
                "/v1/sources/{source}".format(source=sanitize_id(source)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        source: str,
        params: Optional["SourceRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Source":
        """
        Retrieves an existing source object. Supply the unique source ID from a source creation request and Stripe will return the corresponding up-to-date source object information.
        """
        return cast(
            "Source",
            await self._request_async(
                "get",
                "/v1/sources/{source}".format(source=sanitize_id(source)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        source: str,
        params: Optional["SourceUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Source":
        """
        Updates the specified source by setting the values of the parameters passed. Any parameters not provided will be left unchanged.

        This request accepts the metadata and owner as arguments. It is also possible to update type specific information for selected payment methods. Please refer to our [payment method guides](https://docs.stripe.com/docs/sources) for more detail.
        """
        return cast(
            "Source",
            self._request(
                "post",
                "/v1/sources/{source}".format(source=sanitize_id(source)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        source: str,
        params: Optional["SourceUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Source":
        """
        Updates the specified source by setting the values of the parameters passed. Any parameters not provided will be left unchanged.

        This request accepts the metadata and owner as arguments. It is also possible to update type specific information for selected payment methods. Please refer to our [payment method guides](https://docs.stripe.com/docs/sources) for more detail.
        """
        return cast(
            "Source",
            await self._request_async(
                "post",
                "/v1/sources/{source}".format(source=sanitize_id(source)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["SourceCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Source":
        """
        Creates a new source object.
        """
        return cast(
            "Source",
            self._request(
                "post",
                "/v1/sources",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["SourceCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Source":
        """
        Creates a new source object.
        """
        return cast(
            "Source",
            await self._request_async(
                "post",
                "/v1/sources",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def verify(
        self,
        source: str,
        params: "SourceVerifyParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Source":
        """
        Verify a given source.
        """
        return cast(
            "Source",
            self._request(
                "post",
                "/v1/sources/{source}/verify".format(
                    source=sanitize_id(source),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def verify_async(
        self,
        source: str,
        params: "SourceVerifyParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Source":
        """
        Verify a given source.
        """
        return cast(
            "Source",
            await self._request_async(
                "post",
                "/v1/sources/{source}/verify".format(
                    source=sanitize_id(source),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
