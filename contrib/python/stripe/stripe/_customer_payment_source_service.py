# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._account import Account
    from stripe._bank_account import BankAccount
    from stripe._card import Card
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._source import Source
    from stripe.params._customer_payment_source_create_params import (
        CustomerPaymentSourceCreateParams,
    )
    from stripe.params._customer_payment_source_delete_params import (
        CustomerPaymentSourceDeleteParams,
    )
    from stripe.params._customer_payment_source_list_params import (
        CustomerPaymentSourceListParams,
    )
    from stripe.params._customer_payment_source_retrieve_params import (
        CustomerPaymentSourceRetrieveParams,
    )
    from stripe.params._customer_payment_source_update_params import (
        CustomerPaymentSourceUpdateParams,
    )
    from stripe.params._customer_payment_source_verify_params import (
        CustomerPaymentSourceVerifyParams,
    )
    from typing import Union


class CustomerPaymentSourceService(StripeService):
    def list(
        self,
        customer: str,
        params: Optional["CustomerPaymentSourceListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Union[Account, BankAccount, Card, Source]]":
        """
        List sources for a specified customer.
        """
        return cast(
            "ListObject[Union[Account, BankAccount, Card, Source]]",
            self._request(
                "get",
                "/v1/customers/{customer}/sources".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        customer: str,
        params: Optional["CustomerPaymentSourceListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Union[Account, BankAccount, Card, Source]]":
        """
        List sources for a specified customer.
        """
        return cast(
            "ListObject[Union[Account, BankAccount, Card, Source]]",
            await self._request_async(
                "get",
                "/v1/customers/{customer}/sources".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        customer: str,
        params: "CustomerPaymentSourceCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Union[Account, BankAccount, Card, Source]":
        """
        When you create a new credit card, you must specify a customer or recipient on which to create it.

        If the card's owner has no default card, then the new card will become the default.
        However, if the owner already has a default, then it will not change.
        To change the default, you should [update the customer](https://docs.stripe.com/docs/api#update_customer) to have a new default_source.
        """
        return cast(
            "Union[Account, BankAccount, Card, Source]",
            self._request(
                "post",
                "/v1/customers/{customer}/sources".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        customer: str,
        params: "CustomerPaymentSourceCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Union[Account, BankAccount, Card, Source]":
        """
        When you create a new credit card, you must specify a customer or recipient on which to create it.

        If the card's owner has no default card, then the new card will become the default.
        However, if the owner already has a default, then it will not change.
        To change the default, you should [update the customer](https://docs.stripe.com/docs/api#update_customer) to have a new default_source.
        """
        return cast(
            "Union[Account, BankAccount, Card, Source]",
            await self._request_async(
                "post",
                "/v1/customers/{customer}/sources".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        customer: str,
        id: str,
        params: Optional["CustomerPaymentSourceRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Union[Account, BankAccount, Card, Source]":
        """
        Retrieve a specified source for a given customer.
        """
        return cast(
            "Union[Account, BankAccount, Card, Source]",
            self._request(
                "get",
                "/v1/customers/{customer}/sources/{id}".format(
                    customer=sanitize_id(customer),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        customer: str,
        id: str,
        params: Optional["CustomerPaymentSourceRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Union[Account, BankAccount, Card, Source]":
        """
        Retrieve a specified source for a given customer.
        """
        return cast(
            "Union[Account, BankAccount, Card, Source]",
            await self._request_async(
                "get",
                "/v1/customers/{customer}/sources/{id}".format(
                    customer=sanitize_id(customer),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        customer: str,
        id: str,
        params: Optional["CustomerPaymentSourceUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Union[Account, BankAccount, Card, Source]":
        """
        Update a specified source for a given customer.
        """
        return cast(
            "Union[Account, BankAccount, Card, Source]",
            self._request(
                "post",
                "/v1/customers/{customer}/sources/{id}".format(
                    customer=sanitize_id(customer),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        customer: str,
        id: str,
        params: Optional["CustomerPaymentSourceUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Union[Account, BankAccount, Card, Source]":
        """
        Update a specified source for a given customer.
        """
        return cast(
            "Union[Account, BankAccount, Card, Source]",
            await self._request_async(
                "post",
                "/v1/customers/{customer}/sources/{id}".format(
                    customer=sanitize_id(customer),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def delete(
        self,
        customer: str,
        id: str,
        params: Optional["CustomerPaymentSourceDeleteParams"] = None,
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

    async def delete_async(
        self,
        customer: str,
        id: str,
        params: Optional["CustomerPaymentSourceDeleteParams"] = None,
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

    def verify(
        self,
        customer: str,
        id: str,
        params: Optional["CustomerPaymentSourceVerifyParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "BankAccount":
        """
        Verify a specified bank account for a given customer.
        """
        return cast(
            "BankAccount",
            self._request(
                "post",
                "/v1/customers/{customer}/sources/{id}/verify".format(
                    customer=sanitize_id(customer),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def verify_async(
        self,
        customer: str,
        id: str,
        params: Optional["CustomerPaymentSourceVerifyParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "BankAccount":
        """
        Verify a specified bank account for a given customer.
        """
        return cast(
            "BankAccount",
            await self._request_async(
                "post",
                "/v1/customers/{customer}/sources/{id}/verify".format(
                    customer=sanitize_id(customer),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
