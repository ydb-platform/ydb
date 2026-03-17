# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._customer_balance_transaction import CustomerBalanceTransaction
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._customer_balance_transaction_create_params import (
        CustomerBalanceTransactionCreateParams,
    )
    from stripe.params._customer_balance_transaction_list_params import (
        CustomerBalanceTransactionListParams,
    )
    from stripe.params._customer_balance_transaction_retrieve_params import (
        CustomerBalanceTransactionRetrieveParams,
    )
    from stripe.params._customer_balance_transaction_update_params import (
        CustomerBalanceTransactionUpdateParams,
    )


class CustomerBalanceTransactionService(StripeService):
    def list(
        self,
        customer: str,
        params: Optional["CustomerBalanceTransactionListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CustomerBalanceTransaction]":
        """
        Returns a list of transactions that updated the customer's [balances](https://docs.stripe.com/docs/billing/customer/balance).
        """
        return cast(
            "ListObject[CustomerBalanceTransaction]",
            self._request(
                "get",
                "/v1/customers/{customer}/balance_transactions".format(
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
        params: Optional["CustomerBalanceTransactionListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CustomerBalanceTransaction]":
        """
        Returns a list of transactions that updated the customer's [balances](https://docs.stripe.com/docs/billing/customer/balance).
        """
        return cast(
            "ListObject[CustomerBalanceTransaction]",
            await self._request_async(
                "get",
                "/v1/customers/{customer}/balance_transactions".format(
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
        params: "CustomerBalanceTransactionCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "CustomerBalanceTransaction":
        """
        Creates an immutable transaction that updates the customer's credit [balance](https://docs.stripe.com/docs/billing/customer/balance).
        """
        return cast(
            "CustomerBalanceTransaction",
            self._request(
                "post",
                "/v1/customers/{customer}/balance_transactions".format(
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
        params: "CustomerBalanceTransactionCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "CustomerBalanceTransaction":
        """
        Creates an immutable transaction that updates the customer's credit [balance](https://docs.stripe.com/docs/billing/customer/balance).
        """
        return cast(
            "CustomerBalanceTransaction",
            await self._request_async(
                "post",
                "/v1/customers/{customer}/balance_transactions".format(
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
        transaction: str,
        params: Optional["CustomerBalanceTransactionRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CustomerBalanceTransaction":
        """
        Retrieves a specific customer balance transaction that updated the customer's [balances](https://docs.stripe.com/docs/billing/customer/balance).
        """
        return cast(
            "CustomerBalanceTransaction",
            self._request(
                "get",
                "/v1/customers/{customer}/balance_transactions/{transaction}".format(
                    customer=sanitize_id(customer),
                    transaction=sanitize_id(transaction),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        customer: str,
        transaction: str,
        params: Optional["CustomerBalanceTransactionRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CustomerBalanceTransaction":
        """
        Retrieves a specific customer balance transaction that updated the customer's [balances](https://docs.stripe.com/docs/billing/customer/balance).
        """
        return cast(
            "CustomerBalanceTransaction",
            await self._request_async(
                "get",
                "/v1/customers/{customer}/balance_transactions/{transaction}".format(
                    customer=sanitize_id(customer),
                    transaction=sanitize_id(transaction),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        customer: str,
        transaction: str,
        params: Optional["CustomerBalanceTransactionUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CustomerBalanceTransaction":
        """
        Most credit balance transaction fields are immutable, but you may update its description and metadata.
        """
        return cast(
            "CustomerBalanceTransaction",
            self._request(
                "post",
                "/v1/customers/{customer}/balance_transactions/{transaction}".format(
                    customer=sanitize_id(customer),
                    transaction=sanitize_id(transaction),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        customer: str,
        transaction: str,
        params: Optional["CustomerBalanceTransactionUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "CustomerBalanceTransaction":
        """
        Most credit balance transaction fields are immutable, but you may update its description and metadata.
        """
        return cast(
            "CustomerBalanceTransaction",
            await self._request_async(
                "post",
                "/v1/customers/{customer}/balance_transactions/{transaction}".format(
                    customer=sanitize_id(customer),
                    transaction=sanitize_id(transaction),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
