# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from importlib import import_module
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._customer import Customer
    from stripe._customer_balance_transaction_service import (
        CustomerBalanceTransactionService,
    )
    from stripe._customer_cash_balance_service import (
        CustomerCashBalanceService,
    )
    from stripe._customer_cash_balance_transaction_service import (
        CustomerCashBalanceTransactionService,
    )
    from stripe._customer_funding_instructions_service import (
        CustomerFundingInstructionsService,
    )
    from stripe._customer_payment_method_service import (
        CustomerPaymentMethodService,
    )
    from stripe._customer_payment_source_service import (
        CustomerPaymentSourceService,
    )
    from stripe._customer_tax_id_service import CustomerTaxIdService
    from stripe._discount import Discount
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._search_result_object import SearchResultObject
    from stripe.params._customer_create_params import CustomerCreateParams
    from stripe.params._customer_delete_discount_params import (
        CustomerDeleteDiscountParams,
    )
    from stripe.params._customer_delete_params import CustomerDeleteParams
    from stripe.params._customer_list_params import CustomerListParams
    from stripe.params._customer_retrieve_params import CustomerRetrieveParams
    from stripe.params._customer_search_params import CustomerSearchParams
    from stripe.params._customer_update_params import CustomerUpdateParams

_subservices = {
    "balance_transactions": [
        "stripe._customer_balance_transaction_service",
        "CustomerBalanceTransactionService",
    ],
    "cash_balance": [
        "stripe._customer_cash_balance_service",
        "CustomerCashBalanceService",
    ],
    "cash_balance_transactions": [
        "stripe._customer_cash_balance_transaction_service",
        "CustomerCashBalanceTransactionService",
    ],
    "funding_instructions": [
        "stripe._customer_funding_instructions_service",
        "CustomerFundingInstructionsService",
    ],
    "payment_methods": [
        "stripe._customer_payment_method_service",
        "CustomerPaymentMethodService",
    ],
    "payment_sources": [
        "stripe._customer_payment_source_service",
        "CustomerPaymentSourceService",
    ],
    "tax_ids": ["stripe._customer_tax_id_service", "CustomerTaxIdService"],
}


class CustomerService(StripeService):
    balance_transactions: "CustomerBalanceTransactionService"
    cash_balance: "CustomerCashBalanceService"
    cash_balance_transactions: "CustomerCashBalanceTransactionService"
    funding_instructions: "CustomerFundingInstructionsService"
    payment_methods: "CustomerPaymentMethodService"
    payment_sources: "CustomerPaymentSourceService"
    tax_ids: "CustomerTaxIdService"

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
        customer: str,
        params: Optional["CustomerDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Customer":
        """
        Permanently deletes a customer. It cannot be undone. Also immediately cancels any active subscriptions on the customer.
        """
        return cast(
            "Customer",
            self._request(
                "delete",
                "/v1/customers/{customer}".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        customer: str,
        params: Optional["CustomerDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Customer":
        """
        Permanently deletes a customer. It cannot be undone. Also immediately cancels any active subscriptions on the customer.
        """
        return cast(
            "Customer",
            await self._request_async(
                "delete",
                "/v1/customers/{customer}".format(
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
        params: Optional["CustomerRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Customer":
        """
        Retrieves a Customer object.
        """
        return cast(
            "Customer",
            self._request(
                "get",
                "/v1/customers/{customer}".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        customer: str,
        params: Optional["CustomerRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Customer":
        """
        Retrieves a Customer object.
        """
        return cast(
            "Customer",
            await self._request_async(
                "get",
                "/v1/customers/{customer}".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        customer: str,
        params: Optional["CustomerUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Customer":
        """
        Updates the specified customer by setting the values of the parameters passed. Any parameters not provided are left unchanged. For example, if you pass the source parameter, that becomes the customer's active source (such as a card) to be used for all charges in the future. When you update a customer to a new valid card source by passing the source parameter: for each of the customer's current subscriptions, if the subscription bills automatically and is in the past_due state, then the latest open invoice for the subscription with automatic collection enabled is retried. This retry doesn't count as an automatic retry, and doesn't affect the next regularly scheduled payment for the invoice. Changing the default_source for a customer doesn't trigger this behavior.

        This request accepts mostly the same arguments as the customer creation call.
        """
        return cast(
            "Customer",
            self._request(
                "post",
                "/v1/customers/{customer}".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        customer: str,
        params: Optional["CustomerUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Customer":
        """
        Updates the specified customer by setting the values of the parameters passed. Any parameters not provided are left unchanged. For example, if you pass the source parameter, that becomes the customer's active source (such as a card) to be used for all charges in the future. When you update a customer to a new valid card source by passing the source parameter: for each of the customer's current subscriptions, if the subscription bills automatically and is in the past_due state, then the latest open invoice for the subscription with automatic collection enabled is retried. This retry doesn't count as an automatic retry, and doesn't affect the next regularly scheduled payment for the invoice. Changing the default_source for a customer doesn't trigger this behavior.

        This request accepts mostly the same arguments as the customer creation call.
        """
        return cast(
            "Customer",
            await self._request_async(
                "post",
                "/v1/customers/{customer}".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def delete_discount(
        self,
        customer: str,
        params: Optional["CustomerDeleteDiscountParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Discount":
        """
        Removes the currently applied discount on a customer.
        """
        return cast(
            "Discount",
            self._request(
                "delete",
                "/v1/customers/{customer}/discount".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_discount_async(
        self,
        customer: str,
        params: Optional["CustomerDeleteDiscountParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Discount":
        """
        Removes the currently applied discount on a customer.
        """
        return cast(
            "Discount",
            await self._request_async(
                "delete",
                "/v1/customers/{customer}/discount".format(
                    customer=sanitize_id(customer),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: Optional["CustomerListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Customer]":
        """
        Returns a list of your customers. The customers are returned sorted by creation date, with the most recent customers appearing first.
        """
        return cast(
            "ListObject[Customer]",
            self._request(
                "get",
                "/v1/customers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["CustomerListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Customer]":
        """
        Returns a list of your customers. The customers are returned sorted by creation date, with the most recent customers appearing first.
        """
        return cast(
            "ListObject[Customer]",
            await self._request_async(
                "get",
                "/v1/customers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["CustomerCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Customer":
        """
        Creates a new customer object.
        """
        return cast(
            "Customer",
            self._request(
                "post",
                "/v1/customers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["CustomerCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Customer":
        """
        Creates a new customer object.
        """
        return cast(
            "Customer",
            await self._request_async(
                "post",
                "/v1/customers",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def search(
        self,
        params: "CustomerSearchParams",
        options: Optional["RequestOptions"] = None,
    ) -> "SearchResultObject[Customer]":
        """
        Search for customers you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cast(
            "SearchResultObject[Customer]",
            self._request(
                "get",
                "/v1/customers/search",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def search_async(
        self,
        params: "CustomerSearchParams",
        options: Optional["RequestOptions"] = None,
    ) -> "SearchResultObject[Customer]":
        """
        Search for customers you've previously created using Stripe's [Search Query Language](https://docs.stripe.com/docs/search#search-query-language).
        Don't use search in read-after-write flows where strict consistency is necessary. Under normal operating
        conditions, data is searchable in less than a minute. Occasionally, propagation of new or updated data can be up
        to an hour behind during outages. Search functionality is not available to merchants in India.
        """
        return cast(
            "SearchResultObject[Customer]",
            await self._request_async(
                "get",
                "/v1/customers/search",
                base_address="api",
                params=params,
                options=options,
            ),
        )
