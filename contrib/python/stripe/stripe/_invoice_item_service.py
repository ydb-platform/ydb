# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._invoice_item import InvoiceItem
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._invoice_item_create_params import (
        InvoiceItemCreateParams,
    )
    from stripe.params._invoice_item_delete_params import (
        InvoiceItemDeleteParams,
    )
    from stripe.params._invoice_item_list_params import InvoiceItemListParams
    from stripe.params._invoice_item_retrieve_params import (
        InvoiceItemRetrieveParams,
    )
    from stripe.params._invoice_item_update_params import (
        InvoiceItemUpdateParams,
    )


class InvoiceItemService(StripeService):
    def delete(
        self,
        invoiceitem: str,
        params: Optional["InvoiceItemDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InvoiceItem":
        """
        Deletes an invoice item, removing it from an invoice. Deleting invoice items is only possible when they're not attached to invoices, or if it's attached to a draft invoice.
        """
        return cast(
            "InvoiceItem",
            self._request(
                "delete",
                "/v1/invoiceitems/{invoiceitem}".format(
                    invoiceitem=sanitize_id(invoiceitem),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def delete_async(
        self,
        invoiceitem: str,
        params: Optional["InvoiceItemDeleteParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InvoiceItem":
        """
        Deletes an invoice item, removing it from an invoice. Deleting invoice items is only possible when they're not attached to invoices, or if it's attached to a draft invoice.
        """
        return cast(
            "InvoiceItem",
            await self._request_async(
                "delete",
                "/v1/invoiceitems/{invoiceitem}".format(
                    invoiceitem=sanitize_id(invoiceitem),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        invoiceitem: str,
        params: Optional["InvoiceItemRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InvoiceItem":
        """
        Retrieves the invoice item with the given ID.
        """
        return cast(
            "InvoiceItem",
            self._request(
                "get",
                "/v1/invoiceitems/{invoiceitem}".format(
                    invoiceitem=sanitize_id(invoiceitem),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        invoiceitem: str,
        params: Optional["InvoiceItemRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InvoiceItem":
        """
        Retrieves the invoice item with the given ID.
        """
        return cast(
            "InvoiceItem",
            await self._request_async(
                "get",
                "/v1/invoiceitems/{invoiceitem}".format(
                    invoiceitem=sanitize_id(invoiceitem),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        invoiceitem: str,
        params: Optional["InvoiceItemUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InvoiceItem":
        """
        Updates the amount or description of an invoice item on an upcoming invoice. Updating an invoice item is only possible before the invoice it's attached to is closed.
        """
        return cast(
            "InvoiceItem",
            self._request(
                "post",
                "/v1/invoiceitems/{invoiceitem}".format(
                    invoiceitem=sanitize_id(invoiceitem),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        invoiceitem: str,
        params: Optional["InvoiceItemUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InvoiceItem":
        """
        Updates the amount or description of an invoice item on an upcoming invoice. Updating an invoice item is only possible before the invoice it's attached to is closed.
        """
        return cast(
            "InvoiceItem",
            await self._request_async(
                "post",
                "/v1/invoiceitems/{invoiceitem}".format(
                    invoiceitem=sanitize_id(invoiceitem),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        params: Optional["InvoiceItemListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[InvoiceItem]":
        """
        Returns a list of your invoice items. Invoice items are returned sorted by creation date, with the most recently created invoice items appearing first.
        """
        return cast(
            "ListObject[InvoiceItem]",
            self._request(
                "get",
                "/v1/invoiceitems",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["InvoiceItemListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[InvoiceItem]":
        """
        Returns a list of your invoice items. Invoice items are returned sorted by creation date, with the most recently created invoice items appearing first.
        """
        return cast(
            "ListObject[InvoiceItem]",
            await self._request_async(
                "get",
                "/v1/invoiceitems",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["InvoiceItemCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InvoiceItem":
        """
        Creates an item to be added to a draft invoice (up to 250 items per invoice). If no invoice is specified, the item will be on the next invoice created for the customer specified.
        """
        return cast(
            "InvoiceItem",
            self._request(
                "post",
                "/v1/invoiceitems",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["InvoiceItemCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InvoiceItem":
        """
        Creates an item to be added to a draft invoice (up to 250 items per invoice). If no invoice is specified, the item will be on the next invoice created for the customer specified.
        """
        return cast(
            "InvoiceItem",
            await self._request_async(
                "post",
                "/v1/invoiceitems",
                base_address="api",
                params=params,
                options=options,
            ),
        )
