# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._invoice_line_item import InvoiceLineItem
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._invoice_line_item_list_params import (
        InvoiceLineItemListParams,
    )
    from stripe.params._invoice_line_item_update_params import (
        InvoiceLineItemUpdateParams,
    )


class InvoiceLineItemService(StripeService):
    def list(
        self,
        invoice: str,
        params: Optional["InvoiceLineItemListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[InvoiceLineItem]":
        """
        When retrieving an invoice, you'll get a lines property containing the total count of line items and the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            "ListObject[InvoiceLineItem]",
            self._request(
                "get",
                "/v1/invoices/{invoice}/lines".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        invoice: str,
        params: Optional["InvoiceLineItemListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[InvoiceLineItem]":
        """
        When retrieving an invoice, you'll get a lines property containing the total count of line items and the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            "ListObject[InvoiceLineItem]",
            await self._request_async(
                "get",
                "/v1/invoices/{invoice}/lines".format(
                    invoice=sanitize_id(invoice),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        invoice: str,
        line_item_id: str,
        params: Optional["InvoiceLineItemUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InvoiceLineItem":
        """
        Updates an invoice's line item. Some fields, such as tax_amounts, only live on the invoice line item,
        so they can only be updated through this endpoint. Other fields, such as amount, live on both the invoice
        item and the invoice line item, so updates on this endpoint will propagate to the invoice item as well.
        Updating an invoice's line item is only possible before the invoice is finalized.
        """
        return cast(
            "InvoiceLineItem",
            self._request(
                "post",
                "/v1/invoices/{invoice}/lines/{line_item_id}".format(
                    invoice=sanitize_id(invoice),
                    line_item_id=sanitize_id(line_item_id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        invoice: str,
        line_item_id: str,
        params: Optional["InvoiceLineItemUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "InvoiceLineItem":
        """
        Updates an invoice's line item. Some fields, such as tax_amounts, only live on the invoice line item,
        so they can only be updated through this endpoint. Other fields, such as amount, live on both the invoice
        item and the invoice line item, so updates on this endpoint will propagate to the invoice item as well.
        Updating an invoice's line item is only possible before the invoice is finalized.
        """
        return cast(
            "InvoiceLineItem",
            await self._request_async(
                "post",
                "/v1/invoices/{invoice}/lines/{line_item_id}".format(
                    invoice=sanitize_id(invoice),
                    line_item_id=sanitize_id(line_item_id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
