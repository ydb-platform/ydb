# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.tax._transaction_line_item_list_params import (
        TransactionLineItemListParams,
    )
    from stripe.tax._transaction_line_item import TransactionLineItem


class TransactionLineItemService(StripeService):
    def list(
        self,
        transaction: str,
        params: Optional["TransactionLineItemListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[TransactionLineItem]":
        """
        Retrieves the line items of a committed standalone transaction as a collection.
        """
        return cast(
            "ListObject[TransactionLineItem]",
            self._request(
                "get",
                "/v1/tax/transactions/{transaction}/line_items".format(
                    transaction=sanitize_id(transaction),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        transaction: str,
        params: Optional["TransactionLineItemListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[TransactionLineItem]":
        """
        Retrieves the line items of a committed standalone transaction as a collection.
        """
        return cast(
            "ListObject[TransactionLineItem]",
            await self._request_async(
                "get",
                "/v1/tax/transactions/{transaction}/line_items".format(
                    transaction=sanitize_id(transaction),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
