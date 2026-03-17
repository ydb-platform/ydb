# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._credit_note_line_item import CreditNoteLineItem
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._credit_note_line_item_list_params import (
        CreditNoteLineItemListParams,
    )


class CreditNoteLineItemService(StripeService):
    def list(
        self,
        credit_note: str,
        params: Optional["CreditNoteLineItemListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CreditNoteLineItem]":
        """
        When retrieving a credit note, you'll get a lines property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            "ListObject[CreditNoteLineItem]",
            self._request(
                "get",
                "/v1/credit_notes/{credit_note}/lines".format(
                    credit_note=sanitize_id(credit_note),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        credit_note: str,
        params: Optional["CreditNoteLineItemListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CreditNoteLineItem]":
        """
        When retrieving a credit note, you'll get a lines property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            "ListObject[CreditNoteLineItem]",
            await self._request_async(
                "get",
                "/v1/credit_notes/{credit_note}/lines".format(
                    credit_note=sanitize_id(credit_note),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
