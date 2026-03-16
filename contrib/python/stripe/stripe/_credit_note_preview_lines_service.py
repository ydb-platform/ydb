# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._credit_note_line_item import CreditNoteLineItem
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._credit_note_preview_lines_list_params import (
        CreditNotePreviewLinesListParams,
    )


class CreditNotePreviewLinesService(StripeService):
    def list(
        self,
        params: "CreditNotePreviewLinesListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CreditNoteLineItem]":
        """
        When retrieving a credit note preview, you'll get a lines property containing the first handful of those items. This URL you can retrieve the full (paginated) list of line items.
        """
        return cast(
            "ListObject[CreditNoteLineItem]",
            self._request(
                "get",
                "/v1/credit_notes/preview/lines",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: "CreditNotePreviewLinesListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CreditNoteLineItem]":
        """
        When retrieving a credit note preview, you'll get a lines property containing the first handful of those items. This URL you can retrieve the full (paginated) list of line items.
        """
        return cast(
            "ListObject[CreditNoteLineItem]",
            await self._request_async(
                "get",
                "/v1/credit_notes/preview/lines",
                base_address="api",
                params=params,
                options=options,
            ),
        )
