# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._line_item import LineItem
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._payment_link_line_item_list_params import (
        PaymentLinkLineItemListParams,
    )


class PaymentLinkLineItemService(StripeService):
    def list(
        self,
        payment_link: str,
        params: Optional["PaymentLinkLineItemListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[LineItem]":
        """
        When retrieving a payment link, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            "ListObject[LineItem]",
            self._request(
                "get",
                "/v1/payment_links/{payment_link}/line_items".format(
                    payment_link=sanitize_id(payment_link),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        payment_link: str,
        params: Optional["PaymentLinkLineItemListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[LineItem]":
        """
        When retrieving a payment link, there is an includable line_items property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of line items.
        """
        return cast(
            "ListObject[LineItem]",
            await self._request_async(
                "get",
                "/v1/payment_links/{payment_link}/line_items".format(
                    payment_link=sanitize_id(payment_link),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
