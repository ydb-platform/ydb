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
    from stripe.params._quote_computed_upfront_line_items_list_params import (
        QuoteComputedUpfrontLineItemsListParams,
    )


class QuoteComputedUpfrontLineItemsService(StripeService):
    def list(
        self,
        quote: str,
        params: Optional["QuoteComputedUpfrontLineItemsListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[LineItem]":
        """
        When retrieving a quote, there is an includable [computed.upfront.line_items](https://stripe.com/docs/api/quotes/object#quote_object-computed-upfront-line_items) property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of upfront line items.
        """
        return cast(
            "ListObject[LineItem]",
            self._request(
                "get",
                "/v1/quotes/{quote}/computed_upfront_line_items".format(
                    quote=sanitize_id(quote),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        quote: str,
        params: Optional["QuoteComputedUpfrontLineItemsListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[LineItem]":
        """
        When retrieving a quote, there is an includable [computed.upfront.line_items](https://stripe.com/docs/api/quotes/object#quote_object-computed-upfront-line_items) property containing the first handful of those items. There is also a URL where you can retrieve the full (paginated) list of upfront line items.
        """
        return cast(
            "ListObject[LineItem]",
            await self._request_async(
                "get",
                "/v1/quotes/{quote}/computed_upfront_line_items".format(
                    quote=sanitize_id(quote),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
