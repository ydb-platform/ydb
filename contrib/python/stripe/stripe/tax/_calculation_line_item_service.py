# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params.tax._calculation_line_item_list_params import (
        CalculationLineItemListParams,
    )
    from stripe.tax._calculation_line_item import CalculationLineItem


class CalculationLineItemService(StripeService):
    def list(
        self,
        calculation: str,
        params: Optional["CalculationLineItemListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CalculationLineItem]":
        """
        Retrieves the line items of a tax calculation as a collection, if the calculation hasn't expired.
        """
        return cast(
            "ListObject[CalculationLineItem]",
            self._request(
                "get",
                "/v1/tax/calculations/{calculation}/line_items".format(
                    calculation=sanitize_id(calculation),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        calculation: str,
        params: Optional["CalculationLineItemListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[CalculationLineItem]":
        """
        Retrieves the line items of a tax calculation as a collection, if the calculation hasn't expired.
        """
        return cast(
            "ListObject[CalculationLineItem]",
            await self._request_async(
                "get",
                "/v1/tax/calculations/{calculation}/line_items".format(
                    calculation=sanitize_id(calculation),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
