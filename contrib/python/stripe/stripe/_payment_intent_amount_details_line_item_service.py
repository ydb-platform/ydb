# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._payment_intent_amount_details_line_item import (
        PaymentIntentAmountDetailsLineItem,
    )
    from stripe._request_options import RequestOptions
    from stripe.params._payment_intent_amount_details_line_item_list_params import (
        PaymentIntentAmountDetailsLineItemListParams,
    )


class PaymentIntentAmountDetailsLineItemService(StripeService):
    def list(
        self,
        intent: str,
        params: Optional[
            "PaymentIntentAmountDetailsLineItemListParams"
        ] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PaymentIntentAmountDetailsLineItem]":
        """
        Lists all LineItems of a given PaymentIntent.
        """
        return cast(
            "ListObject[PaymentIntentAmountDetailsLineItem]",
            self._request(
                "get",
                "/v1/payment_intents/{intent}/amount_details_line_items".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        intent: str,
        params: Optional[
            "PaymentIntentAmountDetailsLineItemListParams"
        ] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[PaymentIntentAmountDetailsLineItem]":
        """
        Lists all LineItems of a given PaymentIntent.
        """
        return cast(
            "ListObject[PaymentIntentAmountDetailsLineItem]",
            await self._request_async(
                "get",
                "/v1/payment_intents/{intent}/amount_details_line_items".format(
                    intent=sanitize_id(intent),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
