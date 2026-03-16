# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.issuing._card import Card
    from stripe.params.test_helpers.issuing._card_deliver_card_params import (
        CardDeliverCardParams,
    )
    from stripe.params.test_helpers.issuing._card_fail_card_params import (
        CardFailCardParams,
    )
    from stripe.params.test_helpers.issuing._card_return_card_params import (
        CardReturnCardParams,
    )
    from stripe.params.test_helpers.issuing._card_ship_card_params import (
        CardShipCardParams,
    )
    from stripe.params.test_helpers.issuing._card_submit_card_params import (
        CardSubmitCardParams,
    )


class CardService(StripeService):
    def deliver_card(
        self,
        card: str,
        params: Optional["CardDeliverCardParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Card":
        """
        Updates the shipping status of the specified Issuing Card object to delivered.
        """
        return cast(
            "Card",
            self._request(
                "post",
                "/v1/test_helpers/issuing/cards/{card}/shipping/deliver".format(
                    card=sanitize_id(card),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def deliver_card_async(
        self,
        card: str,
        params: Optional["CardDeliverCardParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Card":
        """
        Updates the shipping status of the specified Issuing Card object to delivered.
        """
        return cast(
            "Card",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/cards/{card}/shipping/deliver".format(
                    card=sanitize_id(card),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def fail_card(
        self,
        card: str,
        params: Optional["CardFailCardParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Card":
        """
        Updates the shipping status of the specified Issuing Card object to failure.
        """
        return cast(
            "Card",
            self._request(
                "post",
                "/v1/test_helpers/issuing/cards/{card}/shipping/fail".format(
                    card=sanitize_id(card),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def fail_card_async(
        self,
        card: str,
        params: Optional["CardFailCardParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Card":
        """
        Updates the shipping status of the specified Issuing Card object to failure.
        """
        return cast(
            "Card",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/cards/{card}/shipping/fail".format(
                    card=sanitize_id(card),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def return_card(
        self,
        card: str,
        params: Optional["CardReturnCardParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Card":
        """
        Updates the shipping status of the specified Issuing Card object to returned.
        """
        return cast(
            "Card",
            self._request(
                "post",
                "/v1/test_helpers/issuing/cards/{card}/shipping/return".format(
                    card=sanitize_id(card),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def return_card_async(
        self,
        card: str,
        params: Optional["CardReturnCardParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Card":
        """
        Updates the shipping status of the specified Issuing Card object to returned.
        """
        return cast(
            "Card",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/cards/{card}/shipping/return".format(
                    card=sanitize_id(card),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def ship_card(
        self,
        card: str,
        params: Optional["CardShipCardParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Card":
        """
        Updates the shipping status of the specified Issuing Card object to shipped.
        """
        return cast(
            "Card",
            self._request(
                "post",
                "/v1/test_helpers/issuing/cards/{card}/shipping/ship".format(
                    card=sanitize_id(card),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def ship_card_async(
        self,
        card: str,
        params: Optional["CardShipCardParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Card":
        """
        Updates the shipping status of the specified Issuing Card object to shipped.
        """
        return cast(
            "Card",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/cards/{card}/shipping/ship".format(
                    card=sanitize_id(card),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def submit_card(
        self,
        card: str,
        params: Optional["CardSubmitCardParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Card":
        """
        Updates the shipping status of the specified Issuing Card object to submitted. This method requires Stripe Version ‘2024-09-30.acacia' or later.
        """
        return cast(
            "Card",
            self._request(
                "post",
                "/v1/test_helpers/issuing/cards/{card}/shipping/submit".format(
                    card=sanitize_id(card),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def submit_card_async(
        self,
        card: str,
        params: Optional["CardSubmitCardParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Card":
        """
        Updates the shipping status of the specified Issuing Card object to submitted. This method requires Stripe Version ‘2024-09-30.acacia' or later.
        """
        return cast(
            "Card",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/cards/{card}/shipping/submit".format(
                    card=sanitize_id(card),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
