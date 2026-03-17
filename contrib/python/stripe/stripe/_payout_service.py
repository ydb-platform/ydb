# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._payout import Payout
    from stripe._request_options import RequestOptions
    from stripe.params._payout_cancel_params import PayoutCancelParams
    from stripe.params._payout_create_params import PayoutCreateParams
    from stripe.params._payout_list_params import PayoutListParams
    from stripe.params._payout_retrieve_params import PayoutRetrieveParams
    from stripe.params._payout_reverse_params import PayoutReverseParams
    from stripe.params._payout_update_params import PayoutUpdateParams


class PayoutService(StripeService):
    def list(
        self,
        params: Optional["PayoutListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Payout]":
        """
        Returns a list of existing payouts sent to third-party bank accounts or payouts that Stripe sent to you. The payouts return in sorted order, with the most recently created payouts appearing first.
        """
        return cast(
            "ListObject[Payout]",
            self._request(
                "get",
                "/v1/payouts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["PayoutListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Payout]":
        """
        Returns a list of existing payouts sent to third-party bank accounts or payouts that Stripe sent to you. The payouts return in sorted order, with the most recently created payouts appearing first.
        """
        return cast(
            "ListObject[Payout]",
            await self._request_async(
                "get",
                "/v1/payouts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "PayoutCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Payout":
        """
        To send funds to your own bank account, create a new payout object. Your [Stripe balance](https://docs.stripe.com/api#balance) must cover the payout amount. If it doesn't, you receive an “Insufficient Funds” error.

        If your API key is in test mode, money won't actually be sent, though every other action occurs as if you're in live mode.

        If you create a manual payout on a Stripe account that uses multiple payment source types, you need to specify the source type balance that the payout draws from. The [balance object](https://docs.stripe.com/api#balance_object) details available and pending amounts by source type.
        """
        return cast(
            "Payout",
            self._request(
                "post",
                "/v1/payouts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "PayoutCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Payout":
        """
        To send funds to your own bank account, create a new payout object. Your [Stripe balance](https://docs.stripe.com/api#balance) must cover the payout amount. If it doesn't, you receive an “Insufficient Funds” error.

        If your API key is in test mode, money won't actually be sent, though every other action occurs as if you're in live mode.

        If you create a manual payout on a Stripe account that uses multiple payment source types, you need to specify the source type balance that the payout draws from. The [balance object](https://docs.stripe.com/api#balance_object) details available and pending amounts by source type.
        """
        return cast(
            "Payout",
            await self._request_async(
                "post",
                "/v1/payouts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        payout: str,
        params: Optional["PayoutRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Payout":
        """
        Retrieves the details of an existing payout. Supply the unique payout ID from either a payout creation request or the payout list. Stripe returns the corresponding payout information.
        """
        return cast(
            "Payout",
            self._request(
                "get",
                "/v1/payouts/{payout}".format(payout=sanitize_id(payout)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        payout: str,
        params: Optional["PayoutRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Payout":
        """
        Retrieves the details of an existing payout. Supply the unique payout ID from either a payout creation request or the payout list. Stripe returns the corresponding payout information.
        """
        return cast(
            "Payout",
            await self._request_async(
                "get",
                "/v1/payouts/{payout}".format(payout=sanitize_id(payout)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        payout: str,
        params: Optional["PayoutUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Payout":
        """
        Updates the specified payout by setting the values of the parameters you pass. We don't change parameters that you don't provide. This request only accepts the metadata as arguments.
        """
        return cast(
            "Payout",
            self._request(
                "post",
                "/v1/payouts/{payout}".format(payout=sanitize_id(payout)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        payout: str,
        params: Optional["PayoutUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Payout":
        """
        Updates the specified payout by setting the values of the parameters you pass. We don't change parameters that you don't provide. This request only accepts the metadata as arguments.
        """
        return cast(
            "Payout",
            await self._request_async(
                "post",
                "/v1/payouts/{payout}".format(payout=sanitize_id(payout)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def cancel(
        self,
        payout: str,
        params: Optional["PayoutCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Payout":
        """
        You can cancel a previously created payout if its status is pending. Stripe refunds the funds to your available balance. You can't cancel automatic Stripe payouts.
        """
        return cast(
            "Payout",
            self._request(
                "post",
                "/v1/payouts/{payout}/cancel".format(
                    payout=sanitize_id(payout),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def cancel_async(
        self,
        payout: str,
        params: Optional["PayoutCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Payout":
        """
        You can cancel a previously created payout if its status is pending. Stripe refunds the funds to your available balance. You can't cancel automatic Stripe payouts.
        """
        return cast(
            "Payout",
            await self._request_async(
                "post",
                "/v1/payouts/{payout}/cancel".format(
                    payout=sanitize_id(payout),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def reverse(
        self,
        payout: str,
        params: Optional["PayoutReverseParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Payout":
        """
        Reverses a payout by debiting the destination bank account. At this time, you can only reverse payouts for connected accounts to US and Canadian bank accounts. If the payout is manual and in the pending status, use /v1/payouts/:id/cancel instead.

        By requesting a reversal through /v1/payouts/:id/reverse, you confirm that the authorized signatory of the selected bank account authorizes the debit on the bank account and that no other authorization is required.
        """
        return cast(
            "Payout",
            self._request(
                "post",
                "/v1/payouts/{payout}/reverse".format(
                    payout=sanitize_id(payout),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def reverse_async(
        self,
        payout: str,
        params: Optional["PayoutReverseParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Payout":
        """
        Reverses a payout by debiting the destination bank account. At this time, you can only reverse payouts for connected accounts to US and Canadian bank accounts. If the payout is manual and in the pending status, use /v1/payouts/:id/cancel instead.

        By requesting a reversal through /v1/payouts/:id/reverse, you confirm that the authorized signatory of the selected bank account authorizes the debit on the bank account and that no other authorization is required.
        """
        return cast(
            "Payout",
            await self._request_async(
                "post",
                "/v1/payouts/{payout}/reverse".format(
                    payout=sanitize_id(payout),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
