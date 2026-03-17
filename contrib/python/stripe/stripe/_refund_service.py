# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._refund import Refund
    from stripe._request_options import RequestOptions
    from stripe.params._refund_cancel_params import RefundCancelParams
    from stripe.params._refund_create_params import RefundCreateParams
    from stripe.params._refund_list_params import RefundListParams
    from stripe.params._refund_retrieve_params import RefundRetrieveParams
    from stripe.params._refund_update_params import RefundUpdateParams


class RefundService(StripeService):
    def list(
        self,
        params: Optional["RefundListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Refund]":
        """
        Returns a list of all refunds you created. We return the refunds in sorted order, with the most recent refunds appearing first. The 10 most recent refunds are always available by default on the Charge object.
        """
        return cast(
            "ListObject[Refund]",
            self._request(
                "get",
                "/v1/refunds",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["RefundListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Refund]":
        """
        Returns a list of all refunds you created. We return the refunds in sorted order, with the most recent refunds appearing first. The 10 most recent refunds are always available by default on the Charge object.
        """
        return cast(
            "ListObject[Refund]",
            await self._request_async(
                "get",
                "/v1/refunds",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: Optional["RefundCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Refund":
        """
        When you create a new refund, you must specify a Charge or a PaymentIntent object on which to create it.

        Creating a new refund will refund a charge that has previously been created but not yet refunded.
        Funds will be refunded to the credit or debit card that was originally charged.

        You can optionally refund only part of a charge.
        You can do so multiple times, until the entire charge has been refunded.

        Once entirely refunded, a charge can't be refunded again.
        This method will raise an error when called on an already-refunded charge,
        or when trying to refund more money than is left on a charge.
        """
        return cast(
            "Refund",
            self._request(
                "post",
                "/v1/refunds",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: Optional["RefundCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Refund":
        """
        When you create a new refund, you must specify a Charge or a PaymentIntent object on which to create it.

        Creating a new refund will refund a charge that has previously been created but not yet refunded.
        Funds will be refunded to the credit or debit card that was originally charged.

        You can optionally refund only part of a charge.
        You can do so multiple times, until the entire charge has been refunded.

        Once entirely refunded, a charge can't be refunded again.
        This method will raise an error when called on an already-refunded charge,
        or when trying to refund more money than is left on a charge.
        """
        return cast(
            "Refund",
            await self._request_async(
                "post",
                "/v1/refunds",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        refund: str,
        params: Optional["RefundRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Refund":
        """
        Retrieves the details of an existing refund.
        """
        return cast(
            "Refund",
            self._request(
                "get",
                "/v1/refunds/{refund}".format(refund=sanitize_id(refund)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        refund: str,
        params: Optional["RefundRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Refund":
        """
        Retrieves the details of an existing refund.
        """
        return cast(
            "Refund",
            await self._request_async(
                "get",
                "/v1/refunds/{refund}".format(refund=sanitize_id(refund)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        refund: str,
        params: Optional["RefundUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Refund":
        """
        Updates the refund that you specify by setting the values of the passed parameters. Any parameters that you don't provide remain unchanged.

        This request only accepts metadata as an argument.
        """
        return cast(
            "Refund",
            self._request(
                "post",
                "/v1/refunds/{refund}".format(refund=sanitize_id(refund)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        refund: str,
        params: Optional["RefundUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Refund":
        """
        Updates the refund that you specify by setting the values of the passed parameters. Any parameters that you don't provide remain unchanged.

        This request only accepts metadata as an argument.
        """
        return cast(
            "Refund",
            await self._request_async(
                "post",
                "/v1/refunds/{refund}".format(refund=sanitize_id(refund)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def cancel(
        self,
        refund: str,
        params: Optional["RefundCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Refund":
        """
        Cancels a refund with a status of requires_action.

        You can't cancel refunds in other states. Only refunds for payment methods that require customer action can enter the requires_action state.
        """
        return cast(
            "Refund",
            self._request(
                "post",
                "/v1/refunds/{refund}/cancel".format(
                    refund=sanitize_id(refund),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def cancel_async(
        self,
        refund: str,
        params: Optional["RefundCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Refund":
        """
        Cancels a refund with a status of requires_action.

        You can't cancel refunds in other states. Only refunds for payment methods that require customer action can enter the requires_action state.
        """
        return cast(
            "Refund",
            await self._request_async(
                "post",
                "/v1/refunds/{refund}/cancel".format(
                    refund=sanitize_id(refund),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
