# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._application_fee_refund import ApplicationFeeRefund
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._application_fee_refund_create_params import (
        ApplicationFeeRefundCreateParams,
    )
    from stripe.params._application_fee_refund_list_params import (
        ApplicationFeeRefundListParams,
    )
    from stripe.params._application_fee_refund_retrieve_params import (
        ApplicationFeeRefundRetrieveParams,
    )
    from stripe.params._application_fee_refund_update_params import (
        ApplicationFeeRefundUpdateParams,
    )


class ApplicationFeeRefundService(StripeService):
    def retrieve(
        self,
        fee: str,
        id: str,
        params: Optional["ApplicationFeeRefundRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ApplicationFeeRefund":
        """
        By default, you can see the 10 most recent refunds stored directly on the application fee object, but you can also retrieve details about a specific refund stored on the application fee.
        """
        return cast(
            "ApplicationFeeRefund",
            self._request(
                "get",
                "/v1/application_fees/{fee}/refunds/{id}".format(
                    fee=sanitize_id(fee),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        fee: str,
        id: str,
        params: Optional["ApplicationFeeRefundRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ApplicationFeeRefund":
        """
        By default, you can see the 10 most recent refunds stored directly on the application fee object, but you can also retrieve details about a specific refund stored on the application fee.
        """
        return cast(
            "ApplicationFeeRefund",
            await self._request_async(
                "get",
                "/v1/application_fees/{fee}/refunds/{id}".format(
                    fee=sanitize_id(fee),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        fee: str,
        id: str,
        params: Optional["ApplicationFeeRefundUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ApplicationFeeRefund":
        """
        Updates the specified application fee refund by setting the values of the parameters passed. Any parameters not provided will be left unchanged.

        This request only accepts metadata as an argument.
        """
        return cast(
            "ApplicationFeeRefund",
            self._request(
                "post",
                "/v1/application_fees/{fee}/refunds/{id}".format(
                    fee=sanitize_id(fee),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        fee: str,
        id: str,
        params: Optional["ApplicationFeeRefundUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ApplicationFeeRefund":
        """
        Updates the specified application fee refund by setting the values of the parameters passed. Any parameters not provided will be left unchanged.

        This request only accepts metadata as an argument.
        """
        return cast(
            "ApplicationFeeRefund",
            await self._request_async(
                "post",
                "/v1/application_fees/{fee}/refunds/{id}".format(
                    fee=sanitize_id(fee),
                    id=sanitize_id(id),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def list(
        self,
        id: str,
        params: Optional["ApplicationFeeRefundListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ApplicationFeeRefund]":
        """
        You can see a list of the refunds belonging to a specific application fee. Note that the 10 most recent refunds are always available by default on the application fee object. If you need more than those 10, you can use this API method and the limit and starting_after parameters to page through additional refunds.
        """
        return cast(
            "ListObject[ApplicationFeeRefund]",
            self._request(
                "get",
                "/v1/application_fees/{id}/refunds".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        id: str,
        params: Optional["ApplicationFeeRefundListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[ApplicationFeeRefund]":
        """
        You can see a list of the refunds belonging to a specific application fee. Note that the 10 most recent refunds are always available by default on the application fee object. If you need more than those 10, you can use this API method and the limit and starting_after parameters to page through additional refunds.
        """
        return cast(
            "ListObject[ApplicationFeeRefund]",
            await self._request_async(
                "get",
                "/v1/application_fees/{id}/refunds".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        id: str,
        params: Optional["ApplicationFeeRefundCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ApplicationFeeRefund":
        """
        Refunds an application fee that has previously been collected but not yet refunded.
        Funds will be refunded to the Stripe account from which the fee was originally collected.

        You can optionally refund only part of an application fee.
        You can do so multiple times, until the entire fee has been refunded.

        Once entirely refunded, an application fee can't be refunded again.
        This method will raise an error when called on an already-refunded application fee,
        or when trying to refund more money than is left on an application fee.
        """
        return cast(
            "ApplicationFeeRefund",
            self._request(
                "post",
                "/v1/application_fees/{id}/refunds".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        id: str,
        params: Optional["ApplicationFeeRefundCreateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ApplicationFeeRefund":
        """
        Refunds an application fee that has previously been collected but not yet refunded.
        Funds will be refunded to the Stripe account from which the fee was originally collected.

        You can optionally refund only part of an application fee.
        You can do so multiple times, until the entire fee has been refunded.

        Once entirely refunded, an application fee can't be refunded again.
        This method will raise an error when called on an already-refunded application fee,
        or when trying to refund more money than is left on an application fee.
        """
        return cast(
            "ApplicationFeeRefund",
            await self._request_async(
                "post",
                "/v1/application_fees/{id}/refunds".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
