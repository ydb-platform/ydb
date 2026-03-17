# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._dispute import Dispute
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.params._dispute_close_params import DisputeCloseParams
    from stripe.params._dispute_list_params import DisputeListParams
    from stripe.params._dispute_retrieve_params import DisputeRetrieveParams
    from stripe.params._dispute_update_params import DisputeUpdateParams


class DisputeService(StripeService):
    def list(
        self,
        params: Optional["DisputeListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Dispute]":
        """
        Returns a list of your disputes.
        """
        return cast(
            "ListObject[Dispute]",
            self._request(
                "get",
                "/v1/disputes",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["DisputeListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Dispute]":
        """
        Returns a list of your disputes.
        """
        return cast(
            "ListObject[Dispute]",
            await self._request_async(
                "get",
                "/v1/disputes",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        dispute: str,
        params: Optional["DisputeRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Dispute":
        """
        Retrieves the dispute with the given ID.
        """
        return cast(
            "Dispute",
            self._request(
                "get",
                "/v1/disputes/{dispute}".format(dispute=sanitize_id(dispute)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        dispute: str,
        params: Optional["DisputeRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Dispute":
        """
        Retrieves the dispute with the given ID.
        """
        return cast(
            "Dispute",
            await self._request_async(
                "get",
                "/v1/disputes/{dispute}".format(dispute=sanitize_id(dispute)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        dispute: str,
        params: Optional["DisputeUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Dispute":
        """
        When you get a dispute, contacting your customer is always the best first step. If that doesn't work, you can submit evidence to help us resolve the dispute in your favor. You can do this in your [dashboard](https://dashboard.stripe.com/disputes), but if you prefer, you can use the API to submit evidence programmatically.

        Depending on your dispute type, different evidence fields will give you a better chance of winning your dispute. To figure out which evidence fields to provide, see our [guide to dispute types](https://docs.stripe.com/docs/disputes/categories).
        """
        return cast(
            "Dispute",
            self._request(
                "post",
                "/v1/disputes/{dispute}".format(dispute=sanitize_id(dispute)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        dispute: str,
        params: Optional["DisputeUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Dispute":
        """
        When you get a dispute, contacting your customer is always the best first step. If that doesn't work, you can submit evidence to help us resolve the dispute in your favor. You can do this in your [dashboard](https://dashboard.stripe.com/disputes), but if you prefer, you can use the API to submit evidence programmatically.

        Depending on your dispute type, different evidence fields will give you a better chance of winning your dispute. To figure out which evidence fields to provide, see our [guide to dispute types](https://docs.stripe.com/docs/disputes/categories).
        """
        return cast(
            "Dispute",
            await self._request_async(
                "post",
                "/v1/disputes/{dispute}".format(dispute=sanitize_id(dispute)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def close(
        self,
        dispute: str,
        params: Optional["DisputeCloseParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Dispute":
        """
        Closing the dispute for a charge indicates that you do not have any evidence to submit and are essentially dismissing the dispute, acknowledging it as lost.

        The status of the dispute will change from needs_response to lost. Closing a dispute is irreversible.
        """
        return cast(
            "Dispute",
            self._request(
                "post",
                "/v1/disputes/{dispute}/close".format(
                    dispute=sanitize_id(dispute),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def close_async(
        self,
        dispute: str,
        params: Optional["DisputeCloseParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Dispute":
        """
        Closing the dispute for a charge indicates that you do not have any evidence to submit and are essentially dismissing the dispute, acknowledging it as lost.

        The status of the dispute will change from needs_response to lost. Closing a dispute is irreversible.
        """
        return cast(
            "Dispute",
            await self._request_async(
                "post",
                "/v1/disputes/{dispute}/close".format(
                    dispute=sanitize_id(dispute),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
