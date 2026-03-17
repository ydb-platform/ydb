# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._topup import Topup
    from stripe.params._topup_cancel_params import TopupCancelParams
    from stripe.params._topup_create_params import TopupCreateParams
    from stripe.params._topup_list_params import TopupListParams
    from stripe.params._topup_retrieve_params import TopupRetrieveParams
    from stripe.params._topup_update_params import TopupUpdateParams


class TopupService(StripeService):
    def list(
        self,
        params: Optional["TopupListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Topup]":
        """
        Returns a list of top-ups.
        """
        return cast(
            "ListObject[Topup]",
            self._request(
                "get",
                "/v1/topups",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["TopupListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Topup]":
        """
        Returns a list of top-ups.
        """
        return cast(
            "ListObject[Topup]",
            await self._request_async(
                "get",
                "/v1/topups",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "TopupCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Topup":
        """
        Top up the balance of an account
        """
        return cast(
            "Topup",
            self._request(
                "post",
                "/v1/topups",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "TopupCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Topup":
        """
        Top up the balance of an account
        """
        return cast(
            "Topup",
            await self._request_async(
                "post",
                "/v1/topups",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        topup: str,
        params: Optional["TopupRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Topup":
        """
        Retrieves the details of a top-up that has previously been created. Supply the unique top-up ID that was returned from your previous request, and Stripe will return the corresponding top-up information.
        """
        return cast(
            "Topup",
            self._request(
                "get",
                "/v1/topups/{topup}".format(topup=sanitize_id(topup)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        topup: str,
        params: Optional["TopupRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Topup":
        """
        Retrieves the details of a top-up that has previously been created. Supply the unique top-up ID that was returned from your previous request, and Stripe will return the corresponding top-up information.
        """
        return cast(
            "Topup",
            await self._request_async(
                "get",
                "/v1/topups/{topup}".format(topup=sanitize_id(topup)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        topup: str,
        params: Optional["TopupUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Topup":
        """
        Updates the metadata of a top-up. Other top-up details are not editable by design.
        """
        return cast(
            "Topup",
            self._request(
                "post",
                "/v1/topups/{topup}".format(topup=sanitize_id(topup)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        topup: str,
        params: Optional["TopupUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Topup":
        """
        Updates the metadata of a top-up. Other top-up details are not editable by design.
        """
        return cast(
            "Topup",
            await self._request_async(
                "post",
                "/v1/topups/{topup}".format(topup=sanitize_id(topup)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def cancel(
        self,
        topup: str,
        params: Optional["TopupCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Topup":
        """
        Cancels a top-up. Only pending top-ups can be canceled.
        """
        return cast(
            "Topup",
            self._request(
                "post",
                "/v1/topups/{topup}/cancel".format(topup=sanitize_id(topup)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def cancel_async(
        self,
        topup: str,
        params: Optional["TopupCancelParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Topup":
        """
        Cancels a top-up. Only pending top-ups can be canceled.
        """
        return cast(
            "Topup",
            await self._request_async(
                "post",
                "/v1/topups/{topup}/cancel".format(topup=sanitize_id(topup)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
