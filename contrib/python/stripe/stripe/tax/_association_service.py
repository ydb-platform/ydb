# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.tax._association_find_params import (
        AssociationFindParams,
    )
    from stripe.tax._association import Association


class AssociationService(StripeService):
    def find(
        self,
        params: "AssociationFindParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Association":
        """
        Finds a tax association object by PaymentIntent id.
        """
        return cast(
            "Association",
            self._request(
                "get",
                "/v1/tax/associations/find",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def find_async(
        self,
        params: "AssociationFindParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Association":
        """
        Finds a tax association object by PaymentIntent id.
        """
        return cast(
            "Association",
            await self._request_async(
                "get",
                "/v1/tax/associations/find",
                base_address="api",
                params=params,
                options=options,
            ),
        )
