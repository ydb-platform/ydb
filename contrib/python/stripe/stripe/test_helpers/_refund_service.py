# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._refund import Refund
    from stripe._request_options import RequestOptions
    from stripe.params.test_helpers._refund_expire_params import (
        RefundExpireParams,
    )


class RefundService(StripeService):
    def expire(
        self,
        refund: str,
        params: Optional["RefundExpireParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Refund":
        """
        Expire a refund with a status of requires_action.
        """
        return cast(
            "Refund",
            self._request(
                "post",
                "/v1/test_helpers/refunds/{refund}/expire".format(
                    refund=sanitize_id(refund),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def expire_async(
        self,
        refund: str,
        params: Optional["RefundExpireParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Refund":
        """
        Expire a refund with a status of requires_action.
        """
        return cast(
            "Refund",
            await self._request_async(
                "post",
                "/v1/test_helpers/refunds/{refund}/expire".format(
                    refund=sanitize_id(refund),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
