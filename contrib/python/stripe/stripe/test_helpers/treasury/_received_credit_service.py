# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.test_helpers.treasury._received_credit_create_params import (
        ReceivedCreditCreateParams,
    )
    from stripe.treasury._received_credit import ReceivedCredit


class ReceivedCreditService(StripeService):
    def create(
        self,
        params: "ReceivedCreditCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ReceivedCredit":
        """
        Use this endpoint to simulate a test mode ReceivedCredit initiated by a third party. In live mode, you can't directly create ReceivedCredits initiated by third parties.
        """
        return cast(
            "ReceivedCredit",
            self._request(
                "post",
                "/v1/test_helpers/treasury/received_credits",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "ReceivedCreditCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ReceivedCredit":
        """
        Use this endpoint to simulate a test mode ReceivedCredit initiated by a third party. In live mode, you can't directly create ReceivedCredits initiated by third parties.
        """
        return cast(
            "ReceivedCredit",
            await self._request_async(
                "post",
                "/v1/test_helpers/treasury/received_credits",
                base_address="api",
                params=params,
                options=options,
            ),
        )
