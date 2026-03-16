# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.test_helpers.treasury._received_debit_create_params import (
        ReceivedDebitCreateParams,
    )
    from stripe.treasury._received_debit import ReceivedDebit


class ReceivedDebitService(StripeService):
    def create(
        self,
        params: "ReceivedDebitCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ReceivedDebit":
        """
        Use this endpoint to simulate a test mode ReceivedDebit initiated by a third party. In live mode, you can't directly create ReceivedDebits initiated by third parties.
        """
        return cast(
            "ReceivedDebit",
            self._request(
                "post",
                "/v1/test_helpers/treasury/received_debits",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "ReceivedDebitCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ReceivedDebit":
        """
        Use this endpoint to simulate a test mode ReceivedDebit initiated by a third party. In live mode, you can't directly create ReceivedDebits initiated by third parties.
        """
        return cast(
            "ReceivedDebit",
            await self._request_async(
                "post",
                "/v1/test_helpers/treasury/received_debits",
                base_address="api",
                params=params,
                options=options,
            ),
        )
