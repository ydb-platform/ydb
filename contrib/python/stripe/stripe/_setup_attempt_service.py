# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe._setup_attempt import SetupAttempt
    from stripe.params._setup_attempt_list_params import SetupAttemptListParams


class SetupAttemptService(StripeService):
    def list(
        self,
        params: "SetupAttemptListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[SetupAttempt]":
        """
        Returns a list of SetupAttempts that associate with a provided SetupIntent.
        """
        return cast(
            "ListObject[SetupAttempt]",
            self._request(
                "get",
                "/v1/setup_attempts",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: "SetupAttemptListParams",
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[SetupAttempt]":
        """
        Returns a list of SetupAttempts that associate with a provided SetupIntent.
        """
        return cast(
            "ListObject[SetupAttempt]",
            await self._request_async(
                "get",
                "/v1/setup_attempts",
                base_address="api",
                params=params,
                options=options,
            ),
        )
