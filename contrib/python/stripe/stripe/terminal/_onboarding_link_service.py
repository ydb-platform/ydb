# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.params.terminal._onboarding_link_create_params import (
        OnboardingLinkCreateParams,
    )
    from stripe.terminal._onboarding_link import OnboardingLink


class OnboardingLinkService(StripeService):
    def create(
        self,
        params: "OnboardingLinkCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "OnboardingLink":
        """
        Creates a new OnboardingLink object that contains a redirect_url used for onboarding onto Tap to Pay on iPhone.
        """
        return cast(
            "OnboardingLink",
            self._request(
                "post",
                "/v1/terminal/onboarding_links",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "OnboardingLinkCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "OnboardingLink":
        """
        Creates a new OnboardingLink object that contains a redirect_url used for onboarding onto Tap to Pay on iPhone.
        """
        return cast(
            "OnboardingLink",
            await self._request_async(
                "post",
                "/v1/terminal/onboarding_links",
                base_address="api",
                params=params,
                options=options,
            ),
        )
