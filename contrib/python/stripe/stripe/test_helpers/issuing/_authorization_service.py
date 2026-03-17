# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._request_options import RequestOptions
    from stripe.issuing._authorization import Authorization
    from stripe.params.test_helpers.issuing._authorization_capture_params import (
        AuthorizationCaptureParams,
    )
    from stripe.params.test_helpers.issuing._authorization_create_params import (
        AuthorizationCreateParams,
    )
    from stripe.params.test_helpers.issuing._authorization_expire_params import (
        AuthorizationExpireParams,
    )
    from stripe.params.test_helpers.issuing._authorization_finalize_amount_params import (
        AuthorizationFinalizeAmountParams,
    )
    from stripe.params.test_helpers.issuing._authorization_increment_params import (
        AuthorizationIncrementParams,
    )
    from stripe.params.test_helpers.issuing._authorization_respond_params import (
        AuthorizationRespondParams,
    )
    from stripe.params.test_helpers.issuing._authorization_reverse_params import (
        AuthorizationReverseParams,
    )


class AuthorizationService(StripeService):
    def create(
        self,
        params: "AuthorizationCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Create a test-mode authorization.
        """
        return cast(
            "Authorization",
            self._request(
                "post",
                "/v1/test_helpers/issuing/authorizations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "AuthorizationCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Create a test-mode authorization.
        """
        return cast(
            "Authorization",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/authorizations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def capture(
        self,
        authorization: str,
        params: Optional["AuthorizationCaptureParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Capture a test-mode authorization.
        """
        return cast(
            "Authorization",
            self._request(
                "post",
                "/v1/test_helpers/issuing/authorizations/{authorization}/capture".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def capture_async(
        self,
        authorization: str,
        params: Optional["AuthorizationCaptureParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Capture a test-mode authorization.
        """
        return cast(
            "Authorization",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/authorizations/{authorization}/capture".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def expire(
        self,
        authorization: str,
        params: Optional["AuthorizationExpireParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Expire a test-mode Authorization.
        """
        return cast(
            "Authorization",
            self._request(
                "post",
                "/v1/test_helpers/issuing/authorizations/{authorization}/expire".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def expire_async(
        self,
        authorization: str,
        params: Optional["AuthorizationExpireParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Expire a test-mode Authorization.
        """
        return cast(
            "Authorization",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/authorizations/{authorization}/expire".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def finalize_amount(
        self,
        authorization: str,
        params: "AuthorizationFinalizeAmountParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Finalize the amount on an Authorization prior to capture, when the initial authorization was for an estimated amount.
        """
        return cast(
            "Authorization",
            self._request(
                "post",
                "/v1/test_helpers/issuing/authorizations/{authorization}/finalize_amount".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def finalize_amount_async(
        self,
        authorization: str,
        params: "AuthorizationFinalizeAmountParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Finalize the amount on an Authorization prior to capture, when the initial authorization was for an estimated amount.
        """
        return cast(
            "Authorization",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/authorizations/{authorization}/finalize_amount".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def respond(
        self,
        authorization: str,
        params: "AuthorizationRespondParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Respond to a fraud challenge on a testmode Issuing authorization, simulating either a confirmation of fraud or a correction of legitimacy.
        """
        return cast(
            "Authorization",
            self._request(
                "post",
                "/v1/test_helpers/issuing/authorizations/{authorization}/fraud_challenges/respond".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def respond_async(
        self,
        authorization: str,
        params: "AuthorizationRespondParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Respond to a fraud challenge on a testmode Issuing authorization, simulating either a confirmation of fraud or a correction of legitimacy.
        """
        return cast(
            "Authorization",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/authorizations/{authorization}/fraud_challenges/respond".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def increment(
        self,
        authorization: str,
        params: "AuthorizationIncrementParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Increment a test-mode Authorization.
        """
        return cast(
            "Authorization",
            self._request(
                "post",
                "/v1/test_helpers/issuing/authorizations/{authorization}/increment".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def increment_async(
        self,
        authorization: str,
        params: "AuthorizationIncrementParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Increment a test-mode Authorization.
        """
        return cast(
            "Authorization",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/authorizations/{authorization}/increment".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def reverse(
        self,
        authorization: str,
        params: Optional["AuthorizationReverseParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Reverse a test-mode Authorization.
        """
        return cast(
            "Authorization",
            self._request(
                "post",
                "/v1/test_helpers/issuing/authorizations/{authorization}/reverse".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def reverse_async(
        self,
        authorization: str,
        params: Optional["AuthorizationReverseParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Reverse a test-mode Authorization.
        """
        return cast(
            "Authorization",
            await self._request_async(
                "post",
                "/v1/test_helpers/issuing/authorizations/{authorization}/reverse".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
