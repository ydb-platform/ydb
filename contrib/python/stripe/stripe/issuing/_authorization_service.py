# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.issuing._authorization import Authorization
    from stripe.params.issuing._authorization_approve_params import (
        AuthorizationApproveParams,
    )
    from stripe.params.issuing._authorization_decline_params import (
        AuthorizationDeclineParams,
    )
    from stripe.params.issuing._authorization_list_params import (
        AuthorizationListParams,
    )
    from stripe.params.issuing._authorization_retrieve_params import (
        AuthorizationRetrieveParams,
    )
    from stripe.params.issuing._authorization_update_params import (
        AuthorizationUpdateParams,
    )


class AuthorizationService(StripeService):
    def list(
        self,
        params: Optional["AuthorizationListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Authorization]":
        """
        Returns a list of Issuing Authorization objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[Authorization]",
            self._request(
                "get",
                "/v1/issuing/authorizations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["AuthorizationListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Authorization]":
        """
        Returns a list of Issuing Authorization objects. The objects are sorted in descending order by creation date, with the most recently created object appearing first.
        """
        return cast(
            "ListObject[Authorization]",
            await self._request_async(
                "get",
                "/v1/issuing/authorizations",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        authorization: str,
        params: Optional["AuthorizationRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Retrieves an Issuing Authorization object.
        """
        return cast(
            "Authorization",
            self._request(
                "get",
                "/v1/issuing/authorizations/{authorization}".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        authorization: str,
        params: Optional["AuthorizationRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Retrieves an Issuing Authorization object.
        """
        return cast(
            "Authorization",
            await self._request_async(
                "get",
                "/v1/issuing/authorizations/{authorization}".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def update(
        self,
        authorization: str,
        params: Optional["AuthorizationUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Updates the specified Issuing Authorization object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        return cast(
            "Authorization",
            self._request(
                "post",
                "/v1/issuing/authorizations/{authorization}".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def update_async(
        self,
        authorization: str,
        params: Optional["AuthorizationUpdateParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        Updates the specified Issuing Authorization object by setting the values of the parameters passed. Any parameters not provided will be left unchanged.
        """
        return cast(
            "Authorization",
            await self._request_async(
                "post",
                "/v1/issuing/authorizations/{authorization}".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def approve(
        self,
        authorization: str,
        params: Optional["AuthorizationApproveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        [Deprecated] Approves a pending Issuing Authorization object. This request should be made within the timeout window of the [real-time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to approve an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        return cast(
            "Authorization",
            self._request(
                "post",
                "/v1/issuing/authorizations/{authorization}/approve".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def approve_async(
        self,
        authorization: str,
        params: Optional["AuthorizationApproveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        [Deprecated] Approves a pending Issuing Authorization object. This request should be made within the timeout window of the [real-time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to approve an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        return cast(
            "Authorization",
            await self._request_async(
                "post",
                "/v1/issuing/authorizations/{authorization}/approve".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def decline(
        self,
        authorization: str,
        params: Optional["AuthorizationDeclineParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        [Deprecated] Declines a pending Issuing Authorization object. This request should be made within the timeout window of the [real time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to decline an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        return cast(
            "Authorization",
            self._request(
                "post",
                "/v1/issuing/authorizations/{authorization}/decline".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def decline_async(
        self,
        authorization: str,
        params: Optional["AuthorizationDeclineParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Authorization":
        """
        [Deprecated] Declines a pending Issuing Authorization object. This request should be made within the timeout window of the [real time authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations) flow.
        This method is deprecated. Instead, [respond directly to the webhook request to decline an authorization](https://docs.stripe.com/docs/issuing/controls/real-time-authorizations#authorization-handling).
        """
        return cast(
            "Authorization",
            await self._request_async(
                "post",
                "/v1/issuing/authorizations/{authorization}/decline".format(
                    authorization=sanitize_id(authorization),
                ),
                base_address="api",
                params=params,
                options=options,
            ),
        )
