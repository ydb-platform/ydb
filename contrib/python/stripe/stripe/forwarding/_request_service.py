# -*- coding: utf-8 -*-
# File generated from our OpenAPI spec
from stripe._stripe_service import StripeService
from stripe._util import sanitize_id
from typing import Optional, cast
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._list_object import ListObject
    from stripe._request_options import RequestOptions
    from stripe.forwarding._request import Request
    from stripe.params.forwarding._request_create_params import (
        RequestCreateParams,
    )
    from stripe.params.forwarding._request_list_params import RequestListParams
    from stripe.params.forwarding._request_retrieve_params import (
        RequestRetrieveParams,
    )


class RequestService(StripeService):
    def list(
        self,
        params: Optional["RequestListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Request]":
        """
        Lists all ForwardingRequest objects.
        """
        return cast(
            "ListObject[Request]",
            self._request(
                "get",
                "/v1/forwarding/requests",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def list_async(
        self,
        params: Optional["RequestListParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "ListObject[Request]":
        """
        Lists all ForwardingRequest objects.
        """
        return cast(
            "ListObject[Request]",
            await self._request_async(
                "get",
                "/v1/forwarding/requests",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def create(
        self,
        params: "RequestCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Request":
        """
        Creates a ForwardingRequest object.
        """
        return cast(
            "Request",
            self._request(
                "post",
                "/v1/forwarding/requests",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def create_async(
        self,
        params: "RequestCreateParams",
        options: Optional["RequestOptions"] = None,
    ) -> "Request":
        """
        Creates a ForwardingRequest object.
        """
        return cast(
            "Request",
            await self._request_async(
                "post",
                "/v1/forwarding/requests",
                base_address="api",
                params=params,
                options=options,
            ),
        )

    def retrieve(
        self,
        id: str,
        params: Optional["RequestRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Request":
        """
        Retrieves a ForwardingRequest object.
        """
        return cast(
            "Request",
            self._request(
                "get",
                "/v1/forwarding/requests/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )

    async def retrieve_async(
        self,
        id: str,
        params: Optional["RequestRetrieveParams"] = None,
        options: Optional["RequestOptions"] = None,
    ) -> "Request":
        """
        Retrieves a ForwardingRequest object.
        """
        return cast(
            "Request",
            await self._request_async(
                "get",
                "/v1/forwarding/requests/{id}".format(id=sanitize_id(id)),
                base_address="api",
                params=params,
                options=options,
            ),
        )
