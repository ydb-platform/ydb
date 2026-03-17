from stripe._api_requestor import (
    _APIRequestor,
)
from stripe._stripe_response import (
    StripeStreamResponse,
    StripeStreamResponseAsync,
)
from stripe._stripe_object import StripeObject
from stripe._request_options import RequestOptions
from stripe._base_address import BaseAddress

from typing import Any, Mapping, Optional


class StripeService(object):
    _requestor: _APIRequestor

    def __init__(self, requestor):
        self._requestor = requestor

    def _request(
        self,
        method: str,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        options: Optional[RequestOptions] = None,
        *,
        base_address: BaseAddress,
    ) -> StripeObject:
        return self._requestor.request(
            method,
            url,
            params,
            options,
            base_address=base_address,
            usage=["stripe_client"],
        )

    async def _request_async(
        self,
        method: str,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        options: Optional[RequestOptions] = None,
        *,
        base_address: BaseAddress,
    ) -> StripeObject:
        return await self._requestor.request_async(
            method,
            url,
            params,
            options,
            base_address=base_address,
            usage=["stripe_client"],
        )

    def _request_stream(
        self,
        method: str,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        options: Optional[RequestOptions] = None,
        *,
        base_address: BaseAddress,
    ) -> StripeStreamResponse:
        return self._requestor.request_stream(
            method,
            url,
            params,
            options,
            base_address=base_address,
            usage=["stripe_client"],
        )

    async def _request_stream_async(
        self,
        method: str,
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        options: Optional[RequestOptions] = None,
        *,
        base_address: BaseAddress,
    ) -> StripeStreamResponseAsync:
        return await self._requestor.request_stream_async(
            method,
            url,
            params,
            options,
            base_address=base_address,
            usage=["stripe_client"],
        )
