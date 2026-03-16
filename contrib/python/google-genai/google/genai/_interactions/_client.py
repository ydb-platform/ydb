# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# File generated from our OpenAPI spec by Stainless. See CONTRIBUTING.md for details.

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, Mapping
from typing_extensions import Self, override

import httpx

from . import _exceptions
from ._qs import Querystring
from ._types import (
    Omit,
    Headers,
    Timeout,
    NotGiven,
    Transport,
    ProxiesTypes,
    RequestOptions,
    not_given,
)
from ._utils import is_given, get_async_library
from ._compat import cached_property
from ._models import FinalRequestOptions
from ._version import __version__
from ._streaming import Stream as Stream, AsyncStream as AsyncStream
from ._exceptions import APIStatusError
from ._base_client import (
    DEFAULT_MAX_RETRIES,
    SyncAPIClient,
    AsyncAPIClient,
)
from ._client_adapter import GeminiNextGenAPIClientAdapter, AsyncGeminiNextGenAPIClientAdapter

if TYPE_CHECKING:
    from .resources import interactions
    from .resources.interactions import InteractionsResource, AsyncInteractionsResource

__all__ = [
    "Timeout",
    "Transport",
    "ProxiesTypes",
    "RequestOptions",
    "GeminiNextGenAPIClient",
    "AsyncGeminiNextGenAPIClient",
    "Client",
    "AsyncClient",
]


class GeminiNextGenAPIClient(SyncAPIClient):
    # client options
    api_key: str | None
    api_version: str
    client_adapter: GeminiNextGenAPIClientAdapter | None

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_version: str | None = "v1beta",
        base_url: str | httpx.URL | None = None,
        timeout: float | Timeout | None | NotGiven = not_given,
        max_retries: int = DEFAULT_MAX_RETRIES,
        default_headers: Mapping[str, str] | None = None,
        default_query: Mapping[str, object] | None = None,
        # Configure a custom httpx client.
        # We provide a `DefaultHttpxClient` class that you can pass to retain the default values we use for `limits`, `timeout` & `follow_redirects`.
        # See the [httpx documentation](https://www.python-httpx.org/api/#client) for more details.
        http_client: httpx.Client | None = None,
        client_adapter: GeminiNextGenAPIClientAdapter | None = None,
        # Enable or disable schema validation for data returned by the API.
        # When enabled an error APIResponseValidationError is raised
        # if the API responds with invalid data for the expected schema.
        #
        # This parameter may be removed or changed in the future.
        # If you rely on this feature, please open a GitHub issue
        # outlining your use-case to help us decide if it should be
        # part of our public interface in the future.
        _strict_response_validation: bool = False,
    ) -> None:
        """Construct a new synchronous GeminiNextGenAPIClient client instance.

        This automatically infers the `api_key` argument from the `GEMINI_API_KEY` environment variable if it is not provided.
        """
        if api_key is None:
            api_key = os.environ.get("GEMINI_API_KEY")
        self.api_key = api_key

        if api_version is None:
            api_version = "v1beta"
        self.api_version = api_version

        if base_url is None:
            base_url = os.environ.get("GEMINI_NEXT_GEN_API_BASE_URL")
        if base_url is None:
            base_url = f"https://generativelanguage.googleapis.com"

        self.client_adapter = client_adapter

        super().__init__(
            version=__version__,
            base_url=base_url,
            max_retries=max_retries,
            timeout=timeout,
            http_client=http_client,
            custom_headers=default_headers,
            custom_query=default_query,
            _strict_response_validation=_strict_response_validation,
        )

        self._default_stream_cls = Stream

    @cached_property
    def interactions(self) -> InteractionsResource:
        from .resources.interactions import InteractionsResource

        return InteractionsResource(self)

    @cached_property
    def with_raw_response(self) -> GeminiNextGenAPIClientWithRawResponse:
        return GeminiNextGenAPIClientWithRawResponse(self)

    @cached_property
    def with_streaming_response(self) -> GeminiNextGenAPIClientWithStreamedResponse:
        return GeminiNextGenAPIClientWithStreamedResponse(self)

    @property
    @override
    def qs(self) -> Querystring:
        return Querystring(array_format="comma")

    @property
    @override
    def auth_headers(self) -> dict[str, str]:
        api_key = self.api_key
        if api_key is None:
            return {}
        return {"x-goog-api-key": api_key}

    @property
    @override
    def default_headers(self) -> dict[str, str | Omit]:
        return {
            **super().default_headers,
            "X-Stainless-Async": "false",
            **self._custom_headers,
        }

    @override
    def _validate_headers(self, headers: Headers, custom_headers: Headers) -> None:
        if headers.get("Authorization") or custom_headers.get("Authorization") or isinstance(custom_headers.get("Authorization"), Omit):
            return
        if self.api_key and headers.get("x-goog-api-key"):
            return
        if custom_headers.get("x-goog-api-key") or isinstance(custom_headers.get("x-goog-api-key"), Omit):
            return

        raise TypeError(
            '"Could not resolve authentication method. Expected the api_key to be set. Or for the `x-goog-api-key` headers to be explicitly omitted"'
        )
    
    @override
    def _prepare_options(self, options: FinalRequestOptions) -> FinalRequestOptions:
        if not self.client_adapter or not self.client_adapter.is_vertex_ai():
            return options

        headers = options.headers or {}
        has_auth = headers.get("Authorization") or headers.get("x-goog-api-key")   # pytype: disable=attribute-error
        if has_auth:
            return options

        adapted_headers = self.client_adapter.get_auth_headers()
        if adapted_headers:
            options.headers = {
                **adapted_headers,
                **headers
            }
        return options
    
    def copy(
        self,
        *,
        api_key: str | None = None,
        api_version: str | None = None,
        base_url: str | httpx.URL | None = None,
        timeout: float | Timeout | None | NotGiven = not_given,
        http_client: httpx.Client | None = None,
        max_retries: int | NotGiven = not_given,
        default_headers: Mapping[str, str] | None = None,
        set_default_headers: Mapping[str, str] | None = None,
        default_query: Mapping[str, object] | None = None,
        set_default_query: Mapping[str, object] | None = None,
        client_adapter: GeminiNextGenAPIClientAdapter | None = None,
        _extra_kwargs: Mapping[str, Any] = {},
    ) -> Self:
        """
        Create a new client instance re-using the same options given to the current client with optional overriding.
        """
        if default_headers is not None and set_default_headers is not None:
            raise ValueError("The `default_headers` and `set_default_headers` arguments are mutually exclusive")

        if default_query is not None and set_default_query is not None:
            raise ValueError("The `default_query` and `set_default_query` arguments are mutually exclusive")

        headers = self._custom_headers
        if default_headers is not None:
            headers = {**headers, **default_headers}
        elif set_default_headers is not None:
            headers = set_default_headers

        params = self._custom_query
        if default_query is not None:
            params = {**params, **default_query}
        elif set_default_query is not None:
            params = set_default_query

        http_client = http_client or self._client
        return self.__class__(
            api_key=api_key or self.api_key,
            api_version=api_version or self.api_version,
            base_url=base_url or self.base_url,
            timeout=self.timeout if isinstance(timeout, NotGiven) else timeout,
            http_client=http_client,
            max_retries=max_retries if is_given(max_retries) else self.max_retries,
            default_headers=headers,
            default_query=params,
            client_adapter=self.client_adapter or client_adapter,
            **_extra_kwargs,
        )

    # Alias for `copy` for nicer inline usage, e.g.
    # client.with_options(timeout=10).foo.create(...)
    with_options = copy

    def _get_api_version_path_param(self) -> str:
        return self.api_version

    @override
    def _make_status_error(
        self,
        err_msg: str,
        *,
        body: object,
        response: httpx.Response,
    ) -> APIStatusError:
        if response.status_code == 400:
            return _exceptions.BadRequestError(err_msg, response=response, body=body)

        if response.status_code == 401:
            return _exceptions.AuthenticationError(err_msg, response=response, body=body)

        if response.status_code == 403:
            return _exceptions.PermissionDeniedError(err_msg, response=response, body=body)

        if response.status_code == 404:
            return _exceptions.NotFoundError(err_msg, response=response, body=body)

        if response.status_code == 409:
            return _exceptions.ConflictError(err_msg, response=response, body=body)

        if response.status_code == 422:
            return _exceptions.UnprocessableEntityError(err_msg, response=response, body=body)

        if response.status_code == 429:
            return _exceptions.RateLimitError(err_msg, response=response, body=body)

        if response.status_code >= 500:
            return _exceptions.InternalServerError(err_msg, response=response, body=body)
        return APIStatusError(err_msg, response=response, body=body)


class AsyncGeminiNextGenAPIClient(AsyncAPIClient):
    # client options
    api_key: str | None
    api_version: str
    client_adapter: AsyncGeminiNextGenAPIClientAdapter | None

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_version: str | None = "v1beta",
        base_url: str | httpx.URL | None = None,
        timeout: float | Timeout | None | NotGiven = not_given,
        max_retries: int = DEFAULT_MAX_RETRIES,
        default_headers: Mapping[str, str] | None = None,
        default_query: Mapping[str, object] | None = None,
        # Configure a custom httpx client.
        # We provide a `DefaultAsyncHttpxClient` class that you can pass to retain the default values we use for `limits`, `timeout` & `follow_redirects`.
        # See the [httpx documentation](https://www.python-httpx.org/api/#asyncclient) for more details.
        http_client: httpx.AsyncClient | None = None,
        client_adapter: AsyncGeminiNextGenAPIClientAdapter | None = None,
        # Enable or disable schema validation for data returned by the API.
        # When enabled an error APIResponseValidationError is raised
        # if the API responds with invalid data for the expected schema.
        #
        # This parameter may be removed or changed in the future.
        # If you rely on this feature, please open a GitHub issue
        # outlining your use-case to help us decide if it should be
        # part of our public interface in the future.
        _strict_response_validation: bool = False,
    ) -> None:
        """Construct a new async AsyncGeminiNextGenAPIClient client instance.

        This automatically infers the `api_key` argument from the `GEMINI_API_KEY` environment variable if it is not provided.
        """
        if api_key is None:
            api_key = os.environ.get("GEMINI_API_KEY")
        self.api_key = api_key

        if api_version is None:
            api_version = "v1beta"
        self.api_version = api_version

        if base_url is None:
            base_url = os.environ.get("GEMINI_NEXT_GEN_API_BASE_URL")
        if base_url is None:
            base_url = f"https://generativelanguage.googleapis.com"

        self.client_adapter = client_adapter

        super().__init__(
            version=__version__,
            base_url=base_url,
            max_retries=max_retries,
            timeout=timeout,
            http_client=http_client,
            custom_headers=default_headers,
            custom_query=default_query,
            _strict_response_validation=_strict_response_validation,
        )

        self._default_stream_cls = AsyncStream

    @cached_property
    def interactions(self) -> AsyncInteractionsResource:
        from .resources.interactions import AsyncInteractionsResource

        return AsyncInteractionsResource(self)

    @cached_property
    def with_raw_response(self) -> AsyncGeminiNextGenAPIClientWithRawResponse:
        return AsyncGeminiNextGenAPIClientWithRawResponse(self)

    @cached_property
    def with_streaming_response(self) -> AsyncGeminiNextGenAPIClientWithStreamedResponse:
        return AsyncGeminiNextGenAPIClientWithStreamedResponse(self)

    @property
    @override
    def qs(self) -> Querystring:
        return Querystring(array_format="comma")

    @property
    @override
    def auth_headers(self) -> dict[str, str]:
        api_key = self.api_key
        if api_key is None:
            return {}
        return {"x-goog-api-key": api_key}

    @property
    @override
    def default_headers(self) -> dict[str, str | Omit]:
        return {
            **super().default_headers,
            "X-Stainless-Async": f"async:{get_async_library()}",
            **self._custom_headers,
        }

    @override
    def _validate_headers(self, headers: Headers, custom_headers: Headers) -> None:
        if headers.get("Authorization") or custom_headers.get("Authorization") or isinstance(custom_headers.get("Authorization"), Omit):
            return
        if self.api_key and headers.get("x-goog-api-key"):
            return
        if custom_headers.get("x-goog-api-key") or isinstance(custom_headers.get("x-goog-api-key"), Omit):
            return

        raise TypeError(
            '"Could not resolve authentication method. Expected the api_key to be set. Or for the `x-goog-api-key` headers to be explicitly omitted"'
        )
    
    @override
    async def _prepare_options(self, options: FinalRequestOptions) -> FinalRequestOptions:
        if not self.client_adapter or not self.client_adapter.is_vertex_ai():
            return options

        headers = options.headers or {}
        has_auth = headers.get("Authorization") or headers.get("x-goog-api-key")     # pytype: disable=attribute-error
        if has_auth:
            return options

        adapted_headers = await self.client_adapter.async_get_auth_headers()
        if adapted_headers:
            options.headers = {
                **adapted_headers,
                **headers
            }
        return options

    def copy(
        self,
        *,
        api_key: str | None = None,
        api_version: str | None = None,
        base_url: str | httpx.URL | None = None,
        timeout: float | Timeout | None | NotGiven = not_given,
        http_client: httpx.AsyncClient | None = None,
        max_retries: int | NotGiven = not_given,
        default_headers: Mapping[str, str] | None = None,
        set_default_headers: Mapping[str, str] | None = None,
        default_query: Mapping[str, object] | None = None,
        set_default_query: Mapping[str, object] | None = None,
        client_adapter: AsyncGeminiNextGenAPIClientAdapter | None = None,
        _extra_kwargs: Mapping[str, Any] = {},
    ) -> Self:
        """
        Create a new client instance re-using the same options given to the current client with optional overriding.
        """
        if default_headers is not None and set_default_headers is not None:
            raise ValueError("The `default_headers` and `set_default_headers` arguments are mutually exclusive")

        if default_query is not None and set_default_query is not None:
            raise ValueError("The `default_query` and `set_default_query` arguments are mutually exclusive")

        headers = self._custom_headers
        if default_headers is not None:
            headers = {**headers, **default_headers}
        elif set_default_headers is not None:
            headers = set_default_headers

        params = self._custom_query
        if default_query is not None:
            params = {**params, **default_query}
        elif set_default_query is not None:
            params = set_default_query

        http_client = http_client or self._client
        return self.__class__(
            api_key=api_key or self.api_key,
            api_version=api_version or self.api_version,
            base_url=base_url or self.base_url,
            timeout=self.timeout if isinstance(timeout, NotGiven) else timeout,
            http_client=http_client,
            max_retries=max_retries if is_given(max_retries) else self.max_retries,
            default_headers=headers,
            default_query=params,
            client_adapter=self.client_adapter or client_adapter,
            **_extra_kwargs,
        )

    # Alias for `copy` for nicer inline usage, e.g.
    # client.with_options(timeout=10).foo.create(...)
    with_options = copy

    def _get_api_version_path_param(self) -> str:
        return self.api_version

    @override
    def _make_status_error(
        self,
        err_msg: str,
        *,
        body: object,
        response: httpx.Response,
    ) -> APIStatusError:
        if response.status_code == 400:
            return _exceptions.BadRequestError(err_msg, response=response, body=body)

        if response.status_code == 401:
            return _exceptions.AuthenticationError(err_msg, response=response, body=body)

        if response.status_code == 403:
            return _exceptions.PermissionDeniedError(err_msg, response=response, body=body)

        if response.status_code == 404:
            return _exceptions.NotFoundError(err_msg, response=response, body=body)

        if response.status_code == 409:
            return _exceptions.ConflictError(err_msg, response=response, body=body)

        if response.status_code == 422:
            return _exceptions.UnprocessableEntityError(err_msg, response=response, body=body)

        if response.status_code == 429:
            return _exceptions.RateLimitError(err_msg, response=response, body=body)

        if response.status_code >= 500:
            return _exceptions.InternalServerError(err_msg, response=response, body=body)
        return APIStatusError(err_msg, response=response, body=body)


class GeminiNextGenAPIClientWithRawResponse:
    _client: GeminiNextGenAPIClient

    def __init__(self, client: GeminiNextGenAPIClient) -> None:
        self._client = client

    @cached_property
    def interactions(self) -> interactions.InteractionsResourceWithRawResponse:
        from .resources.interactions import InteractionsResourceWithRawResponse

        return InteractionsResourceWithRawResponse(self._client.interactions)


class AsyncGeminiNextGenAPIClientWithRawResponse:
    _client: AsyncGeminiNextGenAPIClient

    def __init__(self, client: AsyncGeminiNextGenAPIClient) -> None:
        self._client = client

    @cached_property
    def interactions(self) -> interactions.AsyncInteractionsResourceWithRawResponse:
        from .resources.interactions import AsyncInteractionsResourceWithRawResponse

        return AsyncInteractionsResourceWithRawResponse(self._client.interactions)


class GeminiNextGenAPIClientWithStreamedResponse:
    _client: GeminiNextGenAPIClient

    def __init__(self, client: GeminiNextGenAPIClient) -> None:
        self._client = client

    @cached_property
    def interactions(self) -> interactions.InteractionsResourceWithStreamingResponse:
        from .resources.interactions import InteractionsResourceWithStreamingResponse

        return InteractionsResourceWithStreamingResponse(self._client.interactions)


class AsyncGeminiNextGenAPIClientWithStreamedResponse:
    _client: AsyncGeminiNextGenAPIClient

    def __init__(self, client: AsyncGeminiNextGenAPIClient) -> None:
        self._client = client

    @cached_property
    def interactions(self) -> interactions.AsyncInteractionsResourceWithStreamingResponse:
        from .resources.interactions import AsyncInteractionsResourceWithStreamingResponse

        return AsyncInteractionsResourceWithStreamingResponse(self._client.interactions)


Client = GeminiNextGenAPIClient

AsyncClient = AsyncGeminiNextGenAPIClient
