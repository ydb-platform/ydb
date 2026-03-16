import json
import ssl
import typing
import uuid

import aiohttp
import yarl

from testsuite import types
from testsuite.utils import http, url_util

DEFAULT_HOST = 'localhost'
DEFAULT_TIMEOUT = 120.0

TResponse = typing.TypeVar(
    'TResponse',
    aiohttp.ClientResponse,
    http.ClientResponse,
)


class BaseAiohttpClient:
    def __init__(
        self,
        base_url: str,
        *,
        session: aiohttp.ClientSession,
        ssl_context: ssl.SSLContext | None = None,
        span_id_header: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ):
        """
        :param base_url: Base client url
        :param session: ``aiohttp.ClientSession`` instance
        :param headers: default request headers dictionary
        :param timeout: http client default timeout
        """
        self._base_url = url_util.ensure_trailing_separator(base_url)
        self._headers = headers or {}
        self._timeout = timeout
        self._session = session
        self._ssl_context = ssl_context
        self._span_id_header = span_id_header

    def url(self, path: str | yarl.URL):
        if isinstance(path, str):
            return url_util.join(self._base_url, path)
        return path

    async def _aiohttp_request(
        self,
        http_method: str,
        path: str | yarl.URL,
        headers: dict[str, str] | None = None,
        bearer: str | None = None,
        x_real_ip: str | None = None,
        **kwargs,
    ) -> aiohttp.ClientResponse:
        url = self.url(path)
        headers = self._build_headers(
            headers,
            bearer=bearer,
            x_real_ip=x_real_ip,
        )
        kwargs['timeout'] = kwargs.get('timeout', self._timeout)

        params = kwargs.get('params', None)
        if params is not None:
            kwargs['params'] = _flatten(params)
        response = await self._session.request(
            http_method,
            url,
            headers=headers,
            ssl=self._ssl_context,  # type: ignore[arg-type]
            **kwargs,
        )
        return response

    def _build_headers(
        self,
        user_headers: dict[str, str] | None = None,
        bearer: str | None = None,
        x_real_ip: str | None = None,
    ) -> dict[str, str]:
        headers = self._headers.copy()
        if user_headers:
            headers.update(user_headers)
        if bearer:
            headers['Authorization'] = 'Bearer %s' % bearer
        if x_real_ip:
            headers['X-Real-IP'] = x_real_ip
        if self._span_id_header and self._span_id_header not in headers:
            headers[self._span_id_header] = uuid.uuid4().hex

        headers = {
            key: '' if value is None else value
            for key, value in headers.items()
        }
        return headers


class GenericClient(BaseAiohttpClient, typing.Generic[TResponse]):
    """Basic asyncio HTTP service client."""

    async def post(
        self,
        path: str,
        json: types.JsonAnyOptional = None,
        data: typing.Any = None,
        params: dict[str, str] | None = None,
        bearer: str | None = None,
        x_real_ip: str | None = None,
        headers: dict[str, str] | None = None,
        **kwargs,
    ) -> TResponse:
        """Perform HTTP POST request."""
        return await self._request(
            'POST',
            path,
            json=json,
            data=data,
            params=params,
            headers=headers,
            bearer=bearer,
            x_real_ip=x_real_ip,
            **kwargs,
        )

    async def put(
        self,
        path,
        json: types.JsonAnyOptional = None,
        data: typing.Any = None,
        params: dict[str, str] | None = None,
        bearer: str | None = None,
        x_real_ip: str | None = None,
        headers: dict[str, str] | None = None,
        **kwargs,
    ) -> TResponse:
        """Perform HTTP PUT request."""
        return await self._request(
            'PUT',
            path,
            json=json,
            data=data,
            params=params,
            headers=headers,
            bearer=bearer,
            x_real_ip=x_real_ip,
            **kwargs,
        )

    async def patch(
        self,
        path,
        json: types.JsonAnyOptional = None,
        data: typing.Any = None,
        params: dict[str, str] | None = None,
        bearer: str | None = None,
        x_real_ip: str | None = None,
        headers: dict[str, str] | None = None,
        **kwargs,
    ) -> TResponse:
        """Perform HTTP PATCH request."""
        return await self._request(
            'PATCH',
            path,
            json=json,
            data=data,
            params=params,
            headers=headers,
            bearer=bearer,
            x_real_ip=x_real_ip,
            **kwargs,
        )

    async def get(
        self,
        path: str,
        headers: dict[str, str] | None = None,
        bearer: str | None = None,
        x_real_ip: str | None = None,
        **kwargs,
    ) -> TResponse:
        """Perform HTTP GET request."""
        return await self._request(
            'GET',
            path,
            headers=headers,
            bearer=bearer,
            x_real_ip=x_real_ip,
            **kwargs,
        )

    async def delete(
        self,
        path: str,
        headers: dict[str, str] | None = None,
        bearer: str | None = None,
        x_real_ip: str | None = None,
        **kwargs,
    ) -> TResponse:
        """Perform HTTP DELETE request."""
        return await self._request(
            'DELETE',
            path,
            headers=headers,
            bearer=bearer,
            x_real_ip=x_real_ip,
            **kwargs,
        )

    async def options(
        self,
        path: str,
        headers: dict[str, str] | None = None,
        bearer: str | None = None,
        x_real_ip: str | None = None,
        **kwargs,
    ) -> TResponse:
        """Perform HTTP OPTIONS request."""
        return await self._request(
            'OPTIONS',
            path,
            headers=headers,
            bearer=bearer,
            x_real_ip=x_real_ip,
            **kwargs,
        )

    async def request(
        self,
        http_method: str,
        path: str,
        **kwargs,
    ) -> TResponse:
        """Perform HTTP ``http_method`` request."""
        return await self._request(http_method, path, **kwargs)

    async def _request(
        self,
        http_method: str,
        path: str | yarl.URL,
        headers: dict[str, str] | None = None,
        bearer: str | None = None,
        x_real_ip: str | None = None,
        **kwargs,
    ) -> TResponse:
        raise NotImplementedError


class AiohttpClient(GenericClient[aiohttp.ClientResponse]):
    async def _request(
        self,
        http_method: str,
        path: str | yarl.URL,
        headers: dict[str, str] | None = None,
        bearer: str | None = None,
        x_real_ip: str | None = None,
        **kwargs,
    ) -> aiohttp.ClientResponse:
        return await self._aiohttp_request(
            http_method,
            path,
            headers,
            bearer,
            x_real_ip,
            **kwargs,
        )


class Client(GenericClient[http.ClientResponse]):
    async def _request(
        self,
        http_method: str,
        path: str | yarl.URL,
        headers: dict[str, str] | None = None,
        bearer: str | None = None,
        x_real_ip: str | None = None,
        **kwargs,
    ) -> http.ClientResponse:
        response = await self._aiohttp_request(
            http_method,
            path,
            headers,
            bearer,
            x_real_ip,
            **kwargs,
        )
        return await self._wrap_client_response(response)

    def _wrap_client_response(
        self,
        response,
    ) -> typing.Awaitable[http.ClientResponse]:
        return http.wrap_client_response(response, json_loads=json.loads)


def _flatten(query_params):
    result = []
    iterable = (
        query_params.items() if isinstance(query_params, dict) else query_params
    )
    for key, value in iterable:
        if isinstance(value, (tuple, list)):
            for element in value:
                result.append((key, str(element)))
        else:
            result.append((key, str(value)))
    return result
