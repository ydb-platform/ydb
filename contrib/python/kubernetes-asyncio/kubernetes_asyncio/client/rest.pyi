import io
from logging import Logger
from typing import Any
from aiohttp import ClientResponse, ClientSession, ClientTimeout
from multidict import CIMultiDictProxy

from kubernetes_asyncio.client.exceptions import (
    ApiException as ApiException,
    ApiValueError as ApiValueError,
)

logger: Logger

class RESTResponse(io.IOBase):
    aiohttp_response: ClientResponse
    status: int
    reason: str
    data: bytes
    def __init__(self, resp: ClientResponse, data: bytes) -> None: ...
    def getheaders(self) -> CIMultiDictProxy: ...
    def getheader(self, name: str, default: str | None = None) -> str | None: ...

class RESTClientObject:
    server_hostname: str
    proxy: str
    proxy_headers: dict[str, str]
    pool_manager: ClientSession
    def __init__(
        self, configuration, pools_size: int = 4, maxsize: int | None = None
    ) -> None: ...
    async def close(self) -> None: ...
    async def request(
        self,
        method,
        url,
        query_params: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        body: Any | None = None,
        post_params: dict[str, Any] | None = None,
        _preload_content: bool = True,
        _request_timeout: float | tuple[float] | ClientTimeout | None = None,
    ): ...
    async def GET(
        self,
        url,
        headers: dict[str, Any] | None = None,
        query_params: dict[str, Any] | None = None,
        _preload_content: bool = True,
        _request_timeout: float | tuple[float] | ClientTimeout | None = None,
    ): ...
    async def HEAD(
        self,
        url,
        headers: dict[str, Any] | None = None,
        query_params: dict[str, Any] | None = None,
        _preload_content: bool = True,
        _request_timeout: float | tuple[float] | ClientTimeout | None = None,
    ): ...
    async def OPTIONS(
        self,
        url,
        headers: dict[str, Any] | None = None,
        query_params: dict[str, Any] | None = None,
        post_params: dict[str, Any] | None = None,
        body: Any | None = None,
        _preload_content: bool = True,
        _request_timeout: float | tuple[float] | ClientTimeout | None = None,
    ): ...
    async def DELETE(
        self,
        url,
        headers: dict[str, Any] | None = None,
        query_params: dict[str, Any] | None = None,
        body: Any | None = None,
        _preload_content: bool = True,
        _request_timeout: float | tuple[float] | ClientTimeout | None = None,
    ): ...
    async def POST(
        self,
        url,
        headers: dict[str, Any] | None = None,
        query_params: dict[str, Any] | None = None,
        post_params: dict[str, Any] | None = None,
        body: Any | None = None,
        _preload_content: bool = True,
        _request_timeout: float | tuple[float] | ClientTimeout | None = None,
    ): ...
    async def PUT(
        self,
        url,
        headers: dict[str, Any] | None = None,
        query_params: dict[str, Any] | None = None,
        post_params: dict[str, Any] | None = None,
        body: Any | None = None,
        _preload_content: bool = True,
        _request_timeout: float | tuple[float] | ClientTimeout | None = None,
    ): ...
    async def PATCH(
        self,
        url,
        headers: dict[str, Any] | None = None,
        query_params: dict[str, Any] | None = None,
        post_params: dict[str, Any] | None = None,
        body: Any | None = None,
        _preload_content: bool = True,
        _request_timeout: float | tuple[float] | ClientTimeout | None = None,
    ): ...
