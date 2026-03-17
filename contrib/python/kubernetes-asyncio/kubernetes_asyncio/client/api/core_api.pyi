from aiohttp import ClientTimeout
from kubernetes_asyncio.client.api_client import ApiClient
from kubernetes_asyncio.client.models import V1APIVersions
from multidict import CIMultiDictProxy
from typing import Any
from typing import Awaitable

class CoreApi:
    def __init__(self, api_client: ApiClient | None = None) -> None: ...
    def get_api_versions(self, *, async_req: bool = ..., _preload_content: bool = ..., _request_timeout: None | int | float | tuple[float, float] | ClientTimeout = ...) -> Awaitable[V1APIVersions]: ...
    def get_api_versions_with_http_info(self, *, async_req: bool = ..., _return_http_data_only: bool = ..., _preload_content: bool = ..., _request_timeout: None | int | float | tuple[float, float] | ClientTimeout = ..., _request_auth: dict = ...) -> Awaitable[tuple[V1APIVersions, int, CIMultiDictProxy]]: ...
