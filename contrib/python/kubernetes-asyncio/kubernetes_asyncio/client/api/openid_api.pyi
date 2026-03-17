from aiohttp import ClientTimeout
from kubernetes_asyncio.client.api_client import ApiClient
from multidict import CIMultiDictProxy
from typing import Any
from typing import Awaitable

class OpenidApi:
    def __init__(self, api_client: ApiClient | None = None) -> None: ...
    def get_service_account_issuer_open_id_keyset(self, *, async_req: bool = ..., _preload_content: bool = ..., _request_timeout: None | int | float | tuple[float, float] | ClientTimeout = ...) -> Awaitable[str]: ...
    def get_service_account_issuer_open_id_keyset_with_http_info(self, *, async_req: bool = ..., _return_http_data_only: bool = ..., _preload_content: bool = ..., _request_timeout: None | int | float | tuple[float, float] | ClientTimeout = ..., _request_auth: dict = ...) -> Awaitable[tuple[str, int, CIMultiDictProxy]]: ...
