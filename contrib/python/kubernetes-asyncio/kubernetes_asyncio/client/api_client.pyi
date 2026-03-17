from multiprocessing.pool import ThreadPool
from typing import Any

from aiohttp import ClientTimeout
from kubernetes_asyncio.client import rest as rest
from kubernetes_asyncio.client.configuration import Configuration as Configuration
from kubernetes_asyncio.client.exceptions import (
    ApiException as ApiException,
    ApiValueError as ApiValueError,
)

class ApiClient:
    PRIMITIVE_TYPES: list[Any]
    NATIVE_TYPES_MAPPING: dict[str, Any]
    configuration: Configuration
    pool_threads: int
    rest_client: rest.RESTClientObject
    default_headers: dict[str, str]
    cookie: str
    client_side_validation: bool
    def __init__(
        self,
        configuration: Configuration | None = None,
        header_name: str | None = None,
        header_value: str | None = None,
        cookie: str | None = None,
        pool_threads: int = 1,
    ) -> None: ...
    async def __aenter__(self): ...
    async def __aexit__(self, exc_type, exc_value, traceback) -> None: ...
    async def close(self) -> None: ...
    @property
    def pool(self) -> ThreadPool: ...
    @property
    def user_agent(self) -> str | None: ...
    @user_agent.setter
    def user_agent(self, value: str) -> None: ...
    def set_default_header(self, header_name: str, header_value: str) -> None: ...
    def sanitize_for_serialization(self, obj: Any) -> Any: ...
    def deserialize(self, response: rest.RESTResponse, response_type: Any): ...
    def call_api(
        self,
        resource_path: str,
        method: str,
        path_params: Any | None = None,
        query_params: Any | None = None,
        header_params: dict | None = None,
        body: Any | None = None,
        post_params: dict | None = None,
        files: Any | None = None,
        response_types_map: dict | None = None,
        auth_settings: list | None = None,
        async_req: bool | None = None,
        _return_http_data_only: bool | None = None,
        collection_formats: dict | None = None,
        _preload_content: bool = True,
        _request_timeout: float | tuple[float] | ClientTimeout | None = None,
        _host: str | None = None,
        _request_auth: str | None = None,
    ) -> Any: ...
    def request(
        self,
        method,
        url,
        query_params: Any | None = None,
        headers: dict | None = None,
        post_params: dict | None = None,
        body: Any | None = None,
        _preload_content: bool = True,
        _request_timeout: float | tuple[float] | ClientTimeout | None = None,
    ) -> Any: ...
    def parameters_to_tuples(self, params : dict | list, collection_formats: str): ...
    def files_parameters(self, files: Any | None = None) -> list: ...
    def select_header_accept(self, accepts:list[str]) -> str: ...
    def select_header_content_type(
        self,
        content_types: list[str],
        method: str | None = None,
        body: Any | None = None,
    ): ...
    async def update_params_for_auth(
        self, headers: dict, queries: list, auth_settings: list, request_auth: dict | None = None
    ) -> None: ...
