import urllib.parse
from json import JSONDecodeError
from typing import Any, Optional

from aiohttp import FormData
from aiohttp.client import (
    ClientError as AioHttpClientError,
)
from aiohttp.client import (
    ClientResponse,
    ClientSession,
)

from dataclass_rest.base_client import BaseClient
from dataclass_rest.boundmethod import AsyncMethod
from dataclass_rest.exceptions import (
    ClientError,
    ClientLibraryError,
    MalformedResponse,
    ServerError,
)
from dataclass_rest.http_request import HttpRequest


class AiohttpMethod(AsyncMethod):
    def _on_error_default(self, response: ClientResponse) -> Any:
        if 400 <= response.status < 500:
            raise ClientError(response.status)
        else:
            raise ServerError(response.status)

    async def _release_raw_response(self, response: ClientResponse) -> None:
        await response.release()

    async def _response_body(self, response: ClientResponse) -> Any:
        try:
            return await response.json()
        except AioHttpClientError as e:
            raise ClientLibraryError from e
        except JSONDecodeError as e:
            raise MalformedResponse from e

    async def _response_ok(self, response: ClientResponse) -> bool:
        return response.ok


class AiohttpClient(BaseClient):
    method_class = AiohttpMethod

    def __init__(
        self,
        base_url: str,
        session: Optional[ClientSession] = None,
    ):
        super().__init__()
        self.session = session or ClientSession()
        self.base_url = base_url

    async def do_request(self, request: HttpRequest) -> Any:
        if request.is_json_request:
            json = request.data
            data = None
        else:
            json = None
            data = request.data
        if request.files:
            data = FormData(data or {})
            for name, file in request.files.items():
                data.add_field(
                    name,
                    filename=file.filename,
                    content_type=file.content_type,
                    value=file.contents,
                )
        try:
            return await self.session.request(
                url=urllib.parse.urljoin(self.base_url, request.url),
                method=request.method,
                json=json,
                data=data,
                params=request.query_params,
                headers=request.headers,
            )
        except AioHttpClientError as e:
            raise ClientLibraryError from e
