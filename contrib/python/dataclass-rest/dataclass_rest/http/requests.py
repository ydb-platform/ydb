import urllib.parse
from json import JSONDecodeError
from typing import Any, Optional, Tuple

from requests import RequestException, Response, Session

from dataclass_rest.base_client import BaseClient
from dataclass_rest.boundmethod import SyncMethod
from dataclass_rest.exceptions import (
    ClientError,
    ClientLibraryError,
    MalformedResponse,
    ServerError,
)
from dataclass_rest.http_request import File, HttpRequest


class RequestsMethod(SyncMethod):
    def _on_error_default(self, response: Response) -> Any:
        if 400 <= response.status_code < 500:
            raise ClientError(response.status_code)
        else:
            raise ServerError(response.status_code)

    def _response_ok(self, response: Response) -> bool:
        return response.ok

    def _response_body(self, response: Response) -> Any:
        try:
            return response.json()
        except RequestException as e:
            raise ClientLibraryError from e
        except JSONDecodeError as e:
            raise MalformedResponse from e


class RequestsClient(BaseClient):
    method_class = RequestsMethod

    def __init__(
        self,
        base_url: str,
        session: Optional[Session] = None,
    ):
        super().__init__()
        self.session = session or Session()
        self.base_url = base_url

    def _prepare_file(self, fieldname: str, file: File) -> Tuple:
        return (file.filename or fieldname, file.contents, file.content_type)

    def do_request(self, request: HttpRequest) -> Any:
        if request.is_json_request:
            json = request.data
            data = None
        else:
            json = None
            data = request.data

        files = {
            name: self._prepare_file(name, file)
            for name, file in request.files.items()
        }

        try:
            return self.session.request(
                url=urllib.parse.urljoin(self.base_url, request.url),
                method=request.method,
                json=json,
                params=request.query_params,
                data=data,
                headers=request.headers,
                files=files,
            )
        except RequestException as e:
            raise ClientLibraryError from e
