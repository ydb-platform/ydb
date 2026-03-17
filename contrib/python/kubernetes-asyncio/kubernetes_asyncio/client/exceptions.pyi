from aiohttp import ClientResponse
from multidict import CIMultiDictProxy

class OpenApiException(Exception): ...

class ApiTypeError(OpenApiException, TypeError):
    path_to_item: list | None
    valid_classes: tuple | None
    key_type: bool | None
    def __init__(
        self,
        msg: str,
        path_to_item: list | None = None,
        valid_classes: tuple | None = None,
        key_type: bool | None = None,
    ) -> None: ...

class ApiValueError(OpenApiException, ValueError):
    path_to_item: list | None
    def __init__(self, msg: str, path_to_item: list | None = None) -> None: ...

class ApiAttributeError(OpenApiException, AttributeError):
    path_to_item: list | None
    def __init__(self, msg: str, path_to_item: list | None = None) -> None: ...

class ApiKeyError(OpenApiException, KeyError):
    path_to_item: list | None
    def __init__(self, msg: str, path_to_item: list | None = None) -> None: ...

class ApiException(OpenApiException):
    status: int
    reason: str | None
    body: bytes | None
    headers: CIMultiDictProxy | None
    def __init__(
        self,
        status: int | None = None,
        reason: str | None = None,
        http_resp: ClientResponse | None = None,
    ) -> None: ...

class NotFoundException(ApiException):
    def __init__(
        self,
        status: int | None = None,
        reason: str | None = None,
        http_resp: ClientResponse | None = None,
    ) -> None: ...

class UnauthorizedException(ApiException):
    def __init__(
        self,
        status: int | None = None,
        reason: str | None = None,
        http_resp: ClientResponse | None = None,
    ) -> None: ...

class ForbiddenException(ApiException):
    def __init__(
        self,
        status: int | None = None,
        reason: str | None = None,
        http_resp: ClientResponse | None = None,
    ) -> None: ...

class ServiceException(ApiException):
    def __init__(
        self,
        status: int | None = None,
        reason: str | None = None,
        http_resp: ClientResponse | None = None,
    ) -> None: ...

def render_path(path_to_item): ...
