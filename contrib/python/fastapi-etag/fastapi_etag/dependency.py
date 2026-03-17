from typing import Optional, MutableMapping
from enum import Enum
from fastapi import HTTPException, FastAPI
from starlette.requests import Request
from starlette.responses import Response
from inspect import iscoroutinefunction

from fastapi_etag.types import EtagGen


class CacheHit(HTTPException):
    pass


class PreconditionFailed(HTTPException):
    pass


class HeaderType(Enum):
    IF_MATCH = "if-match"
    IF_NONE_MATCH = "if-none-match"


class Etag:
    def __init__(
        self, etag_gen: EtagGen, weak=True, extra_headers: MutableMapping = None
    ):
        self.etag_gen = etag_gen
        self.weak = weak
        self.extra_headers = extra_headers

    def is_modified(self, etag: Optional[str], client_etag: Optional[str]):
        if not etag:
            return True
        return not client_etag or etag != client_etag

    async def __call__(self, request: Request, response: Response) -> Optional[str]:
        etag = (
            await self.etag_gen(request)  # type: ignore
            if iscoroutinefunction(self.etag_gen)
            else self.etag_gen(request)
        )
        if etag and self.weak:
            etag = f'W/"{etag}"'

        client_etag: Optional[str] = None
        header_type: Optional[HeaderType] = None
        if request.headers.get("if-none-match") is not None:
            client_etag = request.headers.get("if-none-match")
            header_type = HeaderType.IF_NONE_MATCH
        elif request.headers.get("if-match") is not None:
            client_etag = request.headers.get("if-match")
            header_type = HeaderType.IF_MATCH

        modified = self.is_modified(etag, client_etag)
        if etag:
            headers = {"etag": etag}
        else:
            headers = {}
        if self.extra_headers:
            headers.update(self.extra_headers)

        if not modified and header_type == HeaderType.IF_NONE_MATCH:
            raise CacheHit(304, headers=headers)
        elif modified and header_type == HeaderType.IF_MATCH:
            raise PreconditionFailed(412, headers=headers)
        response.headers.update(headers)
        return etag


async def etag_cache_hit_exception_handler(request: Request, exc: CacheHit):
    return Response("", 304, headers=exc.headers)


async def etag_precondition_failed_exception_handler(
    request: Request, exc: PreconditionFailed
):
    return Response("", 412, headers=exc.headers)


def add_exception_handler(app: FastAPI):
    app.add_exception_handler(CacheHit, etag_cache_hit_exception_handler)
    app.add_exception_handler(
        PreconditionFailed, etag_precondition_failed_exception_handler
    )
