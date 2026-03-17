from __future__ import annotations

from typing import Any, AnyStr

from werkzeug.datastructures import MultiDict
from werkzeug.local import LocalProxy

from quart.globals import (
    _app_ctx_stack,
    _ctx_lookup,
    _request_ctx_stack,
    current_app,
    g,
    request as quart_request,
    session,
)
from ._synchronise import sync_with_context


class FlaskRequestProxy(LocalProxy):
    @property
    def data(self) -> bytes:
        return sync_with_context(self._get_current_object().data)

    @property
    def form(self) -> MultiDict:
        return sync_with_context(self._get_current_object().form)

    @property
    def files(self) -> MultiDict:
        return sync_with_context(self._get_current_object().files)

    @property
    def json(self) -> Any:
        return sync_with_context(self._get_current_object().json)

    def get_json(self, *args: Any, **kwargs: Any) -> Any:
        return sync_with_context(self._get_current_object().get_json(*args, **kwargs))

    def get_data(self, *args: Any, **kwargs: Any) -> AnyStr:
        return sync_with_context(self._get_current_object().get_data(*args, **kwargs))


request = FlaskRequestProxy(lambda: quart_request)


def _lookup_app_object(name: str) -> Any:
    return _ctx_lookup([_app_ctx_stack], name)


def _lookup_req_object(name: str) -> Any:
    return _ctx_lookup([_request_ctx_stack], name)


__all__ = (
    "_app_ctx_stack",
    "_lookup_app_object",
    "_lookup_req_object",
    "_request_ctx_stack",
    "current_app",
    "g",
    "request",
    "session",
)
