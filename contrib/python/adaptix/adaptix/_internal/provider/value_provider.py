from collections.abc import Sequence
from typing import Generic, TypeVar

from .essential import Provider, Request, RequestHandlerRegisterRecord
from .request_checkers import AlwaysTrueRequestChecker

T = TypeVar("T")


class ValueProvider(Provider, Generic[T]):
    def __init__(self, request_cls: type[Request[T]], value: T):
        self._request_cls = request_cls
        self._value = value

    def get_request_handlers(self) -> Sequence[RequestHandlerRegisterRecord]:
        return [
            (self._request_cls, AlwaysTrueRequestChecker(), lambda m, r: self._value),
        ]

    def __repr__(self):
        return f"{type(self).__name__}({self._request_cls}, {self._value})"
