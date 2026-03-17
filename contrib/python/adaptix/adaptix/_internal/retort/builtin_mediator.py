from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any, Callable, Generic, TypeVar

from ..provider.essential import CannotProvide, Mediator, Request

T = TypeVar("T")


RequestT = TypeVar("RequestT", bound=Request)
ResponseT = TypeVar("ResponseT")


class RequestBus(ABC, Generic[RequestT, ResponseT]):
    @abstractmethod
    def send(self, request: RequestT) -> ResponseT:
        pass

    @abstractmethod
    def send_chaining(self, request: RequestT, search_offset: int) -> ResponseT:
        pass


class BuiltinMediator(Mediator[ResponseT], Generic[ResponseT]):
    __slots__ = ("_call_cache", "_no_request_bus_error_maker", "_request", "_request_buses", "_search_offset")

    def __init__(
        self,
        request_buses: Mapping[type[Request], RequestBus],
        request: Request,
        search_offset: int,
        no_request_bus_error_maker: Callable[[Request], CannotProvide],
        call_cache: dict[Any, Any],
    ):
        self._request_buses = request_buses
        self._request = request
        self._search_offset = search_offset
        self._no_request_bus_error_maker = no_request_bus_error_maker
        self._call_cache = call_cache

    __hash__ = None  # type: ignore[assignment]

    def provide(self, request: Request[T]) -> T:
        try:
            request_bus = self._request_buses[type(request)]
        except KeyError:
            raise self._no_request_bus_error_maker(request) from None

        return request_bus.send(request)

    def provide_from_next(self) -> ResponseT:
        return self._request_buses[type(self._request)].send_chaining(self._request, self._search_offset)

    def cached_call(self, func: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:  # type: ignore[override]
        key = (func, *args, *kwargs.items())
        if key in self._call_cache:
            return self._call_cache[key]
        result = func(*args, **kwargs)
        self._call_cache[key] = result
        return result
