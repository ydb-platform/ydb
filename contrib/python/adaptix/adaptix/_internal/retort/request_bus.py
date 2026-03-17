from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any, Callable, Generic, Optional, TypeVar

from ..provider.essential import (
    AggregateCannotProvide,
    CannotProvide,
    DirectMediator,
    Mediator,
    Request,
    RequestHandler,
)
from ..utils import add_note
from .builtin_mediator import RequestBus

RequestT = TypeVar("RequestT", bound=Request)
ResponseT = TypeVar("ResponseT")


class ErrorRepresentor(ABC, Generic[RequestT]):
    @abstractmethod
    def get_provider_not_found_description(self, request: RequestT) -> str:
        ...

    @abstractmethod
    def get_request_context_notes(self, request: RequestT) -> Iterable[str]:
        ...


class RequestRouter(ABC, Generic[RequestT]):
    """An offset of each element must belong to [0; max_offset)"""

    @abstractmethod
    def route_handler(
        self,
        mediator: DirectMediator,
        request: RequestT,
        search_offset: int,
    ) -> tuple[RequestHandler, int]:
        """
        :raises: StopIteration
        """

    @abstractmethod
    def get_max_offset(self) -> int:
        ...


E = TypeVar("E", bound=Exception)


class BasicRequestBus(RequestBus[RequestT, ResponseT], Generic[RequestT, ResponseT]):
    __slots__ = ("_error_representor", "_mediator_factory", "_router")

    def __init__(
        self,
        router: RequestRouter[RequestT],
        error_representor: ErrorRepresentor[RequestT],
        mediator_factory: Callable[[Request, int], Mediator],
    ):
        self._router = router
        self._error_representor = error_representor
        self._mediator_factory = mediator_factory

    def send(self, request: RequestT) -> Any:
        return self._send_inner(request, 0)

    def send_chaining(self, request: RequestT, search_offset: int) -> Any:
        return self._send_inner(request, search_offset)

    def _send_inner(self, request: RequestT, search_offset: int) -> Any:
        exceptions: list[CannotProvide] = []
        next_offset = search_offset
        mediator = self._mediator_factory(request, next_offset)
        while True:
            try:
                handler, next_offset = self._router.route_handler(mediator, request, next_offset)
            except StopIteration:
                exc = AggregateCannotProvide.make(
                    self._error_representor.get_provider_not_found_description(request),
                    exceptions,
                    is_demonstrative=True,
                )
                self._attach_request_context_notes(exc, request)
                self._attach_sub_exceptions_notes(exc, exceptions)
                raise exc from None
            except CannotProvide:
                raise RuntimeError("RequestChecker raises CannotProvide")

            mediator = self._mediator_factory(request, next_offset)
            try:
                response = handler(mediator, request)
            except CannotProvide as e:
                if e.is_terminal:
                    raise self._attach_request_context_notes(e, request)
                exceptions.append(e)
                continue

            return response

    def _attach_request_context_notes(self, exc: E, request: RequestT) -> E:
        notes = self._error_representor.get_request_context_notes(request)
        for note in notes:
            add_note(exc, note)
        return exc

    def _attach_sub_exceptions_notes(self, exc: E, sub_exceptions: Iterable[CannotProvide]) -> E:
        for sub_exc in sub_exceptions:
            if sub_exc.parent_notes_gen is not None:
                for note in sub_exc.parent_notes_gen():
                    add_note(exc, note)
        return exc


class RecursionResolver(ABC, Generic[RequestT, ResponseT]):
    @abstractmethod
    def track_request(self, request: RequestT) -> Optional[ResponseT]:
        ...

    @abstractmethod
    def track_response(self, request: RequestT, response: ResponseT) -> None:
        ...


class RecursiveRequestBus(BasicRequestBus[RequestT, ResponseT], Generic[RequestT, ResponseT]):
    __slots__ = (*BasicRequestBus.__slots__, "_recursion_resolver")

    def __init__(
        self,
        router: RequestRouter[RequestT],
        error_representor: ErrorRepresentor[RequestT],
        mediator_factory: Callable[[Request, int], Mediator],
        recursion_resolver: RecursionResolver[RequestT, ResponseT],
    ):
        super().__init__(router, error_representor, mediator_factory)
        self._recursion_resolver = recursion_resolver

    def send(self, request: RequestT) -> Any:
        stub = self._recursion_resolver.track_request(request)
        if stub is not None:
            return stub

        result = self._send_inner(request, 0)
        self._recursion_resolver.track_response(request, result)
        return result
