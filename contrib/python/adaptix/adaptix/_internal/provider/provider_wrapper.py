import itertools
from collections.abc import Sequence
from enum import Enum
from typing import TypeVar

from .essential import Mediator, Provider, Request, RequestChecker, RequestHandler, RequestHandlerRegisterRecord

T = TypeVar("T")


class ConcatProvider(Provider):
    def __init__(self, *providers: Provider):
        self._providers = providers

    def get_request_handlers(self) -> Sequence[RequestHandlerRegisterRecord]:
        return list(
            itertools.chain.from_iterable(
                provider.get_request_handlers()
                for provider in self._providers
            ),
        )

    def __repr__(self):
        return f"{type(self).__name__}({self._providers})"


class Chain(Enum):
    FIRST = "FIRST"
    LAST = "LAST"


RequestT = TypeVar("RequestT", bound=Request)
ResponseT = TypeVar("ResponseT")


class ChainingProvider(Provider):
    def __init__(self, chain: Chain, provider: Provider):
        self._chain = chain
        self._provider = provider

    def _make_chain(self, first, second):
        def chain_processor(data):
            return second(first(data))

        return chain_processor

    def _wrap_handler(self, handler: RequestHandler[ResponseT, RequestT]) -> RequestHandler[ResponseT, RequestT]:
        def chaining_handler(mediator: Mediator[ResponseT], request: RequestT) -> ResponseT:
            current_processor = handler(mediator, request)
            next_processor = mediator.provide_from_next()

            if self._chain == Chain.FIRST:
                return self._make_chain(current_processor, next_processor)
            if self._chain == Chain.LAST:
                return self._make_chain(next_processor, current_processor)
            raise ValueError

        return chaining_handler

    def get_request_handlers(self) -> Sequence[tuple[type[Request], RequestChecker, RequestHandler]]:
        return [
            (request_cls, checker, self._wrap_handler(handler))
            for request_cls, checker, handler in self._provider.get_request_handlers()
        ]
