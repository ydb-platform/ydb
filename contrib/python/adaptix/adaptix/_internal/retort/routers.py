from collections.abc import Sequence
from itertools import islice
from typing import Optional, TypeVar, Union

from ..common import TypeHint
from ..provider.essential import DirectMediator, Request, RequestChecker, RequestHandler
from ..provider.loc_stack_filtering import ExactOriginLSC
from ..provider.located_request import LocatedRequest, LocatedRequestChecker
from ..type_tools import normalize_type
from .request_bus import RequestRouter

RequestT = TypeVar("RequestT", bound=Request)
CheckerAndHandler = tuple[RequestChecker, RequestHandler]


class SimpleRouter(RequestRouter[RequestT]):
    __slots__ = ("_checkers_and_handlers", )

    def __init__(self, checkers_and_handlers: Sequence[CheckerAndHandler]):
        self._checkers_and_handlers = checkers_and_handlers

    def route_handler(
        self,
        mediator: DirectMediator,
        request: RequestT,
        search_offset: int,
    ) -> tuple[RequestHandler, int]:
        for i, (checker, handler) in enumerate(
            islice(self._checkers_and_handlers, search_offset, None),
            start=search_offset,
        ):
            if checker.check_request(mediator, request):
                return handler, i + 1
        raise StopIteration

    def get_max_offset(self) -> int:
        return len(self._checkers_and_handlers)


OriginToHandler = dict[TypeHint, RequestHandler]
LRRoutingItem = Union[CheckerAndHandler, OriginToHandler]


class LocatedRequestRouter(RequestRouter[LocatedRequest]):
    __slots__ = ("_items", )

    def __init__(self, items: Sequence[Union[CheckerAndHandler, OriginToHandler]]):
        self._items = items

    def route_handler(
        self,
        mediator: DirectMediator,
        request: LocatedRequest,
        search_offset: int,
    ) -> tuple[RequestHandler, int]:
        try:
            origin = normalize_type(request.last_loc.type).origin
        except ValueError:
            origin = object()

        for i, routing_item in enumerate(
            islice(self._items, search_offset, None),
            start=search_offset,
        ):
            if type(routing_item) is tuple:
                if routing_item[0].check_request(mediator, request):
                    return routing_item[1], i + 1
            else:
                handler = routing_item.get(origin)  # type: ignore[union-attr]
                if handler is not None:
                    return handler, i + 1
        raise StopIteration

    def get_max_offset(self) -> int:
        return len(self._items)


class ExactOriginCombiner:
    def __init__(self) -> None:
        self._combo: OriginToHandler = {}

    def _stop_combo(self, checker_and_handler: Optional[CheckerAndHandler]) -> Sequence[LRRoutingItem]:
        result: list[LRRoutingItem] = []
        if self._combo:
            if len(self._combo) == 1:
                [(origin, handler)] = self._combo.items()
                result.append((LocatedRequestChecker(ExactOriginLSC(origin)), handler))
            else:
                result.append(self._combo)
            self._combo = {}

        if checker_and_handler is not None:
            result.append(checker_and_handler)
        return result

    def register_item(self, checker_and_handler: CheckerAndHandler) -> Sequence[LRRoutingItem]:
        checker, handler = checker_and_handler
        if isinstance(checker, LocatedRequestChecker) and isinstance(checker.loc_stack_checker, ExactOriginLSC):
            origin = checker.loc_stack_checker.origin
            if origin in self._combo:
                return self._stop_combo(checker_and_handler)
            self._combo[origin] = handler
            return []

        return self._stop_combo(checker_and_handler)

    def finalize(self) -> Sequence[LRRoutingItem]:
        return self._stop_combo(None)


def create_router_for_located_request(
    checkers_and_handlers: Sequence[CheckerAndHandler],
) -> RequestRouter[LocatedRequest]:
    items: list[Union[CheckerAndHandler, OriginToHandler]] = []

    combiner = ExactOriginCombiner()
    for checkers_and_handler in checkers_and_handlers:
        items.extend(combiner.register_item(checkers_and_handler))
    items.extend(combiner.finalize())

    return LocatedRequestRouter(items)
