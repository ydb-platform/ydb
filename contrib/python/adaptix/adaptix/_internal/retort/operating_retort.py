from collections.abc import Iterable, Sequence
from typing import Any, Callable, Generic, Optional, TypeVar

from ..conversion.request_cls import CoercerRequest, LinkingRequest
from ..morphing.json_schema.definitions import JSONSchema
from ..morphing.json_schema.request_cls import InlineJSONSchemaRequest, JSONSchemaRequest, RefSourceRequest
from ..morphing.request_cls import DumperRequest, LoaderRequest
from ..provider.essential import Mediator, Provider, Request
from ..provider.loc_stack_tools import format_loc_stack
from ..provider.located_request import LocatedRequest, LocatedRequestMethodsProvider
from ..provider.location import AnyLoc
from ..provider.methods_provider import method_handler
from .request_bus import ErrorRepresentor, RecursionResolver, RequestRouter
from .routers import CheckerAndHandler, SimpleRouter, create_router_for_located_request
from .searching_retort import SearchingRetort


class FuncWrapper:
    __slots__ = ("__call__", "_key")

    def __init__(self, key):
        self._key = key
        self.__call__ = None

    def set_func(self, func):
        self.__call__ = func

    def __eq__(self, other):
        if isinstance(other, FuncWrapper):
            return self._key == other._key
        return NotImplemented

    def __hash__(self):
        return hash(self._key)


CallableT = TypeVar("CallableT", bound=Callable)


class LocatedRequestCallableRecursionResolver(RecursionResolver[LocatedRequest, CallableT], Generic[CallableT]):
    def __init__(self) -> None:
        self._loc_to_stub: dict[AnyLoc, FuncWrapper] = {}

    def track_request(self, request: LocatedRequest) -> Optional[Any]:
        last_loc = request.last_loc
        if sum(loc == last_loc for loc in request.loc_stack) == 1:
            return None

        if last_loc in self._loc_to_stub:
            return self._loc_to_stub[last_loc]
        stub = FuncWrapper(last_loc)
        self._loc_to_stub[last_loc] = stub
        return stub

    def track_response(self, request: LocatedRequest, response: CallableT) -> None:
        last_loc = request.last_loc
        if last_loc in self._loc_to_stub:
            self._loc_to_stub.pop(last_loc).set_func(response)


RequestT = TypeVar("RequestT", bound=Request)
LocatedRequestT = TypeVar("LocatedRequestT", bound=LocatedRequest)


class BaseRequestErrorRepresentor(ErrorRepresentor[RequestT], Generic[RequestT]):
    def __init__(self, not_found_desc: str):
        self._not_found_desc = not_found_desc

    def get_request_context_notes(self, request: RequestT) -> Iterable[str]:
        return ()

    def get_provider_not_found_description(self, request: RequestT) -> str:
        return self._not_found_desc


class LocatedRequestErrorRepresentor(BaseRequestErrorRepresentor[LocatedRequestT], Generic[LocatedRequestT]):
    def get_request_context_notes(self, request: LocatedRequestT) -> Iterable[str]:
        loc_stack_desc = format_loc_stack(request.loc_stack, always_wrap_with_brackets=True)
        yield f"Location: {loc_stack_desc}"


class LinkingRequestErrorRepresentor(ErrorRepresentor[LinkingRequest]):
    def get_request_context_notes(self, request: RequestT) -> Iterable[str]:
        return ()

    def get_provider_not_found_description(self, request: LinkingRequest) -> str:
        dst_desc = format_loc_stack(request.destination)
        return f"Cannot find paired field of {dst_desc} for linking"


class CoercerRequestErrorRepresentor(BaseRequestErrorRepresentor[CoercerRequest]):
    def get_request_context_notes(self, request: CoercerRequest) -> Iterable[str]:
        src_desc = format_loc_stack(request.src)
        dst_desc = format_loc_stack(request.dst)
        yield f"Linking: {src_desc} ──▷ {dst_desc}"


class JSONSchemaMiddlewareProvider(LocatedRequestMethodsProvider):
    @method_handler
    def provide_json_schema(self, mediator: Mediator, request: JSONSchemaRequest) -> JSONSchema:
        loc_stack = request.loc_stack
        ctx = request.ctx
        json_schema = mediator.provide_from_next()
        inline = mediator.mandatory_provide(InlineJSONSchemaRequest(loc_stack=loc_stack, ctx=ctx))
        if inline:
            return json_schema
        ref_source = mediator.mandatory_provide(RefSourceRequest(loc_stack=loc_stack, json_schema=json_schema, ctx=ctx))
        return JSONSchema(ref=ref_source)


class OperatingRetort(SearchingRetort):
    """A retort that can operate as Retort but have no predefined providers and no high-level user interface"""

    def _get_recipe_head(self) -> Sequence[Provider]:
        return (
            JSONSchemaMiddlewareProvider(),
        )

    def _create_router(
        self,
        request_cls: type[RequestT],
        checkers_and_handlers: Sequence[CheckerAndHandler],
    ) -> RequestRouter[RequestT]:
        if issubclass(request_cls, LocatedRequest):
            return create_router_for_located_request(checkers_and_handlers)  # type: ignore[return-value]
        return SimpleRouter(checkers_and_handlers)

    def _create_error_representor(self, request_cls: type[RequestT]) -> ErrorRepresentor[RequestT]:
        if issubclass(request_cls, LoaderRequest):
            return LocatedRequestErrorRepresentor("Cannot find loader")
        if issubclass(request_cls, DumperRequest):
            return LocatedRequestErrorRepresentor("Cannot find dumper")

        if issubclass(request_cls, CoercerRequest):
            return CoercerRequestErrorRepresentor("Cannot find coercer")  # type: ignore[return-value]
        if issubclass(request_cls, LinkingRequest):
            return LinkingRequestErrorRepresentor()  # type: ignore[return-value]

        return BaseRequestErrorRepresentor(f"Cannot satisfy {request_cls}")

    def _create_recursion_resolver(self, request_cls: type[RequestT]) -> Optional[RecursionResolver[RequestT, Any]]:
        if issubclass(request_cls, (LoaderRequest, DumperRequest)):
            return LocatedRequestCallableRecursionResolver()  # type: ignore[return-value]
        return None
