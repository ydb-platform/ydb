from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Callable, Optional, TypeVar

from ..compat import CompatBaseExceptionGroup
from ..provider.essential import (
    AggregateCannotProvide,
    CannotProvide,
    Mediator,
    Provider,
    Request,
    RequestHandlerRegisterRecord,
)
from ..provider.request_checkers import AlwaysTrueRequestChecker
from ..tree_renderer import TreeRendererConfig
from ..utils import add_note, copy_exception_dunders, with_module
from .base_retort import BaseRetort
from .builtin_mediator import BuiltinMediator, RequestBus
from .error_renderer import BuiltinErrorRenderer, ErrorRenderer
from .request_bus import BasicRequestBus, ErrorRepresentor, RecursionResolver, RecursiveRequestBus, RequestRouter
from .routers import CheckerAndHandler


@with_module("adaptix")
class ProviderNotFoundError(Exception):
    def __init__(self, message: str, description: Optional[str] = None):
        self.message = message
        self.description = description

    def __str__(self):
        if self.description is not None:
            return self.message + "\n" + self.description
        return self.message


T = TypeVar("T")
RequestT = TypeVar("RequestT", bound=Request)


default_error_renderer = BuiltinErrorRenderer(TreeRendererConfig())


class SearchingRetort(BaseRetort, Provider, ABC):
    """A retort that can operate as Retort but have no predefined providers and no high-level user interface"""

    def __init__(
        self,
        *,
        recipe: Iterable[Provider] = (),
        error_renderer: Optional[ErrorRenderer] = default_error_renderer,
    ):
        self._error_renderer = error_renderer
        super().__init__(recipe=recipe)

    def _provide_from_recipe(self, request: Request[T]) -> T:
        return self._create_mediator(request).provide(request)

    def get_request_handlers(self) -> Sequence[RequestHandlerRegisterRecord]:
        def retort_request_handler(mediator, request):
            return self._provide_from_recipe(request)

        request_classes = {
            request_cls
            for provider in self._get_full_recipe()
            for request_cls, checker, handler in provider.get_request_handlers()
        }
        return [
            (request_class, AlwaysTrueRequestChecker(), retort_request_handler)
            for request_class in request_classes
        ]

    def _exception_walk(self, exc: BaseException) -> Iterable[BaseException]:
        yield exc
        if isinstance(exc, CompatBaseExceptionGroup):
            for sub_exc in exc.exceptions:
                yield from self._exception_walk(sub_exc)
        if exc.__cause__ is not None:
            yield from self._exception_walk(exc.__cause__)
        if exc.__context__ is not None:
            yield from self._exception_walk(exc.__context__)

    def _facade_provide(self, request: Request[T], *, error_message: str) -> T:
        try:
            return self._provide_from_recipe(request)
        except CannotProvide as e:
            raise self._get_facade_error(e, error_message)

    def _get_facade_error(self, e: CannotProvide, error_message: str) -> Exception:
        cause = self._get_exception_cause(e)

        if self._error_renderer is not None:
            exception = ProviderNotFoundError(
                error_message,
                self._error_renderer.render(cause) if cause is not None else None,
            )
            exception.__cause__ = None
            return exception

        exception = ProviderNotFoundError(error_message)
        if cause is not None:
            for sub_exc in self._exception_walk(cause):
                sub_exc.__traceback__ = None
            add_note(exception, "Note: The attached exception above contains verbose description of the problem")

        exception.__cause__ = cause
        return exception

    def _get_exception_cause(self, exc: CannotProvide) -> Optional[CannotProvide]:
        if isinstance(exc, AggregateCannotProvide):
            return self._extract_demonstrative_exc(exc)
        return exc if exc.is_demonstrative else None

    def _extract_demonstrative_exc(self, exc: AggregateCannotProvide) -> Optional[CannotProvide]:
        demonstrative_exc_list: list[CannotProvide] = []
        for sub_exc in exc.exceptions:
            if isinstance(sub_exc, AggregateCannotProvide):
                sub_exc = self._extract_demonstrative_exc(sub_exc)  # type: ignore[assignment]  # noqa: PLW2901
                if sub_exc is not None:
                    demonstrative_exc_list.append(sub_exc)
            elif sub_exc.is_demonstrative:
                demonstrative_exc_list.append(sub_exc)

        if not exc.is_demonstrative and not demonstrative_exc_list:
            return None
        new_exc = exc.derive_upcasting(demonstrative_exc_list)
        copy_exception_dunders(source=exc, target=new_exc)
        return new_exc

    def _calculate_derived(self) -> None:
        super()._calculate_derived()
        self._request_cls_to_router = self._create_request_cls_to_router(self._full_recipe)
        self._request_cls_to_error_representor = {
            request_cls: self._create_error_representor(request_cls)
            for request_cls in self._request_cls_to_router
        }
        self._call_cache: dict[Any, Any] = {}

    def _create_request_cls_to_router(self, full_recipe: Sequence[Provider]) -> Mapping[type[Request], RequestRouter]:
        request_cls_to_checkers_and_handlers: defaultdict[type[Request], list[CheckerAndHandler]] = defaultdict(list)
        for provider in full_recipe:
            for request_cls, checker, handler in provider.get_request_handlers():
                request_cls_to_checkers_and_handlers[request_cls].append((checker, handler))

        return {
            request_cls: self._create_router(request_cls, checkers_and_handlers)
            for request_cls, checkers_and_handlers in request_cls_to_checkers_and_handlers.items()
        }

    @abstractmethod
    def _create_router(
        self,
        request_cls: type[RequestT],
        checkers_and_handlers: Sequence[CheckerAndHandler],
    ) -> RequestRouter[RequestT]:
        ...

    @abstractmethod
    def _create_error_representor(self, request_cls: type[RequestT]) -> ErrorRepresentor[RequestT]:
        ...

    @abstractmethod
    def _create_recursion_resolver(self, request_cls: type[RequestT]) -> Optional[RecursionResolver[RequestT, Any]]:
        ...

    def _create_request_bus(
        self,
        request_cls: type[RequestT],
        router: RequestRouter[RequestT],
        mediator_factory: Callable[[Request, int], Mediator],
    ) -> RequestBus:
        error_representor = self._request_cls_to_error_representor[request_cls]
        recursion_resolver = self._create_recursion_resolver(request_cls)
        if recursion_resolver is not None:
            return RecursiveRequestBus(
                router=router,
                error_representor=error_representor,
                mediator_factory=mediator_factory,
                recursion_resolver=recursion_resolver,
            )
        return BasicRequestBus(
            router=router,
            error_representor=error_representor,
            mediator_factory=mediator_factory,
        )

    def _create_no_request_bus_error_maker(self) -> Callable[[Request], CannotProvide]:
        def no_request_bus_error_maker(request: Request) -> CannotProvide:
            return CannotProvide(f"Cannot satisfy {type(request)}")

        return no_request_bus_error_maker

    def _create_mediator(self, init_request: Request[T]) -> Mediator[T]:
        request_buses: Mapping[type[Request], RequestBus]
        no_request_bus_error_maker = self._create_no_request_bus_error_maker()
        call_cache = self._call_cache

        def mediator_factory(request, search_offset):
            return BuiltinMediator(
                request_buses=request_buses,
                request=request,
                search_offset=search_offset,
                no_request_bus_error_maker=no_request_bus_error_maker,
                call_cache=call_cache,
            )

        request_buses = {
            request_cls: self._create_request_bus(request_cls, router, mediator_factory)
            for request_cls, router in self._request_cls_to_router.items()
        }
        return mediator_factory(init_request, 0)
