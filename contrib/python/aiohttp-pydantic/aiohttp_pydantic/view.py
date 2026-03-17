from functools import update_wrapper
from inspect import iscoroutinefunction
from typing import Any, Callable, Generator, Iterable, Set, ClassVar
import warnings

from aiohttp.abc import AbstractView
from aiohttp.hdrs import METH_ALL
from aiohttp.web_exceptions import HTTPMethodNotAllowed
from aiohttp.web_response import StreamResponse
from pydantic import ValidationError

from .decorator import json_response_error
from .injectors import (
    AbstractInjector,
    BodyGetter,
    HeadersGetter,
    MatchInfoGetter,
    QueryGetter,
    _parse_func_signature,
    CONTEXT,
    Group,
)


class PydanticView(AbstractView):
    """
    An AIOHTTP View that validate request using function annotations.
    """

    # Allowed HTTP methods; overridden when subclassed.
    allowed_methods: ClassVar[Set[str]] = {}

    async def _iter(self) -> StreamResponse:
        if (method_name := self.request.method) not in self.allowed_methods:
            self._raise_allowed_methods()
        return await getattr(self, method_name.lower())()

    def __await__(self) -> Generator[Any, None, StreamResponse]:
        return self._iter().__await__()

    def __init_subclass__(cls, **kwargs) -> None:
        """Define allowed methods and decorate handlers.

        Handlers are decorated if and only if they directly bound on the PydanticView class or
        PydanticView subclass. This prevents that methods are decorated multiple times and that method
        defined in aiohttp.View parent class is decorated.
        """

        cls.allowed_methods = {
            meth_name for meth_name in METH_ALL if hasattr(cls, meth_name.lower())
        }

        for meth_name in METH_ALL:
            if meth_name.lower() in vars(cls):
                handler = getattr(cls, meth_name.lower())
                # FUTURE: remove cls.parse_func_signature, cls._parse_func_signature
                #   remove in this module inject_params, _inject_params and use:
                #   from .decorator import inject_params
                #   decorated_handler = inject_params.in_method(handler)
                decorated_handler = _inject_params(handler, cls._parse_func_signature)
                setattr(cls, meth_name.lower(), decorated_handler)

    def _raise_allowed_methods(self) -> None:
        raise HTTPMethodNotAllowed(self.request.method, self.allowed_methods)

    def raise_not_allowed(self) -> None:
        warnings.warn(
            "PydanticView.raise_not_allowed is deprecated and renamed _raise_allowed_methods",
            DeprecationWarning,
            stacklevel=2,
        )
        self._raise_allowed_methods()

    @staticmethod
    def parse_func_signature(func: Callable) -> Iterable[AbstractInjector]:
        warnings.warn(
            "PydanticView.parse_func_signature is deprecated and will be removed in a future version",
            DeprecationWarning,
            stacklevel=2,
        )
        return PydanticView._parse_func_signature(func)

    @staticmethod
    def _parse_func_signature(func: Callable) -> Iterable[AbstractInjector]:
        path_args, body_args, qs_args, header_args, defaults = _parse_func_signature(
            func
        )
        injectors = []

        def default_value(args: dict) -> dict:
            """
            Returns the default values of args.
            """
            return {name: defaults[name] for name in args if name in defaults}

        if path_args:
            injectors.append(MatchInfoGetter(path_args, default_value(path_args)))
        if body_args:
            injectors.append(BodyGetter(body_args, default_value(body_args)))
        if qs_args:
            injectors.append(QueryGetter(qs_args, default_value(qs_args)))
        if header_args:
            injectors.append(HeadersGetter(header_args, default_value(header_args)))
        return injectors

    async def on_validation_error(
        self, exception: ValidationError, context: CONTEXT
    ) -> StreamResponse:
        """
        This method is a hook to intercept ValidationError.

        This hook can be redefined to return a custom HTTP response error.
        The exception is a pydantic.ValidationError and the context is "body",
        "headers", "path" or "query string"
        """
        return await json_response_error(exception, context)


def inject_params(
    handler, parse_func_signature: Callable[[Callable], Iterable[AbstractInjector]]
):
    """
    Decorator to unpack the query string, route path, body and http header in
    the parameters of the web handler regarding annotations.
    """
    warnings.warn(
        "aiohttp_pydantic.view.inject_params is deprecated. Use aiohttp_pydantic.decorator.inject_params",
        DeprecationWarning,
        stacklevel=2,
    )
    return _inject_params(handler, parse_func_signature)


def _inject_params(
    handler, parse_func_signature: Callable[[Callable], Iterable[AbstractInjector]]
):

    injectors = parse_func_signature(handler)

    async def wrapped_handler(self):
        args = []
        kwargs = {}
        for injector in injectors:
            try:
                if iscoroutinefunction(injector.inject):
                    await injector.inject(self.request, args, kwargs)
                else:
                    injector.inject(self.request, args, kwargs)
            except ValidationError as error:
                return await self.on_validation_error(error, injector.context)

        return await handler(self, *args, **kwargs)

    update_wrapper(wrapped_handler, handler)
    return wrapped_handler


def is_pydantic_view(obj) -> bool:
    """
    Return True if obj is a PydanticView subclass else False.
    """
    try:
        return issubclass(obj, PydanticView)
    except TypeError:
        return False


__all__ = (
    "AbstractInjector",
    "BodyGetter",
    "HeadersGetter",
    "MatchInfoGetter",
    "QueryGetter",
    "CONTEXT",
    "Group",
)
