from typing import Any, Callable, Mapping, Type

from mypy.plugin import MethodContext
from mypy.types import AnyType
from mypy.types import Type as MypyType
from mypy.types import TypeOfAny

_MethodCallback = Callable[[Any, MethodContext], MypyType]  # type: ignore


def error_to_any(
    error_map: Mapping[Type[Exception], str],
) -> Callable[[_MethodCallback], _MethodCallback]:
    """
    Decorator for ``mypy`` callbacks to catch given errors.

    We use to show custom messages for given exceptions.
    If other exceptions are present, then we show just an error message.
    """
    def decorator(func: _MethodCallback) -> _MethodCallback:
        def factory(self, ctx: MethodContext) -> MypyType:
            try:
                return func(self, ctx)
            except Exception as ex:
                ctx.api.fail(
                    error_map.get(type(ex), str(ex)),
                    ctx.context,
                )
                return AnyType(TypeOfAny.from_error)
        return factory
    return decorator
