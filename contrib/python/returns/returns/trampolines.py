from collections.abc import Callable
from functools import wraps
from typing import Generic, TypeVar, final

from typing_extensions import ParamSpec

_ReturnType = TypeVar('_ReturnType')
_FuncParams = ParamSpec('_FuncParams')


@final
class Trampoline(Generic[_ReturnType]):
    """
    Represents a wrapped function call.

    Primitive to convert recursion into an actual object.
    """

    __slots__ = ('args', 'func', 'kwargs')

    def __init__(  # noqa: WPS451
        self,
        func: Callable[_FuncParams, _ReturnType],
        /,  # We use pos-only here to be able to store `kwargs` correctly.
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> None:
        """Save function and given arguments."""
        self.func = getattr(func, '_orig_func', func)
        self.args = args
        self.kwargs = kwargs

    def __call__(self) -> _ReturnType:
        """Call wrapped function with given arguments."""
        return self.func(*self.args, **self.kwargs)


def trampoline(
    func: Callable[_FuncParams, _ReturnType | Trampoline[_ReturnType]],
) -> Callable[_FuncParams, _ReturnType]:
    """
    Convert functions using recursion to regular functions.

    Trampolines allow to unwrap recursion into a regular ``while`` loop,
    which does not raise any ``RecursionError`` ever.

    Since python does not have TCO (tail call optimization),
    we have to provide this helper.

    This is done by wrapping real function calls into
    :class:`returns.trampolines.Trampoline` objects:

    .. code:: python

        >>> from typing import Union
        >>> from returns.trampolines import Trampoline, trampoline

        >>> @trampoline
        ... def get_factorial(
        ...     for_number: int,
        ...     current_number: int = 0,
        ...     acc: int = 1,
        ... ) -> Union[int, Trampoline[int]]:
        ...     assert for_number >= 0
        ...     if for_number <= current_number:
        ...         return acc
        ...     return Trampoline(
        ...         get_factorial,
        ...         for_number,
        ...         current_number=current_number + 1,
        ...         acc=acc * (current_number + 1),
        ...     )

        >>> assert get_factorial(0) == 1
        >>> assert get_factorial(3) == 6
        >>> assert get_factorial(4) == 24

    See also:
        - eli.thegreenplace.net/2017/on-recursion-continuations-and-trampolines
        - https://en.wikipedia.org/wiki/Tail_call

    """

    @wraps(func)
    def decorator(
        *args: _FuncParams.args,
        **kwargs: _FuncParams.kwargs,
    ) -> _ReturnType:
        trampoline_result = func(*args, **kwargs)
        while isinstance(trampoline_result, Trampoline):
            trampoline_result = trampoline_result()
        return trampoline_result

    decorator._orig_func = func  # type: ignore[attr-defined]  # noqa: SLF001
    return decorator
