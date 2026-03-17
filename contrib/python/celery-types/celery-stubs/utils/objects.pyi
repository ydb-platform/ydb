from collections.abc import Callable
from contextlib import AbstractContextManager
from types import TracebackType
from typing import Any, Generic, ParamSpec, TypeVar, overload

from typing_extensions import Never, override

__all__ = ("Bunch", "FallbackContext", "getitem_property", "mro_lookup")

class Bunch:
    def __init__(self, **kwargs: Any) -> None: ...

def mro_lookup(
    cls: type[Any],
    attr: str,
    stop: set[type[Any]] | None = None,
    monkey_patched: list[Any] | None = None,
) -> type[Any] | None: ...

_P = ParamSpec("_P")
_T_co = TypeVar("_T_co", bound=object, covariant=True)

class FallbackContext(
    AbstractContextManager[_T_co],
    Generic[_T_co, _P],
):
    provided: _T_co | None
    fallback: Callable[_P, AbstractContextManager[_T_co]] | None
    fb_args: tuple[Any, ...]
    fb_kwargs: dict[str, Any]
    @overload
    def __new__(
        cls,
        provided: None,
        fallback: None,
    ) -> Never: ...
    @overload
    def __new__(
        cls,
        provided: _T_co,
        fallback: None,
    ) -> FallbackContext[_T_co, _P]: ...
    @overload
    def __new__(
        cls,
        provided: None,
        fallback: Callable[_P, AbstractContextManager[_T_co]],
        *fb_args: _P.args,
        **fb_kwargs: _P.kwargs,
    ) -> FallbackContext[_T_co, _P]: ...
    @override
    def __enter__(self) -> _T_co: ...
    @override
    def __exit__(
        self,
        typ: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None: ...

class getitem_property:
    path: list[str] | None
    key: str
    __doc__: str | None

    def __init__(self, keypath: str, doc: str | None = None) -> None: ...
    def _path(self, obj: Any) -> dict[str, Any]: ...
    def __get__(self, obj: Any, type: Any | None = None) -> Any: ...
    def __set__(self, obj: Any, value: Any) -> None: ...
