from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast, overload

_T = TypeVar("_T")
_S = TypeVar("_S")

if TYPE_CHECKING:
    from collections.abc import Callable

    class _singledispatchmethod(functools.singledispatchmethod[_T]):  # noqa: N801
        pass

else:

    class _singledispatchmethod(  # noqa: N801
        functools.singledispatchmethod, Generic[_T]
    ):
        pass


class singledispatchmethod(_singledispatchmethod[_T]):  # noqa: N801
    def __init__(self, func: Callable[..., _T]) -> None:
        super().__init__(func)
        self.deferred_registrations: list[
            tuple[type[Any] | Callable[..., _T], Callable[..., _T] | None]
        ] = []

    @overload
    def register(
        self, cls: type[Any], method: None = None
    ) -> Callable[[Callable[..., _T]], Callable[..., _T]]: ...  # pragma: no cover
    @overload
    def register(
        self, cls: Callable[..., _T], method: None = None
    ) -> Callable[..., _T]: ...  # pragma: no cover

    @overload
    def register(
        self, cls: type[Any], method: Callable[..., _T]
    ) -> Callable[..., _T]: ...  # pragma: no cover

    def register(
        self,
        cls: type[Any] | Callable[..., _T],
        method: Callable[..., _T] | None = None,
    ) -> Callable[[Callable[..., _T]], Callable[..., _T]] | Callable[..., _T]:
        """generic_method.register(cls, func) -> func

        Registers a new implementation for the given *cls* on a *generic_method*.
        """
        if isinstance(cls, (classmethod, staticmethod)):
            first_annotation = {}
            for k, v in cls.__func__.__annotations__.items():
                first_annotation[k] = v
                break
            cls.__annotations__ = first_annotation

            # for globals in typing.get_type_hints() in Python 3.8 and 3.9
            if not hasattr(cls, "__wrapped__"):
                cls.__dict__["__wrapped__"] = cls.__func__
                # cls.__wrapped__ = cls.__func__

        try:
            return self.dispatcher.register(cast("type[Any]", cls), func=method)
        except (NameError, TypeError):  # NameError <= Py3.13, TypeError >= Py3.14
            self.deferred_registrations.append(
                (cls, method)  # pyright: ignore [reportArgumentType]
            )
            # TODO: Fix this....
            return method or cls  # pyright: ignore [reportReturnType]

    def __get__(self, obj: _S, cls: type[_S] | None = None) -> Callable[..., _T]:
        for registered_cls, registered_method in self.deferred_registrations:
            self.dispatcher.register(
                cast("type[Any]", registered_cls), func=registered_method
            )
        self.deferred_registrations = []
        return super().__get__(obj, cls=cls)
