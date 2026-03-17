import types
from typing import Any, Callable, Iterator, TypeVar

from typing_extensions import Self

F = TypeVar("F", bound=Callable[..., None])


class EventHandler:
    def __init__(self, once=False):
        # type: (bool) -> None
        self._listeners = []  # type: list[F]
        self._once = once

    def __contains__(self, listener):
        # type: (F) -> bool
        return listener in self._listeners

    def __iter__(self):
        # type: () -> Iterator[F]
        return iter(self._listeners)

    def __iadd__(self, listener):
        # type: (F) -> Self
        self._listeners.append(listener)
        return self

    def __isub__(self, listener):
        # type: (F) -> Self
        self._listeners.remove(listener)
        return self

    def __len__(self):
        return len(self._listeners)

    def notify(self, *args, **kwargs):
        # type: (Any, Any) -> None
        for listener in self._listeners[:]:
            if self._once:
                self._listeners.remove(listener)
            listener(*args, **kwargs)

    @staticmethod
    def is_system(listener):
        # type: (F) -> bool
        from office365.runtime.client_request import ClientRequest
        from office365.runtime.client_runtime_context import ClientRuntimeContext

        if isinstance(listener, types.MethodType):
            return isinstance(listener.__self__, (ClientRequest, ClientRuntimeContext))
        if isinstance(listener, types.FunctionType):
            return listener.__module__ == ClientRuntimeContext.__module__
        else:
            raise ValueError("Invalid listener type")
