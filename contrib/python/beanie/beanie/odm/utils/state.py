import inspect
from functools import wraps
from typing import TYPE_CHECKING, TypeVar

from typing_extensions import ParamSpec

from beanie.exceptions import StateManagementIsTurnedOff, StateNotSaved

if TYPE_CHECKING:
    from beanie.odm.documents import AnyDocMethod, AsyncDocMethod, DocType

P = ParamSpec("P")
R = TypeVar("R")


def check_if_state_saved(self: "DocType"):
    if not self.use_state_management():
        raise StateManagementIsTurnedOff(
            "State management is turned off for this document"
        )
    if self._saved_state is None:
        raise StateNotSaved("No state was saved")


def saved_state_needed(
    f: "AnyDocMethod[DocType, P, R]",
) -> "AnyDocMethod[DocType, P, R]":
    @wraps(f)
    def sync_wrapper(self: "DocType", *args: P.args, **kwargs: P.kwargs) -> R:
        check_if_state_saved(self)
        return f(self, *args, **kwargs)

    @wraps(f)
    async def async_wrapper(
        self: "DocType", *args: P.args, **kwargs: P.kwargs
    ) -> R:
        check_if_state_saved(self)
        # type ignore because there is no nice/proper way to annotate both sync
        # and async case without parametrized TypeVar, which is not supported
        return await f(self, *args, **kwargs)  # type: ignore[misc]

    if inspect.iscoroutinefunction(f):
        # type ignore because there is no nice/proper way to annotate both sync
        # and async case without parametrized TypeVar, which is not supported
        return async_wrapper  # type: ignore[return-value]
    return sync_wrapper


def check_if_previous_state_saved(self: "DocType"):
    if not self.use_state_management():
        raise StateManagementIsTurnedOff(
            "State management is turned off for this document"
        )
    if not self.state_management_save_previous():
        raise StateManagementIsTurnedOff(
            "State management's option to save previous state is turned off for this document"
        )


def previous_saved_state_needed(
    f: "AnyDocMethod[DocType, P, R]",
) -> "AnyDocMethod[DocType, P, R]":
    @wraps(f)
    def sync_wrapper(self: "DocType", *args: P.args, **kwargs: P.kwargs) -> R:
        check_if_previous_state_saved(self)
        return f(self, *args, **kwargs)

    @wraps(f)
    async def async_wrapper(
        self: "DocType", *args: P.args, **kwargs: P.kwargs
    ) -> R:
        check_if_previous_state_saved(self)
        # type ignore because there is no nice/proper way to annotate both sync
        # and async case without parametrized TypeVar, which is not supported
        return await f(self, *args, **kwargs)  # type: ignore[misc]

    if inspect.iscoroutinefunction(f):
        # type ignore because there is no nice/proper way to annotate both sync
        # and async case without parametrized TypeVar, which is not supported
        return async_wrapper  # type: ignore[return-value]
    return sync_wrapper


def save_state_after(
    f: "AsyncDocMethod[DocType, P, R]",
) -> "AsyncDocMethod[DocType, P, R]":
    @wraps(f)
    async def wrapper(self: "DocType", *args: P.args, **kwargs: P.kwargs) -> R:
        result = await f(self, *args, **kwargs)
        self._save_state()
        return result

    return wrapper
