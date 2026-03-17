__all__ = ["CONTAINER_NAME", "FromDishka", "inject"]

from collections.abc import Awaitable, Callable
from typing import (
    Any,
    Concatenate,
    Final,
    ParamSpec,
    TypeVar,
    cast,
    overload,
)

from aiogram_dialog import Data, DialogManager
from aiogram_dialog.api.entities import ChatEvent
from aiogram_dialog.api.internal import Widget
from aiogram_dialog.widgets.common import ManagedWidget

from dishka import AsyncContainer, FromDishka
from dishka.integrations.base import wrap_injection

CONTAINER_NAME: Final = "dishka_container"
ON_DIALOG_EVENT_LEN_ARGS: Final = 2

_ParamsP = ParamSpec("_ParamsP")
_ReturnT = TypeVar("_ReturnT", bound=Awaitable[Any])

_EventT = TypeVar("_EventT", bound=ChatEvent)
_OnDialogEventData = TypeVar("_OnDialogEventData", bound=Data)
_OnProcessResultEventResultT = TypeVar("_OnProcessResultEventResultT")

_WidgetItemT = TypeVar("_WidgetItemT")
_WidgetT = TypeVar("_WidgetT", bound=Widget)
_ManagedWidgetT = TypeVar("_ManagedWidgetT", bound=ManagedWidget[Any])
_DialogManagerT = TypeVar("_DialogManagerT", bound=DialogManager)


def _container_getter(
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> AsyncContainer:
    if not args:
        container = kwargs[CONTAINER_NAME]
    elif len(args) == ON_DIALOG_EVENT_LEN_ARGS:
        container = args[-1].middleware_data[CONTAINER_NAME]
    else:
        container = args[2].middleware_data[CONTAINER_NAME]
    # typing.cast is used because middleware_data is not typed
    return cast(AsyncContainer, container)


# overload for callback with 4 arguments and managed widget
@overload
def inject(
    func: Callable[
        Concatenate[
            _EventT,
            _ManagedWidgetT,
            _DialogManagerT,
            _WidgetItemT,
            _ParamsP,
        ],
        _ReturnT,
    ],
) -> Callable[
    [_EventT, _ManagedWidgetT, _DialogManagerT, _WidgetItemT],
    _ReturnT,
]:
    ...


# overload for callback with 3 arguments
@overload
def inject(
    func: Callable[
        Concatenate[
            _EventT,
            _WidgetT,
            _DialogManagerT,
            _ParamsP,
        ],
        _ReturnT,
    ],
) -> Callable[[_EventT, _WidgetT, _DialogManagerT], _ReturnT]:
    ...


# overload for on result event
# `type: ignore[overload-overlap]` is used because mypy is dumb
@overload
def inject(  # type: ignore[overload-overlap]
    func: Callable[
        Concatenate[
            _OnDialogEventData,
            _OnProcessResultEventResultT,
            _DialogManagerT,
            _ParamsP,
        ],
        _ReturnT,
    ],
) -> Callable[
    [_OnDialogEventData, _OnProcessResultEventResultT, _DialogManagerT],
    _ReturnT,
]:
    ...


# overload for on dialog event
@overload
def inject(
    func: Callable[
        Concatenate[
            _OnDialogEventData,
            _DialogManagerT,
            _ParamsP,
        ],
        _ReturnT,
    ],
) -> Callable[[_OnDialogEventData, _DialogManagerT], _ReturnT]:
    ...


# overload for getter and custom callbacks
@overload
def inject(func: Callable[..., _ReturnT]) -> Callable[..., _ReturnT]:
    ...


def inject(func: Callable[..., _ReturnT]) -> Callable[..., _ReturnT]:
    return wrap_injection(
        func=func,
        is_async=True,
        container_getter=_container_getter,
    )
