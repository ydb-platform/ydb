from __future__ import annotations

import inspect
import typing
import warnings
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from functools import partial
from typing import Any, Literal, cast, overload

from typing_extensions import Protocol, runtime_checkable

import temporalio.common

from ..types import (
    CallableSyncOrAsyncReturnNoneType,
    CallableSyncOrAsyncType,
    CallableType,
    MultiParamSpec,
    ProtocolReturnType,
    ReturnType,
)

__all__ = [
    "HandlerUnfinishedPolicy",
    "UnfinishedSignalHandlersWarning",
    "UnfinishedUpdateHandlersWarning",
    "UpdateMethodMultiParam",
    "query",
    "signal",
    "update",
]


class HandlerUnfinishedPolicy(Enum):
    """Actions taken if a workflow terminates with running handlers.

    Policy defining actions taken when a workflow exits while update or signal handlers are running.
    The workflow exit may be due to successful return, failure, cancellation, or continue-as-new.
    """

    WARN_AND_ABANDON = 1
    """Issue a warning in addition to abandoning."""
    ABANDON = 2
    """Abandon the handler.

    In the case of an update handler this means that the client will receive an error rather than
    the update result."""


class UnfinishedUpdateHandlersWarning(RuntimeWarning):
    """The workflow exited before all update handlers had finished executing."""


class UnfinishedSignalHandlersWarning(RuntimeWarning):
    """The workflow exited before all signal handlers had finished executing."""


@overload
def signal(
    fn: CallableSyncOrAsyncReturnNoneType,
) -> CallableSyncOrAsyncReturnNoneType: ...


@overload
def signal(
    *,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: str | None = None,
) -> Callable[
    [CallableSyncOrAsyncReturnNoneType], CallableSyncOrAsyncReturnNoneType
]: ...


@overload
def signal(
    *,
    name: str,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: str | None = None,
) -> Callable[
    [CallableSyncOrAsyncReturnNoneType], CallableSyncOrAsyncReturnNoneType
]: ...


@overload
def signal(
    *,
    dynamic: Literal[True],
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: str | None = None,
) -> Callable[
    [CallableSyncOrAsyncReturnNoneType], CallableSyncOrAsyncReturnNoneType
]: ...


def signal(
    fn: CallableSyncOrAsyncReturnNoneType | None = None,
    *,
    name: str | None = None,
    dynamic: bool | None = False,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: str | None = None,
) -> (
    Callable[[CallableSyncOrAsyncReturnNoneType], CallableSyncOrAsyncReturnNoneType]
    | CallableSyncOrAsyncReturnNoneType
):
    """Decorator for a workflow signal method.

    This is used on any async or non-async method that you wish to be called upon
    receiving a signal. If a function overrides one with this decorator, it too
    must be decorated.

    Signal methods can only have positional parameters. Best practice for
    non-dynamic signal methods is to only take a single object/dataclass
    argument that can accept more fields later if needed. Return values from
    signal methods are ignored.

    Args:
        fn: The function to decorate.
        name: Signal name. Defaults to method ``__name__``. Cannot be present
            when ``dynamic`` is present.
        dynamic: If true, this handles all signals not otherwise handled. The
            parameters of the method must be self, a string name, and a
            ``*args`` positional varargs. Cannot be present when ``name`` is
            present.
        unfinished_policy: Actions taken if a workflow terminates with
            a running instance of this handler.
        description: A short description of the signal that may appear in the UI/CLI.
    """

    def decorator(
        name: str | None,
        unfinished_policy: HandlerUnfinishedPolicy,
        fn: CallableSyncOrAsyncReturnNoneType,
    ) -> CallableSyncOrAsyncReturnNoneType:
        if not name and not dynamic:
            name = fn.__name__
        defn = _SignalDefinition(
            name=name,
            fn=fn,
            is_method=True,
            unfinished_policy=unfinished_policy,
            description=description,
        )
        setattr(fn, "__temporal_signal_definition", defn)
        if defn.dynamic_vararg:
            warnings.warn(
                "Dynamic signals with vararg third param is deprecated, use Sequence[RawValue]",
                DeprecationWarning,
                stacklevel=2,
            )
        return fn

    if not fn:
        if name is not None and dynamic:
            raise RuntimeError("Cannot provide name and dynamic boolean")
        return partial(decorator, name, unfinished_policy)
    else:
        return decorator(fn.__name__, unfinished_policy, fn)


@overload
def query(fn: CallableType) -> CallableType: ...


@overload
def query(
    *, name: str, description: str | None = None
) -> Callable[[CallableType], CallableType]: ...


@overload
def query(
    *, dynamic: Literal[True], description: str | None = None
) -> Callable[[CallableType], CallableType]: ...


@overload
def query(*, description: str) -> Callable[[CallableType], CallableType]: ...


def query(
    fn: CallableType | None = None,  # type: ignore[reportInvalidTypeVarUse]
    *,
    name: str | None = None,
    dynamic: bool | None = False,
    description: str | None = None,
):
    """Decorator for a workflow query method.

    This is used on any non-async method that expects to handle a query. If a
    function overrides one with this decorator, it too must be decorated.

    Query methods can only have positional parameters. Best practice for
    non-dynamic query methods is to only take a single object/dataclass
    argument that can accept more fields later if needed. The return value is
    the resulting query value. Query methods must not mutate any workflow state.

    Args:
        fn: The function to decorate.
        name: Query name. Defaults to method ``__name__``. Cannot be present
            when ``dynamic`` is present.
        dynamic: If true, this handles all queries not otherwise handled. The
            parameters of the method should be self, a string name, and a
            ``Sequence[RawValue]``. An older form of this accepted vararg
            parameters which will now warn. Cannot be present when ``name`` is
            present.
        description: A short description of the query that may appear in the UI/CLI.
    """

    def decorator(
        name: str | None,
        description: str | None,
        fn: CallableType,
        *,
        bypass_async_check: bool = False,
    ) -> CallableType:
        if not name and not dynamic:
            name = fn.__name__
        if not bypass_async_check and inspect.iscoroutinefunction(fn):
            warnings.warn(
                "Queries as async def functions are deprecated",
                DeprecationWarning,
                stacklevel=2,
            )
        defn = _QueryDefinition(
            name=name, fn=fn, is_method=True, description=description
        )
        setattr(fn, "__temporal_query_definition", defn)
        if defn.dynamic_vararg:
            warnings.warn(
                "Dynamic queries with vararg third param is deprecated, use Sequence[RawValue]",
                DeprecationWarning,
                stacklevel=2,
            )
        return fn

    if name is not None or dynamic or description:
        if name is not None and dynamic:
            raise RuntimeError("Cannot provide name and dynamic boolean")
        return partial(decorator, name, description)
    if fn is None:
        raise RuntimeError("Cannot create query without function or name or dynamic")
    if inspect.iscoroutinefunction(fn):
        warnings.warn(
            "Queries as async def functions are deprecated",
            DeprecationWarning,
            stacklevel=2,
        )
    return decorator(fn.__name__, description, fn, bypass_async_check=True)


@runtime_checkable
class UpdateMethodMultiParam(Protocol[MultiParamSpec, ProtocolReturnType]):
    """Decorated workflow update functions implement this."""

    _defn: _UpdateDefinition

    def __call__(
        self, *args: MultiParamSpec.args, **kwargs: MultiParamSpec.kwargs
    ) -> ProtocolReturnType | Awaitable[ProtocolReturnType]:
        """Generic callable type callback."""
        ...

    def validator(
        self, vfunc: Callable[MultiParamSpec, None]
    ) -> Callable[MultiParamSpec, None]:
        """Use to decorate a function to validate the arguments passed to the update handler."""
        ...


@overload
def update(
    fn: Callable[MultiParamSpec, Awaitable[ReturnType]],
) -> UpdateMethodMultiParam[MultiParamSpec, ReturnType]: ...


@overload
def update(
    fn: Callable[MultiParamSpec, ReturnType],
) -> UpdateMethodMultiParam[MultiParamSpec, ReturnType]: ...


@overload
def update(
    *,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: str | None = None,
) -> Callable[
    [Callable[MultiParamSpec, ReturnType]],
    UpdateMethodMultiParam[MultiParamSpec, ReturnType],
]: ...


@overload
def update(
    *,
    name: str,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: str | None = None,
) -> Callable[
    [Callable[MultiParamSpec, ReturnType]],
    UpdateMethodMultiParam[MultiParamSpec, ReturnType],
]: ...


@overload
def update(
    *,
    dynamic: Literal[True],
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: str | None = None,
) -> Callable[
    [Callable[MultiParamSpec, ReturnType]],
    UpdateMethodMultiParam[MultiParamSpec, ReturnType],
]: ...


def update(
    fn: CallableSyncOrAsyncType | None = None,  # type: ignore[reportInvalidTypeVarUse]
    *,
    name: str | None = None,
    dynamic: bool | None = False,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: str | None = None,
) -> (
    UpdateMethodMultiParam[MultiParamSpec, ReturnType]
    | Callable[
        [Callable[MultiParamSpec, ReturnType]],
        UpdateMethodMultiParam[MultiParamSpec, ReturnType],
    ]
):
    """Decorator for a workflow update handler method.

    This is used on any async or non-async method that you wish to be called upon
    receiving an update. If a function overrides one with this decorator, it too
    must be decorated.

    You may also optionally define a validator method that will be called before
    this handler you have applied this decorator to. You can specify the validator
    with ``@update_handler_function_name.validator``.

    Update methods can only have positional parameters. Best practice for
    non-dynamic update methods is to only take a single object/dataclass
    argument that can accept more fields later if needed. The handler may return
    a serializable value which will be sent back to the caller of the update.

    Args:
        fn: The function to decorate.
        name: Update name. Defaults to method ``__name__``. Cannot be present
            when ``dynamic`` is present.
        dynamic: If true, this handles all updates not otherwise handled. The
            parameters of the method must be self, a string name, and a
            ``*args`` positional varargs. Cannot be present when ``name`` is
            present.
        unfinished_policy: Actions taken if a workflow terminates with
            a running instance of this handler.
        description: A short description of the update that may appear in the UI/CLI.
    """

    def decorator(
        name: str | None,
        unfinished_policy: HandlerUnfinishedPolicy,
        fn: CallableSyncOrAsyncType,
    ) -> CallableSyncOrAsyncType:
        if not name and not dynamic:
            name = fn.__name__
        defn = _UpdateDefinition(
            name=name,
            fn=fn,
            is_method=True,
            unfinished_policy=unfinished_policy,
            description=description,
        )
        if defn.dynamic_vararg:
            raise RuntimeError(
                "Dynamic updates do not support a vararg third param, use Sequence[RawValue]",
            )
        setattr(fn, "_defn", defn)
        setattr(fn, "validator", partial(_update_validator, defn))
        return fn

    if not fn:
        if name is not None and dynamic:
            raise RuntimeError("Cannot provide name and dynamic boolean")
        return partial(decorator, name, unfinished_policy)  # type: ignore[reportReturnType, return-value]
    else:
        return decorator(fn.__name__, unfinished_policy, fn)  # type: ignore[reportReturnType, return-value]


def _update_validator(
    update_def: _UpdateDefinition, fn: Callable[..., None] | None = None
) -> Callable[..., None] | None:
    """Decorator for a workflow update validator method."""
    if fn is not None:
        update_def.set_validator(fn)
    return fn


def _bind_method(obj: Any, fn: Callable[..., Any]) -> Callable[..., Any]:
    # Curry instance on the definition function since that represents an
    # unbound method
    if inspect.iscoroutinefunction(fn):
        # We cannot use functools.partial here because in <= 3.7 that isn't
        # considered an inspect.iscoroutinefunction
        fn = cast(Callable[..., Awaitable[Any]], fn)

        async def with_object(*args: Any, **kwargs: Any) -> Any:
            return await fn(obj, *args, **kwargs)

        return with_object
    return partial(fn, obj)


def _assert_dynamic_handler_args(
    fn: Callable, arg_types: list[type] | None, is_method: bool
) -> bool:
    # Dynamic query/signal/update must have three args: self, name, and
    # Sequence[RawValue]. An older form accepted varargs for the third param for signals/queries so
    # we will too (but will warn in the signal/query code).
    params = list(inspect.signature(fn).parameters.values())
    total_expected_params = 3 if is_method else 2
    if (
        len(params) == total_expected_params
        and params[-2].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        and params[-1].kind is inspect.Parameter.VAR_POSITIONAL
    ):
        # Old var-arg form
        return False
    if (
        not arg_types
        or len(arg_types) != 2
        or arg_types[0] is not str
        or (
            arg_types[1] != Sequence[temporalio.common.RawValue]
            and arg_types[1] != typing.Sequence[temporalio.common.RawValue]  # type: ignore[reportDeprecated]
        )
    ):
        raise RuntimeError(
            "Dynamic handler must have 3 arguments: self, str, and Sequence[RawValue]"
        )
    return True


@dataclass(frozen=True)
class _SignalDefinition:
    # None if dynamic
    name: str | None
    fn: Callable[..., None | Awaitable[None]]
    is_method: bool
    unfinished_policy: HandlerUnfinishedPolicy = (
        HandlerUnfinishedPolicy.WARN_AND_ABANDON
    )
    description: str | None = None
    # Types loaded on post init if None
    arg_types: list[type] | None = None
    dynamic_vararg: bool = False

    @staticmethod
    def from_fn(fn: Callable) -> _SignalDefinition | None:
        return getattr(fn, "__temporal_signal_definition", None)

    @staticmethod
    def must_name_from_fn_or_str(signal: str | Callable) -> str:
        if callable(signal):
            defn = _SignalDefinition.from_fn(signal)
            if not defn:
                raise RuntimeError(
                    f"Signal definition not found on {signal.__qualname__}, "
                    "is it decorated with @workflow.signal?"
                )
            elif not defn.name:
                raise RuntimeError("Cannot invoke dynamic signal definition")
            # TODO(cretz): Check count/type of args at runtime?
            return defn.name
        return str(signal)

    def __post_init__(self) -> None:
        if self.arg_types is None:
            arg_types, _ = temporalio.common._type_hints_from_func(self.fn)
            # If dynamic, assert it
            if not self.name:
                object.__setattr__(
                    self,
                    "dynamic_vararg",
                    not _assert_dynamic_handler_args(
                        self.fn, arg_types, self.is_method
                    ),
                )
            object.__setattr__(self, "arg_types", arg_types)

    def bind_fn(self, obj: Any) -> Callable[..., Any]:
        return _bind_method(obj, self.fn)


@dataclass(frozen=True)
class _QueryDefinition:
    # None if dynamic
    name: str | None
    fn: Callable[..., Any]
    is_method: bool
    description: str | None = None
    # Types loaded on post init if both are None
    arg_types: list[type] | None = None
    ret_type: type | None = None
    dynamic_vararg: bool = False

    @staticmethod
    def from_fn(fn: Callable) -> _QueryDefinition | None:
        return getattr(fn, "__temporal_query_definition", None)

    def __post_init__(self) -> None:
        if self.arg_types is None and self.ret_type is None:
            arg_types, ret_type = temporalio.common._type_hints_from_func(self.fn)
            # If dynamic, assert it
            if not self.name:
                object.__setattr__(
                    self,
                    "dynamic_vararg",
                    not _assert_dynamic_handler_args(
                        self.fn, arg_types, self.is_method
                    ),
                )
            object.__setattr__(self, "arg_types", arg_types)
            object.__setattr__(self, "ret_type", ret_type)

    def bind_fn(self, obj: Any) -> Callable[..., Any]:
        return _bind_method(obj, self.fn)


@dataclass(frozen=True)
class _UpdateDefinition:
    # None if dynamic
    name: str | None
    fn: Callable[..., Any | Awaitable[Any]]
    is_method: bool
    unfinished_policy: HandlerUnfinishedPolicy = (
        HandlerUnfinishedPolicy.WARN_AND_ABANDON
    )
    description: str | None = None
    # Types loaded on post init if None
    arg_types: list[type] | None = None
    ret_type: type | None = None
    validator: Callable[..., None] | None = None
    dynamic_vararg: bool = False

    def __post_init__(self) -> None:
        if self.arg_types is None:
            arg_types, ret_type = temporalio.common._type_hints_from_func(self.fn)
            # Disallow dynamic varargs
            if not self.name and not _assert_dynamic_handler_args(
                self.fn, arg_types, self.is_method
            ):
                raise RuntimeError(
                    "Dynamic updates do not support a vararg third param, use Sequence[RawValue]",
                )
            object.__setattr__(self, "arg_types", arg_types)
            object.__setattr__(self, "ret_type", ret_type)

    def bind_fn(self, obj: Any) -> Callable[..., Any]:
        return _bind_method(obj, self.fn)

    def bind_validator(self, obj: Any) -> Callable[..., Any]:
        if self.validator is not None:
            return _bind_method(obj, self.validator)
        return lambda *args, **kwargs: None

    def set_validator(self, validator: Callable[..., None]) -> None:
        if self.validator:
            raise RuntimeError(f"Validator already set for update {self.name}")
        object.__setattr__(self, "validator", validator)

    @classmethod
    def get_name_and_result_type(
        cls,
        name_or_update_fn: str | Callable[..., Any],
    ) -> tuple[str, type | None]:
        if isinstance(name_or_update_fn, UpdateMethodMultiParam):
            defn = name_or_update_fn._defn
            if not defn.name:
                raise RuntimeError("Cannot invoke dynamic update definition")
            # TODO(cretz): Check count/type of args at runtime?
            return defn.name, defn.ret_type
        else:
            return str(name_or_update_fn), None
