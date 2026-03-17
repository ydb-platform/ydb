"""Advanced types."""

from typing import Any, Awaitable, Callable, Type, TypeVar, Union

from typing_extensions import ParamSpec, Protocol

AnyType = TypeVar("AnyType")
ClassType = TypeVar("ClassType", bound=Type)
SelfType = TypeVar("SelfType")
ParamType = TypeVar("ParamType")
ReturnType = TypeVar("ReturnType", covariant=True)
LocalReturnType = TypeVar("LocalReturnType", covariant=True)
CallableType = TypeVar("CallableType", bound=Callable[..., Any])
CallableAsyncType = TypeVar("CallableAsyncType", bound=Callable[..., Awaitable[Any]])
CallableSyncOrAsyncType = TypeVar(
    "CallableSyncOrAsyncType",
    bound=Callable[..., Union[Any, Awaitable[Any]]],
)
CallableSyncOrAsyncReturnNoneType = TypeVar(
    "CallableSyncOrAsyncReturnNoneType",
    bound=Callable[..., Union[None, Awaitable[None]]],
)
MultiParamSpec = ParamSpec("MultiParamSpec")


ProtocolParamType = TypeVar("ProtocolParamType", contravariant=True)
ProtocolReturnType = TypeVar("ProtocolReturnType", covariant=True)
ProtocolSelfType = TypeVar("ProtocolSelfType", contravariant=True)


class CallableAsyncNoParam(Protocol[ProtocolReturnType]):
    """Generic callable type."""

    def __call__(self) -> Awaitable[ProtocolReturnType]:
        """Generic callable type callback."""
        ...


class CallableSyncNoParam(Protocol[ProtocolReturnType]):
    """Generic callable type."""

    def __call__(self) -> ProtocolReturnType:
        """Generic callable type callback."""
        ...


class CallableAsyncSingleParam(Protocol[ProtocolParamType, ProtocolReturnType]):
    """Generic callable type."""

    def __call__(self, __arg: ProtocolParamType) -> Awaitable[ProtocolReturnType]:
        """Generic callable type callback."""
        ...


class CallableSyncSingleParam(Protocol[ProtocolParamType, ProtocolReturnType]):
    """Generic callable type."""

    def __call__(self, __arg: ProtocolParamType) -> ProtocolReturnType:
        """Generic callable type callback."""
        ...


class MethodAsyncNoParam(Protocol[ProtocolSelfType, ProtocolReturnType]):
    """Generic callable type."""

    def __call__(__self, self: ProtocolSelfType) -> Awaitable[ProtocolReturnType]:
        """Generic callable type callback."""
        ...


class MethodSyncNoParam(Protocol[ProtocolSelfType, ProtocolReturnType]):
    """Generic callable type."""

    def __call__(__self, self: ProtocolSelfType) -> ProtocolReturnType:
        """Generic callable type callback."""
        ...


class MethodAsyncSingleParam(
    Protocol[ProtocolSelfType, ProtocolParamType, ProtocolReturnType]
):
    """Generic callable type."""

    def __call__(
        self, __self: ProtocolSelfType, __arg: ProtocolParamType, /
    ) -> Awaitable[ProtocolReturnType]:
        """Generic callable type callback."""
        ...


class MethodSyncSingleParam(
    Protocol[ProtocolSelfType, ProtocolParamType, ProtocolReturnType]
):
    """Generic callable type."""

    def __call__(
        self, __self: ProtocolSelfType, __arg: ProtocolParamType, /
    ) -> ProtocolReturnType:
        """Generic callable type callback."""
        ...


class MethodSyncOrAsyncNoParam(Protocol[ProtocolSelfType, ProtocolReturnType]):
    """Generic callable type."""

    def __call__(
        self, __self: ProtocolSelfType
    ) -> Union[ProtocolReturnType, Awaitable[ProtocolReturnType]]:
        """Generic callable type callback."""
        ...


class MethodSyncOrAsyncSingleParam(
    Protocol[ProtocolSelfType, ProtocolParamType, ProtocolReturnType]
):
    """Generic callable type."""

    def __call__(
        self, __self: ProtocolSelfType, __param: ProtocolParamType, /
    ) -> Union[ProtocolReturnType, Awaitable[ProtocolReturnType]]:
        """Generic callable type callback."""
        ...


# TODO(cretz): Open MyPy bug on this, see https://gitter.im/python/typing?at=62cc24c5904f20479ac0c854

# class CallableAsyncMultiParam(Protocol[ReturnType]):
#     def __call__(self, *__args: Any) -> Awaitable[ReturnType]:
#         ...
# class CallableSyncMultiParam(Protocol[ReturnType]):
#     def __call__(self, *__args: Any) -> ReturnType:
#         ...
# class MethodAsyncMultiParam(Protocol[SelfType, ReturnType]):
#     def __call__(__self, self: SelfType, *__args: Any) -> Awaitable[ReturnType]:
#         ...

# TODO(cretz): Does not work, see https://github.com/python/mypy/issues/11855
# MethodAsyncMultiParam = Callable[Concatenate[SelfType, MultiParamSpec], Awaitable[ReturnType]]
