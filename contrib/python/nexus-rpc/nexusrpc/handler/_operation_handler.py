from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from typing import (
    Any,
    Awaitable,
    Callable,
    Generic,
    Optional,
    Type,
    Union,
)

from nexusrpc._common import InputT, OperationInfo, OutputT, ServiceHandlerT
from nexusrpc._service import Operation, ServiceDefinition
from nexusrpc._util import (
    get_operation_factory,
    is_async_callable,
    is_callable,
    is_subtype,
)

from ._common import (
    CancelOperationContext,
    FetchOperationInfoContext,
    FetchOperationResultContext,
    StartOperationContext,
    StartOperationResultAsync,
    StartOperationResultSync,
)


class OperationHandler(ABC, Generic[InputT, OutputT]):
    """
    Base class for an operation handler in a Nexus service implementation.

    To define a Nexus operation handler, create a method on your service handler class
    that takes `self` and returns an instance of :py:class:`OperationHandler`, and apply
    the :py:func:`@nexusrpc.handler.operation_handler` decorator.

    To create an operation handler that is limited to returning synchronously, use
    :py:func:`@nexusrpc.handler.SyncOperationHandler` to create the
    instance of :py:class:`OperationHandler` from the start method.
    """

    @abstractmethod
    def start(
        self, ctx: StartOperationContext, input: InputT
    ) -> Union[
        StartOperationResultSync[OutputT],
        Awaitable[StartOperationResultSync[OutputT]],
        StartOperationResultAsync,
        Awaitable[StartOperationResultAsync],
    ]:
        """
        Start the operation, completing either synchronously or asynchronously.

        Returns the result synchronously, or returns an operation token. Which path is
        taken may be decided at operation handling time.
        """
        ...

    @abstractmethod
    def fetch_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> Union[OperationInfo, Awaitable[OperationInfo]]:
        """
        Return information about the current status of the operation.
        """
        ...

    @abstractmethod
    def fetch_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Union[OutputT, Awaitable[OutputT]]:
        """
        Return the result of the operation.
        """
        ...

    @abstractmethod
    def cancel(
        self, ctx: CancelOperationContext, token: str
    ) -> Union[None, Awaitable[None]]:
        """
        Cancel the operation.
        """
        ...


class SyncOperationHandler(OperationHandler[InputT, OutputT]):
    """
    An :py:class:`OperationHandler` that is limited to responding synchronously.

    This version of the class uses `async def` methods. For the syncio version, see
    :py:class:`nexusrpc.handler._syncio.SyncOperationHandler`.
    """

    def __init__(
        self, start: Callable[[StartOperationContext, InputT], Awaitable[OutputT]]
    ):
        if not is_async_callable(start):
            raise RuntimeError(
                f"{start} is not an `async def` method. "
                "SyncOperationHandler must be initialized with an `async def` method. "
            )
        self._start = start
        if start.__doc__:
            if start_func := getattr(self.start, "__func__", None):
                start_func.__doc__ = start.__doc__

    async def start(
        self, ctx: StartOperationContext, input: InputT
    ) -> StartOperationResultSync[OutputT]:
        """
        Start the operation and return its final result synchronously.

        The name 'SyncOperationHandler' means that it responds synchronously in the
        sense that the start method delivers the final operation result as its return
        value, rather than returning an operation token representing an in-progress
        operation. This version of the class uses `async def` methods. For the syncio
        version, see :py:class:`nexusrpc.handler.syncio.SyncOperationHandler`.
        """
        return StartOperationResultSync(await self._start(ctx, input))

    async def fetch_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> OperationInfo:
        raise NotImplementedError(
            "Cannot fetch operation info for an operation that responded synchronously."
        )

    async def fetch_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> OutputT:
        raise NotImplementedError(
            "Cannot fetch the result of an operation that responded synchronously."
        )

    async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        raise NotImplementedError(
            "An operation that responded synchronously cannot be cancelled."
        )


def collect_operation_handler_factories_by_method_name(
    user_service_cls: Type[ServiceHandlerT],
    service: Optional[ServiceDefinition],
) -> dict[str, Callable[[ServiceHandlerT], OperationHandler[Any, Any]]]:
    """
    Collect operation handler methods from a user service handler class.
    """
    factories: dict[str, Callable[[ServiceHandlerT], OperationHandler[Any, Any]]] = {}
    service_method_names = (
        {
            op.method_name
            for op in service.operations.values()
            if op.method_name is not None
        }
        if service
        else set()
    )
    seen = set()
    for _, method in inspect.getmembers(user_service_cls, is_callable):
        factory, op_defn = get_operation_factory(method)  # type: ignore[var-annotated]
        if factory and isinstance(op_defn, Operation):
            # This is a method decorated with one of the *operation_handler decorators
            if op_defn.name in seen:
                raise RuntimeError(
                    f"Operation '{op_defn.name}' in service '{user_service_cls.__name__}' "
                    f"is defined multiple times."
                )
            if service and op_defn.method_name not in service_method_names:
                _names = ", ".join(f"'{s}'" for s in sorted(service_method_names))
                msg = (
                    f"Operation method name '{op_defn.method_name}' in service handler {user_service_cls} "
                    f"does not match an operation method name in the service definition. "
                    f"Available method names in the service definition: "
                )
                msg += _names if _names else "[none]"
                msg += "."
                raise TypeError(msg)

            # TODO(preview) op_defn.method name should be non-nullable
            assert op_defn.method_name, (
                f"Operation '{op_defn}' method name should not be None. This is an SDK bug."
            )
            factories[op_defn.method_name] = factory
            seen.add(op_defn.name)
    return factories


def validate_operation_handler_methods(
    user_service_cls: Type[ServiceHandlerT],
    user_methods_by_method_name: dict[
        str, Callable[[ServiceHandlerT], OperationHandler[Any, Any]]
    ],
    service_definition: ServiceDefinition,
) -> None:
    """Validate operation handler methods against a service definition.

    For every operation in ``service_definition``:

    1. There must be a method in ``user_methods`` whose method name matches the method
       name from the service definition.

    2. The input and output types of the user method must be such that the user method
       is a subtype of the operation defined in the service definition, i.e. respecting
       input type contravariance and output type covariance.
    """
    user_methods_by_method_name = user_methods_by_method_name.copy()
    for op_defn in service_definition.operations.values():
        if not op_defn.method_name:
            raise ValueError(
                f"Operation '{op_defn}' in service definition '{service_definition}' "
                f"does not have a method name. "
            )
        method = user_methods_by_method_name.pop(op_defn.method_name, None)
        if not method:
            raise TypeError(
                f"Service '{user_service_cls}' does not implement an operation with "
                f"method name '{op_defn.method_name}'. But this operation is in service "
                f"definition '{service_definition}'."
            )
        method, method_op_defn = get_operation_factory(method)
        if not isinstance(method_op_defn, Operation):
            raise ValueError(
                f"Method '{method}' in class '{user_service_cls.__name__}' "
                f"does not have a valid __nexus_operation__ attribute. "
                f"Did you forget to decorate the operation method with an operation handler decorator such as "
                f":py:func:`@nexusrpc.handler.operation_handler`?"
            )
        if method_op_defn.name not in [method_op_defn.method_name, op_defn.name]:
            raise TypeError(
                f"Operation '{op_defn.method_name}' in service '{user_service_cls}' "
                f"has name '{method_op_defn.name}', but the name in the service definition "
                f"is '{op_defn.name}'. Operation handlers may not override the name of an operation "
                f"in the service definition."
            )
        # Input type is contravariant: op handler input must be superclass of op defn output
        if (
            method_op_defn.input_type is not None
            and op_defn.input_type is not None
            and Any not in (method_op_defn.input_type, op_defn.input_type)
            and not (
                op_defn.input_type == method_op_defn.input_type
                or is_subtype(op_defn.input_type, method_op_defn.input_type)
            )
        ):
            raise TypeError(
                f"Operation '{op_defn.method_name}' in service '{user_service_cls}' "
                f"has input type '{method_op_defn.input_type}', which is not "
                f"compatible with the input type '{op_defn.input_type}' in interface "
                f"'{service_definition.name}'. The input type must be the same as or a "
                f"superclass of the operation definition input type."
            )

        # Output type is covariant: op handler output must be subclass of op defn output
        if (
            method_op_defn.output_type is not None
            and op_defn.output_type is not None
            and Any not in (method_op_defn.output_type, op_defn.output_type)
            and not is_subtype(method_op_defn.output_type, op_defn.output_type)
        ):
            raise TypeError(
                f"Operation '{op_defn.method_name}' in service '{user_service_cls}' "
                f"has output type '{method_op_defn.output_type}', which is not "
                f"compatible with the output type '{op_defn.output_type}' in interface "
                f" '{service_definition}'. The output type must be the same as or a "
                f"subclass of the operation definition output type."
            )
    if user_methods_by_method_name:
        raise ValueError(
            f"Service '{user_service_cls}' implements more operations than the interface '{service_definition}'. "
            f"Extra operations: {', '.join(sorted(user_methods_by_method_name.keys()))}."
        )


def service_definition_from_operation_handler_methods(
    service_name: str,
    user_methods: dict[str, Callable[[ServiceHandlerT], OperationHandler[Any, Any]]],
) -> ServiceDefinition:
    """
    Create a service definition from operation handler factory methods.

    In general, users should have access to, or define, a service definition, and validate
    their service handler against it by passing the service definition to the
    :py:func:`@nexusrpc.handler.service_handler` decorator. This function is used when
    that is not the case.
    """
    op_defns: dict[str, Operation[Any, Any]] = {}
    for name, method in user_methods.items():
        _, op_defn = get_operation_factory(method)
        if not isinstance(op_defn, Operation):
            raise ValueError(
                f"In service '{service_name}', could not locate operation definition for "
                f"user operation handler method '{name}'. Did you forget to decorate the operation "
                f"method with an operation handler decorator such as "
                f":py:func:`@nexusrpc.handler.operation_handler`?"
            )
        op_defns[op_defn.name] = op_defn

    return ServiceDefinition(name=service_name, operations=op_defns)
