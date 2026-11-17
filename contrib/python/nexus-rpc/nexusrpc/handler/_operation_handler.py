from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from collections.abc import Awaitable
from typing import Any, Callable, Generic, Optional

from nexusrpc._common import InputT, OutputT, ServiceHandlerT
from nexusrpc._service import Operation, OperationDefinition, ServiceDefinition
from nexusrpc._util import (
    get_operation,
    get_operation_factory,
    is_async_callable,
    is_callable,
)

from ._common import (
    CancelOperationContext,
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
    ) -> (
        StartOperationResultSync[OutputT]
        | StartOperationResultAsync
        | Awaitable[StartOperationResultSync[OutputT] | StartOperationResultAsync]
    ):
        """
        Start the operation, completing either synchronously or asynchronously.

        Returns the result synchronously, or returns an operation token. Which path is
        taken may be decided at operation handling time.
        """
        ...

    @abstractmethod
    def cancel(self, ctx: CancelOperationContext, token: str) -> None | Awaitable[None]:
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

    async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        raise NotImplementedError(
            "An operation that responded synchronously cannot be cancelled."
        )


class MiddlewareSafeOperationHandler(OperationHandler[Any, Any], ABC):
    """
    An :py:class:`OperationHandler` where :py:attr:`start` and :py:attr:`cancel`
    can be awaited by an async runtime. It can produce a result synchronously by returning
    :py:class:`StartOperationResultSync` or asynchronously by returning :py:class:`StartOperationResultAsync`
    in the same fashion that :py:class:`OperationHandler` does.

    .. warning::
        This API is experimental and may change in the future.
    """

    @abstractmethod
    async def start(
        self, ctx: StartOperationContext, input: Any
    ) -> StartOperationResultSync[Any] | StartOperationResultAsync:
        """
        Start the operation and return it's result or an async token.
        """
        ...

    @abstractmethod
    async def cancel(self, ctx: CancelOperationContext, token: str) -> None:
        """
        Cancel an in progress operation identified by the given token.
        """
        ...


def collect_operation_handler_factories_by_method_name(
    user_service_cls: type[ServiceHandlerT],
    service: Optional[ServiceDefinition],
) -> dict[str, Callable[[ServiceHandlerT], OperationHandler[Any, Any]]]:
    """
    Collect operation handler methods from a user service handler class.
    """
    factories: dict[str, Callable[[ServiceHandlerT], OperationHandler[Any, Any]]] = {}
    service_method_names = (
        {op_defn.method_name for op_defn in service.operation_definitions.values()}
        if service
        else set()
    )
    seen = set()
    for _, method in inspect.getmembers(user_service_cls, is_callable):
        if factory := get_operation_factory(method):
            if op := get_operation(factory):
                if op.name in seen:
                    raise RuntimeError(
                        f"Operation '{op.name}' in service '{user_service_cls.__name__}' "
                        f"is defined multiple times."
                    )
                if service and op.method_name not in service_method_names:
                    _names = ", ".join(f"'{s}'" for s in sorted(service_method_names))
                    msg = (
                        f"Operation method name '{op.method_name}' in service handler {user_service_cls} "
                        f"does not match an operation method name in the service definition. "
                        f"Available method names in the service definition: "
                    )
                    msg += _names if _names else "[none]"
                    raise TypeError(msg)

                assert op.method_name, (
                    f"Operation '{op}' method name should not be empty. Please report this as a bug."
                )
                factories[op.method_name] = factory
                seen.add(op.name)
    return factories


def validate_operation_handler_methods(
    service_cls: type[ServiceHandlerT],
    operation_handler_factories_by_method_name: dict[
        str, Callable[[ServiceHandlerT], OperationHandler[Any, Any]]
    ],
    service_definition: ServiceDefinition,
) -> None:
    """Validate operation handler methods against a service definition.

    For every operation in ``service_definition``:

    1. There must be a method in ``user_methods`` whose method name matches the method
       name from the service definition.

    2. The input and output types of the handler method must exactly match the types
       declared in the service definition.
    """
    operation_handler_factories_by_method_name = (
        operation_handler_factories_by_method_name.copy()
    )
    for op_defn in service_definition.operation_definitions.values():
        factory = operation_handler_factories_by_method_name.pop(
            op_defn.method_name, None
        )
        if not factory:
            raise TypeError(
                f"Service '{service_cls}' does not implement an operation with "
                f"method name '{op_defn.method_name}'. But this operation is in service "
                f"definition '{service_definition}'."
            )
        op = get_operation(factory)
        if not isinstance(op, Operation):
            raise ValueError(
                f"Method '{factory}' in class '{service_cls.__name__}' "
                f"does not have a valid __nexus_operation__ attribute. "
                f"Did you forget to decorate the operation method with an operation handler decorator such as "
                f":py:func:`@nexusrpc.handler.operation_handler`?"
            )
        if op.name not in [op.method_name, op_defn.name]:
            raise TypeError(
                f"Operation '{op_defn.method_name}' in service '{service_cls}' "
                f"has name '{op.name}', but the name in the service definition "
                f"is '{op_defn.name}'. Operation handlers may not override the name of an operation "
                f"in the service definition."
            )
        # If handler's input_type is None (missing annotation), skip validation - the handler
        # relies on the service definition for type information. This supports handlers without
        # explicit type annotations when a service definition is provided.
        if op.input_type is not None and op_defn.input_type != op.input_type:
            raise TypeError(
                f"OperationHandler input type mismatch for '{service_cls}.{op_defn.method_name}': "
                f"expected {op_defn.input_type}, got {op.input_type}"
            )

        # If handler's output_type is None (missing annotation), skip validation - the handler
        # relies on the service definition for type information.
        if op.output_type is not None and op.output_type != op_defn.output_type:
            raise TypeError(
                f"OperationHandler output type mismatch for '{service_cls}.{op_defn.method_name}': "
                f"expected {op_defn.output_type}, got {op.output_type}"
            )
    if operation_handler_factories_by_method_name:
        raise ValueError(
            f"Service '{service_cls}' implements more operations than the interface '{service_definition}'. "
            f"Extra operations: {', '.join(sorted(operation_handler_factories_by_method_name.keys()))}."
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
    op_defns: dict[str, OperationDefinition[Any, Any]] = {}
    for name, method in user_methods.items():
        op = get_operation(method)
        if not isinstance(op, Operation):
            raise ValueError(
                f"In service '{service_name}', could not locate operation definition for "
                f"user operation handler method '{name}'. Did you forget to decorate the operation "
                f"method with an operation handler decorator such as "
                f"@nexusrpc.handler.sync_operation?"
            )
        op_defns[op.name] = OperationDefinition.from_operation(op)

    return ServiceDefinition(name=service_name, operation_definitions=op_defns)
