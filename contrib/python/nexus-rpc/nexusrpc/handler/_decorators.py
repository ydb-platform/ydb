from __future__ import annotations

import typing
import warnings
from collections.abc import Awaitable
from typing import Any, Callable, Optional, TypeVar, Union, overload

import nexusrpc.handler._syncio
from nexusrpc._common import InputT, OutputT, ServiceHandlerT
from nexusrpc._service import Operation
from nexusrpc._util import (
    get_callable_name,
    get_service_definition,
    is_async_callable,
    set_operation,
    set_operation_factory,
    set_service_definition,
)
from nexusrpc.handler._common import StartOperationContext
from nexusrpc.handler._util import (
    get_start_method_input_and_output_type_annotations,
)

from ._operation_handler import (
    OperationHandler,
    SyncOperationHandler,
    collect_operation_handler_factories_by_method_name,
    service_definition_from_operation_handler_methods,
    validate_operation_handler_methods,
)


@overload
def service_handler(cls: type[ServiceHandlerT]) -> type[ServiceHandlerT]: ...


@overload
def service_handler(
    *,
    service: Optional[type[Any]] = None,
) -> Callable[[type[ServiceHandlerT]], type[ServiceHandlerT]]: ...


@overload
def service_handler(
    *, name: str
) -> Callable[[type[ServiceHandlerT]], type[ServiceHandlerT]]: ...


def service_handler(
    cls: Optional[type[ServiceHandlerT]] = None,
    *,
    service: Optional[type[Any]] = None,
    name: Optional[str] = None,
) -> Union[
    type[ServiceHandlerT], Callable[[type[ServiceHandlerT]], type[ServiceHandlerT]]
]:
    """Decorator that marks a class as a Nexus service handler.

    A service handler is a class that implements the Nexus service by providing
    operation handler implementations for all operations in the service.

    The class should implement Nexus operation handlers as methods decorated with
    operation handler decorators such as :py:func:`@nexusrpc.handler.operation_handler`.

    Args:
        cls: The service handler class to decorate.
        service: The service definition that the service handler implements.
        name: Optional name to use for the service, if a service definition is not provided.
              `service` and `name` are mutually exclusive. If neither is provided, the
              class name will be used.

    Example:
        .. code-block:: python

            from nexusrpc.handler import service_handler, sync_operation

            @service_handler(service=MyService)
            class MyServiceHandler:
                @sync_operation
                async def my_operation(
                    self, ctx: StartOperationContext, input: MyInput
                ) -> MyOutput:
                    return MyOutput(processed=input.data)
    """
    if service and name:
        raise ValueError(
            "You cannot specify both service and name: "
            "if you provide a service then the name will be taken from the service."
        )
    _service = None
    if service:
        _service = get_service_definition(service)
        if not _service:
            raise ValueError(
                f"{service} is not a valid Nexus service definition. "
                f"Use the @nexusrpc.service decorator on a class to define a Nexus service definition."
            )

    def decorator(cls: type[ServiceHandlerT]) -> type[ServiceHandlerT]:
        # The name by which the service must be addressed in Nexus requests.
        _name = (
            _service.name if _service else name if name is not None else cls.__name__
        )
        if not _name:
            raise ValueError("Service name must not be empty.")
        factories_by_method_name = collect_operation_handler_factories_by_method_name(
            cls, _service
        )
        service = _service or service_definition_from_operation_handler_methods(
            _name, factories_by_method_name
        )
        validate_operation_handler_methods(cls, factories_by_method_name, service)
        set_service_definition(cls, service)
        return cls

    if cls is None:
        return decorator

    return decorator(cls)


OperationHandlerFactoryT = TypeVar(
    "OperationHandlerFactoryT", bound=Callable[[Any], OperationHandler[Any, Any]]
)


@overload
def operation_handler(
    method: OperationHandlerFactoryT,
) -> OperationHandlerFactoryT: ...


@overload
def operation_handler(
    *, name: Optional[str] = None
) -> Callable[[OperationHandlerFactoryT], OperationHandlerFactoryT]: ...


def operation_handler(
    method: Optional[OperationHandlerFactoryT] = None,
    *,
    name: Optional[str] = None,
) -> Union[
    OperationHandlerFactoryT,
    Callable[[OperationHandlerFactoryT], OperationHandlerFactoryT],
]:
    """
    Decorator marking an operation handler factory method in a service handler class.

    An operation handler factory method is a method that takes no arguments other than
    `self` and returns an :py:class:`OperationHandler` instance.

    Args:
        method: The method to decorate.
        name: Optional name for the operation. If not provided, the method name will be used.
    """

    def decorator(
        method: OperationHandlerFactoryT,
    ) -> OperationHandlerFactoryT:
        # Extract input and output types from the return type annotation
        input_type = None
        output_type = None

        return_type = typing.get_type_hints(method).get("return")
        if typing.get_origin(return_type) == OperationHandler:
            type_args = typing.get_args(return_type)
            if len(type_args) == 2:
                input_type, output_type = type_args
            else:
                warnings.warn(
                    f"OperationHandler return type should have two type parameters (input and output type), "
                    f"but operation {method.__name__} has {len(type_args)} type parameters: {type_args}"
                )

        op: Operation[Any, Any] = Operation(
            name=name or method.__name__,
            input_type=input_type,
            output_type=output_type,
        )
        op.method_name = method.__name__
        set_operation(method, op)
        return method

    if method is None:
        return decorator

    return decorator(method)


F = TypeVar("F", bound=Callable[[Any, StartOperationContext, Any], Any])


@overload
def sync_operation(
    start: Callable[
        [ServiceHandlerT, StartOperationContext, InputT], Awaitable[OutputT]
    ],
) -> Callable[[ServiceHandlerT, StartOperationContext, InputT], Awaitable[OutputT]]: ...


@overload
def sync_operation(
    start: Callable[[ServiceHandlerT, StartOperationContext, InputT], OutputT],
) -> Callable[[ServiceHandlerT, StartOperationContext, InputT], OutputT]: ...


@overload
def sync_operation(
    *,
    name: Optional[str] = None,
) -> Callable[
    [Callable[[ServiceHandlerT, StartOperationContext, InputT], Any]],
    Callable[[ServiceHandlerT, StartOperationContext, InputT], Any],
]: ...


def sync_operation(
    start: Optional[F] = None,
    *,
    name: Optional[str] = None,
) -> Union[F, Callable[[F], F]]:
    """
    Decorator marking a method as the start method for a synchronous operation.

    Example:
        .. code-block:: python

            import httpx
            from nexusrpc.handler import service_handler, sync_operation

            @service_handler
            class MyServiceHandler:
                @sync_operation
                async def process_data(
                    self, ctx: StartOperationContext, input: str
                ) -> str:
                    # You can use asynchronous I/O libraries
                    async with httpx.AsyncClient() as client:
                        response = await client.get("https://api.example.com/data")

                    data = response.json()
                    return f"Processed: {data}"
    """

    def decorator(start: F) -> F:
        def operation_handler_factory(
            self: Any,
        ) -> OperationHandler[Any, Any]:
            if is_async_callable(start):

                async def asyncio_start(ctx: StartOperationContext, input: Any) -> Any:
                    return await start(self, ctx, input)

                asyncio_start.__doc__ = start.__doc__
                return SyncOperationHandler(asyncio_start)
            else:

                def _start(ctx: StartOperationContext, input: Any) -> Any:
                    return start(self, ctx, input)

                _start.__doc__ = start.__doc__
                return nexusrpc.handler._syncio.SyncOperationHandler(_start)

        # Type inspection happens here at @sync_operation decoration time, before we know
        # if a service definition will be provided to @service_handler. While we could
        # theoretically defer this inspection until @service_handler time (and skip it when
        # a service definition provides the types), doing so would require storing the
        # original method reference and implementing lazy evaluation. The added complexity
        # isn't worth the marginal performance benefit. When types are None and a service
        # definition is provided, type validation is simply skipped (see
        # validate_operation_handler_methods in _operation_handler.py).
        input_type, output_type = get_start_method_input_and_output_type_annotations(  # type: ignore[var-annotated]
            start  # type: ignore[arg-type]
        )

        method_name = get_callable_name(start)
        op = Operation(
            name=name or method_name,
            input_type=input_type,
            output_type=output_type,
        )
        op.method_name = method_name
        set_operation(operation_handler_factory, op)

        set_operation_factory(start, operation_handler_factory)
        return start

    if start is None:
        return decorator

    return decorator(start)
