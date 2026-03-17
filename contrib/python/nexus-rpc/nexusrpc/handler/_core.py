"""
This module contains Handler classes. A Handler manages a collection of Nexus
service handlers. It receives and responds to incoming Nexus requests, dispatching to
the corresponding operation handler.

A description of the dispatch logic follows.

There are two cases:

Case 1: Every user service handler class has a corresponding service definition
===============================================================================

I.e., there are service definitions that look like::

    @service
    class MyServiceDefinition:
        my_op: nexusrpc.Operation[I, O]


and every service handler class looks like::

    @service_handler(service=MyServiceDefinition)
    class MyServiceHandler:
        @sync_operation
        def my_op(self, ...)


Import time
-----------

1. The @service decorator builds a ServiceDefinition instance and attaches it to
   MyServiceDefinition.

   The ServiceDefinition contains `name` and a map of Operation instances,
   keyed by Operation.name (this is the publicly advertised name).

   An Operation contains `name`, `method_name`, and input and output types.

2. The @sync_operation decorator builds a second Operation instance and attaches
   it to a factory method that is attached to the my_op method object.

3. The @service_handler decorator acquires the ServiceDefinition instance from
   MyServiceDefinition and attaches it to the MyServiceHandler class.


Handler-registration time
-------------------------

1. Handler.__init__ is called with [MyServiceHandler()]

2. A ServiceHandler instance is built from the user service handler class. This comprises a
   ServiceDefinition and a map {op.name: OperationHandler}. The map is built by taking
   every operation in the service definition and locating the operation handler factory method
   whose *method name* matches the method name of the operation in the service definition.

3. Finally we build a map {service_definition.name: ServiceHandler} using the service definition
   in each ServiceHandler.

Request-handling time
---------------------

Now suppose a request has arrived for service S and operation O.

1. The Handler does self.service_handlers[S], yielding an instance of ServiceHandler.

2. The ServiceHandler does self.operation_handlers[O], yielding an instance of
   OperationHandler

Therefore we require that Handler.service_handlers and ServiceHandler.operation_handlers
are keyed by the publicly advertised service and operation name respectively. This was achieved
at steps (3) and (2) respectively.


Case 2: There exists a user service handler class without a corresponding service definition
============================================================================================

I.e., at least one user service handler class looks like::

    @service_handler
    class MyServiceHandler:
        @sync_operation
        def my_op(...)

This follows Case 1 with the following differences at import time:

- Step (1) does not occur.
- At step (3) the ServiceDefinition is synthesized by the @service_handler decorator from
  MyServiceHandler.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    Any,
    Awaitable,
    Callable,
    Mapping,
    Optional,
    Sequence,
    Union,
)

from typing_extensions import Self, TypeGuard

from nexusrpc._common import HandlerError, HandlerErrorType, OperationInfo
from nexusrpc._serializer import LazyValueT
from nexusrpc._service import ServiceDefinition
from nexusrpc._util import get_service_definition, is_async_callable

from ._common import (
    CancelOperationContext,
    FetchOperationInfoContext,
    FetchOperationResultContext,
    StartOperationContext,
    StartOperationResultAsync,
    StartOperationResultSync,
)
from ._operation_handler import (
    OperationHandler,
    collect_operation_handler_factories_by_method_name,
)


class AbstractHandler(ABC):
    @abstractmethod
    def start_operation(
        self,
        ctx: StartOperationContext,
        input: LazyValueT,
    ) -> Union[
        StartOperationResultSync[Any],
        StartOperationResultAsync,
        Awaitable[
            Union[
                StartOperationResultSync[Any],
                StartOperationResultAsync,
            ]
        ],
    ]: ...

    @abstractmethod
    def fetch_operation_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> Union[OperationInfo, Awaitable[OperationInfo]]:
        """Handle a Fetch Operation Info request.

        Args:
            ctx: The operation context.
            token: The operation token.
        """
        ...

    @abstractmethod
    def fetch_operation_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Union[Any, Awaitable[Any]]:
        """Handle a Fetch Operation Result request.

        Args:
            ctx: The operation context.
            token: The operation token.
        """
        ...

    @abstractmethod
    def cancel_operation(
        self, ctx: CancelOperationContext, token: str
    ) -> Union[None, Awaitable[None]]:
        """Handle a Cancel Operation request.

        Args:
            ctx: The operation context.
            token: The operation token.
        """
        ...


class BaseServiceCollectionHandler(AbstractHandler):
    """
    A Nexus handler, managing a collection of Nexus service handlers.

    Operation requests are delegated to a :py:class:`ServiceHandler` based on the service
    name in the operation context.
    """

    def __init__(
        self,
        user_service_handlers: Sequence[Any],
        executor: Optional[concurrent.futures.Executor] = None,
    ):
        """Initialize a :py:class:`Handler` instance from user service handler instances.

        The user service handler instances must have been decorated with the
        :py:func:`@nexusrpc.handler.service_handler` decorator.

        Args:
            user_service_handlers: A sequence of user service handlers.
            executor: A concurrent.futures.Executor in which to run non-`async def` operation handlers.
        """
        self.executor = _Executor(executor) if executor else None
        self.service_handlers = self._register_service_handlers(user_service_handlers)

    def _register_service_handlers(
        self, user_service_handlers: Sequence[Any]
    ) -> Mapping[str, ServiceHandler]:
        service_handlers = {}
        for sh in user_service_handlers:
            if isinstance(sh, type):
                raise TypeError(
                    f"Expected a service instance, but got a class: {type(sh)}. "
                    "Nexus service handlers must be supplied as instances, not classes."
                )
            # Users may register ServiceHandler instances directly.
            if not isinstance(sh, ServiceHandler):
                # It must be a user service handler instance (i.e. an instance of a class
                # decorated with @nexusrpc.handler.service_handler).
                sh = ServiceHandler.from_user_instance(sh)
            if sh.service.name in service_handlers:
                raise RuntimeError(
                    f"Service '{sh.service.name}' has already been registered."
                )
            service_handlers[sh.service.name] = sh
        return service_handlers

    def _get_service_handler(self, service_name: str) -> ServiceHandler:
        """Return a service handler, given the service name."""
        service = self.service_handlers.get(service_name)
        if service is None:
            raise HandlerError(
                f"No handler for service '{service_name}'.",
                type=HandlerErrorType.NOT_FOUND,
            )
        return service


class Handler(BaseServiceCollectionHandler):
    """
    A Nexus handler manages a collection of Nexus service handlers.

    Operation requests are dispatched to a :py:class:`ServiceHandler` based on the
    service name in the operation context.

    This class supports user operation handlers that are either `async def` or `def`. If
    `def` user operation handlers are to be supported, an executor must be provided.

    The methods of this class itself are `async def`. There is currently no alternative
    Handler class with `def` methods.

    Example:
        .. code-block:: python

            import concurrent.futures
            from nexusrpc.handler import Handler

            # Create service handler instances
            my_service = MyServiceHandler()

            # Create handler with async operations only
            handler = Handler([my_service])

            # Create handler that supports both async and sync operations
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)
            handler = Handler([my_service], executor=executor)

            # Use handler to process requests
            result = await handler.start_operation(ctx, input_lazy_value)
    """

    def __init__(
        self,
        user_service_handlers: Sequence[Any],
        executor: Optional[concurrent.futures.Executor] = None,
    ):
        super().__init__(user_service_handlers, executor=executor)
        if not self.executor:
            self._validate_all_operation_handlers_are_async()

    async def start_operation(
        self,
        ctx: StartOperationContext,
        input: LazyValueT,
    ) -> Union[
        StartOperationResultSync[Any],
        StartOperationResultAsync,
    ]:
        """Handle a Start Operation request.

        Args:
            ctx: The operation context.
            input: The input to the operation, as a LazyValue.
        """
        service_handler = self._get_service_handler(ctx.service)
        op_handler = service_handler._get_operation_handler(ctx.operation)
        op = service_handler.service.operations[ctx.operation]
        deserialized_input = await input.consume(as_type=op.input_type)
        # TODO(preview): apply middleware stack
        if is_async_callable(op_handler.start):
            return await op_handler.start(ctx, deserialized_input)
        else:
            assert self.executor
            return await self.executor.submit_to_event_loop(
                op_handler.start, ctx, deserialized_input
            )

    async def cancel_operation(self, ctx: CancelOperationContext, token: str) -> None:
        """Handle a Cancel Operation request.

        Args:
            ctx: The operation context.
            token: The operation token.
        """
        service_handler = self._get_service_handler(ctx.service)
        op_handler = service_handler._get_operation_handler(ctx.operation)
        if is_async_callable(op_handler.cancel):
            return await op_handler.cancel(ctx, token)
        else:
            assert self.executor
            return self.executor.submit(op_handler.cancel, ctx, token).result()

    async def fetch_operation_info(
        self, ctx: FetchOperationInfoContext, token: str
    ) -> OperationInfo:
        service_handler = self._get_service_handler(ctx.service)
        op_handler = service_handler._get_operation_handler(ctx.operation)
        if is_async_callable(op_handler.fetch_info):
            return await op_handler.fetch_info(ctx, token)
        else:
            assert self.executor
            return self.executor.submit(op_handler.fetch_info, ctx, token).result()

    async def fetch_operation_result(
        self, ctx: FetchOperationResultContext, token: str
    ) -> Any:
        if ctx.wait is not None or ctx.headers.get("request-timeout"):
            raise NotImplementedError(
                "The Nexus SDK is in pre-release and does not support the fetch result "
                "wait parameter or request-timeout header."
            )
        service_handler = self._get_service_handler(ctx.service)
        op_handler = service_handler._get_operation_handler(ctx.operation)
        if is_async_callable(op_handler.fetch_result):
            return await op_handler.fetch_result(ctx, token)
        else:
            assert self.executor
            return self.executor.submit(op_handler.fetch_result, ctx, token).result()

    def _validate_all_operation_handlers_are_async(self) -> None:
        for service_handler in self.service_handlers.values():
            for op_handler in service_handler.operation_handlers.values():
                self._assert_async_callable(op_handler.start)
                self._assert_async_callable(op_handler.cancel)
                self._assert_async_callable(op_handler.fetch_info)
                self._assert_async_callable(op_handler.fetch_result)

    def _assert_async_callable(
        self, method: Callable[..., Any]
    ) -> TypeGuard[Callable[..., Awaitable[Any]]]:
        if not is_async_callable(method):
            raise RuntimeError(
                f"Operation handler method {method} is not an `async def` method, "
                f"but you have not supplied an executor to nexusrpc.handler.Handler. "
            )
        return True


@dataclass(frozen=True)
class ServiceHandler:
    """Internal representation of an instance of a user's service handler class.

    A service handler is a class decorated with
    :py:func:`@nexusrpc.handler.service_handler` that defines operation handler methods
    using decorators such as :py:func:`@nexusrpc.handler.operation_handler`.

    Instances of this class are created automatically from user service implementation
    instances on creation of a Handler instance, at Nexus handler start time. While the
    user's class defines operation handlers as factory methods to be called at handler
    start time, this class contains the :py:class:`OperationHandler` instances
    themselves.

    You may create instances of this class manually and pass them to the Handler
    constructor, for example when programmatically creating Nexus service
    implementations.
    """

    service: ServiceDefinition
    operation_handlers: dict[str, OperationHandler[Any, Any]]

    @classmethod
    def from_user_instance(cls, user_instance: Any) -> Self:
        """Create a :py:class:`ServiceHandler` from a user service instance."""

        service = get_service_definition(user_instance.__class__)
        if not isinstance(service, ServiceDefinition):
            raise RuntimeError(
                f"Service '{user_instance}' does not have a service definition. "
                f"Use the :py:func:`@nexusrpc.handler.service_handler` decorator on your class to define "
                f"a Nexus service implementation."
            )

        # Construct a map of operation handlers keyed by the op name from the service
        # definition (i.e. by the name by which the operation can be requested)
        factories_by_method_name = collect_operation_handler_factories_by_method_name(
            user_instance.__class__, service
        )
        op_handlers = {
            op_name: factories_by_method_name[op.method_name](user_instance)
            for op_name, op in service.operations.items()
            # TODO(preview): op.method_name should be non-nullable
            if op.method_name
        }
        return cls(
            service=service,
            operation_handlers=op_handlers,
        )

    def _get_operation_handler(self, operation: str) -> OperationHandler[Any, Any]:
        """Return an operation handler, given the operation name."""
        if operation not in self.service.operations:
            raise HandlerError(
                f"Nexus service definition '{self.service.name}' has no operation "
                f"'{operation}'. There are {len(self.service.operations)} operations "
                f"in the definition.",
                type=HandlerErrorType.NOT_FOUND,
            )
        operation_handler = self.operation_handlers.get(operation)
        if operation_handler is None:
            raise HandlerError(
                f"Nexus service implementation '{self.service.name}' has no handler for "
                f"operation '{operation}'. There are {len(self.operation_handlers)} "
                f"available operation handlers.",
                type=HandlerErrorType.NOT_FOUND,
            )
        return operation_handler


class _Executor:
    """An executor for synchronous functions."""

    # Users are require to pass in a `concurrent.futures.Executor` in order to use
    # non-`async def`s. This is what `run_in_executor` is documented to require.
    # This means that nexusrpc initially has a hard-coded dependency on the asyncio
    # event loop; if necessary we can add the ability to pass in an event loop
    # implementation at the level of Handler.

    def __init__(self, executor: concurrent.futures.Executor):
        self._executor = executor

    def submit_to_event_loop(
        self, fn: Callable[..., Any], *args: Any
    ) -> Awaitable[Any]:
        return asyncio.get_event_loop().run_in_executor(self._executor, fn, *args)

    def submit(
        self, fn: Callable[..., Any], *args: Any
    ) -> concurrent.futures.Future[Any]:
        return self._executor.submit(fn, *args)
