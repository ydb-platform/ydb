__all__ = [
    "DishkaAioInterceptor",
    "DishkaInterceptor",
    "FromDishka",
    "GrpcioProvider",
    "inject",
]

from collections.abc import Awaitable, Callable, Iterator
from contextvars import ContextVar
from inspect import isasyncgenfunction, iscoroutine, iscoroutinefunction
from typing import Any, ParamSpec, TypeVar

from google.protobuf.message import Message
from grpc import (
    HandlerCallDetails,
    RpcMethodHandler,
    ServerInterceptor,
    ServicerContext,
    stream_stream_rpc_method_handler,
    stream_unary_rpc_method_handler,
    unary_stream_rpc_method_handler,
    unary_unary_rpc_method_handler,
)
from grpc.aio import ServerInterceptor as AioServerInterceptor

from dishka import (
    AsyncContainer,
    Container,
    FromDishka,
    Provider,
    Scope,
    from_context,
)
from dishka.integrations.base import wrap_injection

P = ParamSpec("P")
RT = TypeVar("RT")

_dishka_scoped_container = ContextVar("_dishka_scoped_container")


def inject(func: Callable[P, RT]) -> Callable[P, RT]:
    return wrap_injection(
        func=func,
        is_async=iscoroutinefunction(func) or isasyncgenfunction(func),
        container_getter=lambda _, __: _dishka_scoped_container.get(),
    )


class GrpcioProvider(Provider):
    message = from_context(Message, scope=Scope.REQUEST)
    servicer_context = from_context(ServicerContext, scope=Scope.SESSION)


class DishkaInterceptor(ServerInterceptor):   # type: ignore[misc]
    def __init__(self, container: Container) -> None:
        self._container = container

    def intercept_service(
        self,
        continuation: Callable[
            [HandlerCallDetails],
            RpcMethodHandler,
        ],
        handler_call_details: HandlerCallDetails,
    ) -> RpcMethodHandler:
        rpc_handler = continuation(handler_call_details)

        def unary_unary_behavior(
            request: Message,
            context: ServicerContext,
        ) -> Any:
            context_ = {
                Message: request,
                ServicerContext: context,
            }
            with self._container(context=context_) as container:
                _dishka_scoped_container.set(container)
                return rpc_handler.unary_unary(request, context)

        def stream_unary_behavior(
            request_iterator: Iterator[Message],
            context: ServicerContext,
        ) -> Any:
            context_ = {ServicerContext: context}
            with self._container(
                context=context_,
                scope=Scope.SESSION,
            ) as container:
                _dishka_scoped_container.set(container)
                return rpc_handler.stream_unary(
                    request_iterator, context,
                )

        def unary_stream_behavior(
            request: Message,
            context: ServicerContext,
        ) -> Any:
            context_ = {
                Message: request,
                ServicerContext: context,
            }
            with self._container(context=context_) as container:
                _dishka_scoped_container.set(container)
                yield from rpc_handler.unary_stream(request, context)

        def stream_stream_behavior(
            request_iterator: Iterator[Message],
            context: ServicerContext,
        ) -> Any:
            context_ = {ServicerContext: context}
            with self._container(
                context=context_,
                scope=Scope.SESSION,
            ) as container:
                _dishka_scoped_container.set(container)
                yield from rpc_handler.stream_stream(request_iterator, context)

        if rpc_handler.unary_unary:
            return unary_unary_rpc_method_handler(
                unary_unary_behavior,
                rpc_handler.request_deserializer,
                rpc_handler.response_serializer,
            )
        elif rpc_handler.stream_unary:
            return stream_unary_rpc_method_handler(
                stream_unary_behavior,
                rpc_handler.request_deserializer,
                rpc_handler.response_serializer,
            )
        elif rpc_handler.unary_stream:
            return unary_stream_rpc_method_handler(
                unary_stream_behavior,
                rpc_handler.request_deserializer,
                rpc_handler.response_serializer,
            )
        elif rpc_handler.stream_stream:
            return stream_stream_rpc_method_handler(
                stream_stream_behavior,
                rpc_handler.request_deserializer,
                rpc_handler.response_serializer,
            )

        return rpc_handler


class DishkaAioInterceptor(AioServerInterceptor):  # type: ignore[misc]
    def __init__(self, container: AsyncContainer) -> None:
        self._container = container

    async def intercept_service(  # noqa: C901
        self,
        continuation: Callable[
            [HandlerCallDetails],
            Awaitable[RpcMethodHandler],
        ],
        handler_call_details: HandlerCallDetails,
    ) -> RpcMethodHandler:
        rpc_handler = await continuation(handler_call_details)

        async def unary_unary_behavior(
            request: Message,
            context: ServicerContext,
        ) -> Any:
            context_ = {
                Message: request,
                ServicerContext: context,
            }
            async with self._container(context=context_) as container:
                _dishka_scoped_container.set(container)
                return await rpc_handler.unary_unary(request, context)

        async def stream_unary_behavior(
            request_iterator: Iterator[Message],
            context: ServicerContext,
        ) -> Any:
            context_ = {ServicerContext: context}
            async with self._container(
                context=context_,
                scope=Scope.SESSION,
            ) as container:
                _dishka_scoped_container.set(container)
                return await rpc_handler.stream_unary(
                    request_iterator, context,
                )

        async def unary_stream_behavior(
            request: Message,
            context: ServicerContext,
        ) -> Any:
            context_ = {
                Message: request,
                ServicerContext: context,
            }
            async with self._container(
                context=context_,
                scope=Scope.REQUEST,
            ) as container:
                _dishka_scoped_container.set(container)
                res = rpc_handler.unary_stream(request, context)
                if iscoroutine(res):
                    return await res
                async for result in res:
                    await context.write(result)
                return None

        async def stream_stream_behavior(
            request_iterator: Iterator[Message],
            context: ServicerContext,
        ) -> Any:
            context_ = {ServicerContext: context}
            async with self._container(
                context=context_,
                scope=Scope.SESSION,
            ) as container:
                _dishka_scoped_container.set(container)
                res = rpc_handler.stream_stream(request_iterator, context)
                if iscoroutine(res):
                    return await res
                async for result in res:
                    await context.write(result)
                return None

        if rpc_handler.unary_unary:
            return unary_unary_rpc_method_handler(
                unary_unary_behavior,
                rpc_handler.request_deserializer,
                rpc_handler.response_serializer,
            )
        elif rpc_handler.stream_unary:
            return stream_unary_rpc_method_handler(
                stream_unary_behavior,
                rpc_handler.request_deserializer,
                rpc_handler.response_serializer,
            )
        elif rpc_handler.unary_stream:
            return unary_stream_rpc_method_handler(
                unary_stream_behavior,
                rpc_handler.request_deserializer,
                rpc_handler.response_serializer,
            )
        elif rpc_handler.stream_stream:
            return stream_stream_rpc_method_handler(
                stream_stream_behavior,
                rpc_handler.request_deserializer,
                rpc_handler.response_serializer,
            )

        return rpc_handler
