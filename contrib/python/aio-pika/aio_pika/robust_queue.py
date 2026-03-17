import asyncio
import uuid
import warnings
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, Union

import aiormq
from aiormq import ChannelInvalidStateError
from pamqp.common import Arguments

from .abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueueIterator,
    AbstractRobustQueue,
    ConsumerTag,
    TimeoutType,
)
from .exchange import ExchangeParamType
from .log import get_logger
from .queue import Queue, QueueIterator


log = get_logger(__name__)


class RobustQueue(Queue, AbstractRobustQueue):
    __slots__ = ("_consumers", "_bindings")

    _consumers: Dict[ConsumerTag, Dict[str, Any]]
    _bindings: Dict[Tuple[Union[AbstractExchange, str], str], Dict[str, Any]]

    def __init__(
        self,
        channel: AbstractChannel,
        name: Optional[str],
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: Arguments = None,
        passive: bool = False,
    ):

        super().__init__(
            channel=channel,
            name=name or f"amq_{uuid.uuid4().hex}",
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments,
            passive=passive,
        )

        self._consumers = {}
        self._bindings = {}

    async def restore(self, channel: Any = None) -> None:
        if channel is not None:
            warnings.warn(
                "Channel argument will be ignored because you "
                "don't need to pass this anymore.",
                DeprecationWarning,
            )

        await self.declare()
        bindings = tuple(self._bindings.items())
        consumers = tuple(self._consumers.items())

        for (exchange, routing_key), kwargs in bindings:
            await self.bind(exchange, routing_key, **kwargs)

        for consumer_tag, kwargs in consumers:
            await self.consume(consumer_tag=consumer_tag, **kwargs)

    async def bind(
        self,
        exchange: ExchangeParamType,
        routing_key: Optional[str] = None,
        *,
        arguments: Arguments = None,
        timeout: TimeoutType = None,
        robust: bool = True,
    ) -> aiormq.spec.Queue.BindOk:
        if routing_key is None:
            routing_key = self.name

        result = await super().bind(
            exchange=exchange,
            routing_key=routing_key,
            arguments=arguments,
            timeout=timeout,
        )

        if robust:
            self._bindings[(exchange, routing_key)] = dict(
                arguments=arguments,
            )

        return result

    async def unbind(
        self,
        exchange: ExchangeParamType,
        routing_key: Optional[str] = None,
        arguments: Arguments = None,
        timeout: TimeoutType = None,
    ) -> aiormq.spec.Queue.UnbindOk:
        if routing_key is None:
            routing_key = self.name

        result = await super().unbind(
            exchange,
            routing_key,
            arguments,
            timeout,
        )
        self._bindings.pop((exchange, routing_key), None)

        return result

    async def consume(
        self,
        callback: Callable[[AbstractIncomingMessage], Awaitable[Any]],
        no_ack: bool = False,
        exclusive: bool = False,
        arguments: Arguments = None,
        consumer_tag: Optional[ConsumerTag] = None,
        timeout: TimeoutType = None,
        robust: bool = True,
    ) -> ConsumerTag:
        consumer_tag = await super().consume(
            consumer_tag=consumer_tag,
            timeout=timeout,
            callback=callback,
            no_ack=no_ack,
            exclusive=exclusive,
            arguments=arguments,
        )

        if robust:
            self._consumers[consumer_tag] = dict(
                callback=callback,
                no_ack=no_ack,
                exclusive=exclusive,
                arguments=arguments,
            )

        return consumer_tag

    async def cancel(
        self,
        consumer_tag: ConsumerTag,
        timeout: TimeoutType = None,
        nowait: bool = False,
    ) -> aiormq.spec.Basic.CancelOk:
        result = await super().cancel(consumer_tag, timeout, nowait)
        self._consumers.pop(consumer_tag, None)
        return result

    def iterator(self, **kwargs: Any) -> AbstractQueueIterator:
        return RobustQueueIterator(self, **kwargs)


class RobustQueueIterator(QueueIterator):
    """Queue iterator that survives channel reconnection.

    This iterator handles channel disconnection/reconnection gracefully
    by waiting for channel restoration instead of raising StopAsyncIteration.
    """

    RETRY_DELAY: float = 0.5

    def __init__(self, queue: Queue, **kwargs: Any):
        super().__init__(queue, **kwargs)

        # Remove close callback to survive reconnection
        self._amqp_queue.close_callbacks.discard(self._set_closed)

        # But listen to connection close to stop iteration when
        # connection is intentionally closed
        channel = self._amqp_queue.channel
        if hasattr(channel, "_connection"):
            connection = channel._connection
            connection.closed().add_done_callback(self._on_connection_closed)

    def _on_connection_closed(self, _: asyncio.Future) -> None:
        """Handle connection closed - set _closed to stop iteration."""
        if not self._closed.done():
            self._closed.set_result(True)
        self._message_or_closed.set()

    async def consume(self) -> None:
        """Consume with retry on channel errors.

        Waits for channel to be ready before consuming, with backoff delay
        between retries to prevent CPU spinning during reconnection.
        """
        while True:
            try:
                # Wait for channel to be fully ready before consuming
                channel = self._amqp_queue.channel
                if hasattr(channel, "ready"):
                    await channel.ready()

                return await super().consume()
            except ChannelInvalidStateError:
                log.debug(
                    "Channel invalid state in %r, waiting for restoration",
                    self,
                )
                # Backoff to prevent CPU spinning during reconnection
                await asyncio.sleep(self.RETRY_DELAY)

    async def __anext__(self) -> AbstractIncomingMessage:
        """Get next message, handling reconnection gracefully.

        During reconnection, the queue may be empty and _message_or_closed
        may be set due to channel closure. In robust mode, we wait for
        reconnection instead of raising StopAsyncIteration.
        """
        while True:
            # Check if explicitly closed
            if self._closed.done():
                raise StopAsyncIteration

            if not hasattr(self, "_consumer_tag"):
                await self.consume()

            timeout: Optional[float] = self._consume_kwargs.get("timeout")

            if not self._message_or_closed.is_set():
                coroutine: Awaitable[Any] = self._message_or_closed.wait()
                if timeout is not None and timeout > 0:
                    coroutine = asyncio.wait_for(coroutine, timeout=timeout)

                try:
                    await coroutine
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    # Handle timeout same as parent class
                    if timeout is not None:
                        timeout_val = (
                            timeout
                            if timeout > 0
                            else self.DEFAULT_CLOSE_TIMEOUT
                        )
                        log.info(
                            "%r closing with timeout %d seconds",
                            self,
                            timeout_val,
                        )

                    task = asyncio.create_task(self.close())
                    close_coro: Awaitable[Any] = task
                    if timeout is not None:
                        close_coro = asyncio.wait_for(
                            asyncio.shield(task),
                            timeout=timeout_val,
                        )

                    try:
                        await close_coro
                    except asyncio.TimeoutError:
                        self._QueueIterator__closing = task

                    raise

            # Check queue for messages
            if not self._queue.empty():
                msg = self._queue.get_nowait()

                if (
                    self._queue.empty()
                    and not self._amqp_queue.channel.is_closed
                    and not self._closed.done()
                ):
                    self._message_or_closed.clear()

                return msg

            # Queue is empty - check if this is a reconnection scenario
            channel = self._amqp_queue.channel
            if hasattr(channel, "ready"):
                # This is a RobustChannel - check if connection is still alive
                if hasattr(channel, "_connection"):
                    connection = channel._connection
                    # Only wait for reconnection if connection wasn't
                    # intentionally closed
                    if not connection.is_closed and not connection.close_called:
                        # Connection is alive, channel is being restored
                        log.debug(
                            "%r queue empty during channel restoration, "
                            "waiting for reconnection",
                            self,
                        )

                        # Clear the event and wait for channel restoration
                        self._message_or_closed.clear()

                        try:
                            # Wait for channel to become ready
                            await asyncio.wait_for(
                                channel.ready(),
                                timeout=60.0,
                            )

                            # Re-establish consumer if needed
                            if not hasattr(self, "_consumer_tag"):
                                await self.consume()

                            # Continue loop to wait for new messages
                            continue
                        except asyncio.TimeoutError:
                            log.error(
                                "%r timeout waiting for channel reconnection",
                                self,
                            )
                            raise StopAsyncIteration

            # Truly empty and not reconnecting - stop iteration
            raise StopAsyncIteration


__all__ = ("RobustQueue",)
