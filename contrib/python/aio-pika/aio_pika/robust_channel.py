import asyncio
import warnings
from collections import defaultdict
from itertools import chain
from typing import Any, DefaultDict, Dict, Optional, Set, Type, Union, cast
from warnings import warn

import aiormq

from .abc import (
    AbstractConnection,
    AbstractRobustChannel,
    AbstractRobustExchange,
    AbstractRobustQueue,
    TimeoutType,
)
from .channel import Channel
from .exchange import Exchange, ExchangeType
from .log import get_logger
from .queue import Queue
from .robust_exchange import RobustExchange
from .robust_queue import RobustQueue
from .tools import CallbackCollection


log = get_logger(__name__)


class RobustChannel(Channel, AbstractRobustChannel):
    """Channel abstraction"""

    QUEUE_CLASS: Type[Queue] = RobustQueue
    EXCHANGE_CLASS: Type[Exchange] = RobustExchange

    RESTORE_RETRY_DELAY: int = 2

    _exchanges: DefaultDict[str, AbstractRobustExchange]
    _queues: DefaultDict[str, Set[RobustQueue]]
    default_exchange: RobustExchange

    def __init__(
        self,
        connection: AbstractConnection,
        channel_number: Optional[int] = None,
        publisher_confirms: bool = True,
        on_return_raises: bool = False,
    ):
        """

        :param connection: :class:`aio_pika.adapter.AsyncioConnection` instance
        :param loop:
            Event loop (:func:`asyncio.get_event_loop()` when :class:`None`)
        :param future_store: :class:`aio_pika.common.FutureStore` instance
        :param publisher_confirms:
            False if you don't need delivery confirmations
            (in pursuit of performance)
        """

        super().__init__(
            connection=connection,
            channel_number=channel_number,
            publisher_confirms=publisher_confirms,
            on_return_raises=on_return_raises,
        )

        self._exchanges = defaultdict()
        self._queues = defaultdict(set)
        self._prefetch_count: int = 0
        self._prefetch_size: int = 0
        self._global_qos: bool = False
        self.reopen_callbacks = CallbackCollection(self)
        self.__restore_lock = asyncio.Lock()
        self.__restored = asyncio.Event()

        self.close_callbacks.remove(self._set_closed_callback)

    async def ready(self) -> None:
        await self._connection.ready()
        await self.__restored.wait()

    async def get_underlay_channel(self) -> aiormq.abc.AbstractChannel:
        await self._connection.ready()
        return await super().get_underlay_channel()

    async def restore(self, channel: Any = None) -> None:
        if channel is not None:
            warnings.warn(
                "Channel argument will be ignored because you "
                "don't need to pass this anymore.",
                DeprecationWarning,
            )

        async with self.__restore_lock:
            if self.__restored.is_set():
                return

            await self.reopen()
            self.__restored.set()

    async def _on_close(
        self, closing: asyncio.Future
    ) -> Optional[BaseException]:
        exc = await super()._on_close(closing)

        if isinstance(exc, asyncio.CancelledError):
            # Connection forced close (e.g., stuck connection).
            # Clear restored but do NOT set _closed - the robust connection
            # will restore this channel via restore() -> reopen() -> _open()
            # which properly handles _closed state reset.
            self.__restored.clear()
            return exc

        in_restore_state = not self.__restored.is_set()
        self.__restored.clear()

        if self._closed.done() or in_restore_state:
            return exc

        await self.restore()

        return exc

    async def close(
        self,
        exc: Optional[aiormq.abc.ExceptionType] = None,
    ) -> None:
        # Avoid recovery when channel is explicitely closed using this method
        self.__restored.clear()
        await super().close(exc)

    async def reopen(self) -> None:
        await super().reopen()
        await self.reopen_callbacks()

    async def _on_open(self) -> None:
        if not hasattr(self, "default_exchange"):
            await super()._on_open()

        exchanges = self._exchanges.values()
        queues = tuple(chain(*self._queues.values()))
        channel = await self.get_underlay_channel()

        await channel.basic_qos(
            prefetch_count=self._prefetch_count,
            prefetch_size=self._prefetch_size,
            global_=self._global_qos,
        )

        for exchange in exchanges:
            await exchange.restore()

        for queue in queues:
            await queue.restore()

        if hasattr(self, "default_exchange"):
            self.default_exchange.channel = self

        self.__restored.set()

    async def set_qos(
        self,
        prefetch_count: int = 0,
        prefetch_size: int = 0,
        global_: bool = False,
        timeout: TimeoutType = None,
        all_channels: Optional[bool] = None,
    ) -> aiormq.spec.Basic.QosOk:
        if all_channels is not None:
            warn('Use "global_" instead of "all_channels"', DeprecationWarning)
            global_ = all_channels

        await self.ready()

        self._prefetch_count = prefetch_count
        self._prefetch_size = prefetch_size
        self._global_qos = global_

        return await super().set_qos(
            prefetch_count=prefetch_count,
            prefetch_size=prefetch_size,
            global_=global_,
            timeout=timeout,
        )

    async def declare_exchange(
        self,
        name: str,
        type: Union[ExchangeType, str] = ExchangeType.DIRECT,
        durable: bool = False,
        auto_delete: bool = False,
        internal: bool = False,
        passive: bool = False,
        arguments: Optional[Dict[str, Any]] = None,
        timeout: TimeoutType = None,
        robust: bool = True,
    ) -> AbstractRobustExchange:
        """
        :param robust: If True, the exchange will be re-declared during
        reconnection.
        Set to False for temporary exchanges that should not be restored.
        """
        await self.ready()
        # Passive is True so expecting the exchange to be already declared
        # if we can just return it instead of creating a new class instance
        if passive and name in self._exchanges:
            return self._exchanges[name]

        exchange = await super().declare_exchange(
            name=name,
            type=type,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            passive=passive,
            arguments=arguments,
            timeout=timeout,
        )
        exchange = cast(AbstractRobustExchange, exchange)

        if not internal and robust:
            self._exchanges[name] = exchange

        return exchange

    async def exchange_delete(
        self,
        exchange_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        nowait: bool = False,
    ) -> aiormq.spec.Exchange.DeleteOk:
        await self.ready()
        result = await super().exchange_delete(
            exchange_name=exchange_name,
            timeout=timeout,
            if_unused=if_unused,
            nowait=nowait,
        )
        self._exchanges.pop(exchange_name, None)
        return result

    async def declare_queue(
        self,
        name: Optional[str] = None,
        *,
        durable: bool = False,
        exclusive: bool = False,
        passive: bool = False,
        auto_delete: bool = False,
        arguments: Optional[Dict[str, Any]] = None,
        timeout: TimeoutType = None,
        robust: bool = True,
    ) -> AbstractRobustQueue:
        """
        :param robust: If True, the queue will be re-declared during
        reconnection.
        Set to False for temporary queues that should not be restored.
        """
        await self.ready()
        # Passive is True so expecting the queue to be already declared
        # if we can just return it instead of creating a new class instance
        if passive and name and name in self._queues:
            return list(self._queues[name])[0]

        queue = await super().declare_queue(
            name=name,
            durable=durable,
            exclusive=exclusive,
            passive=passive,
            auto_delete=auto_delete,
            arguments=arguments,
            timeout=timeout,
        )
        queue = cast(RobustQueue, queue)

        if robust:
            self._queues[queue.name].add(queue)
        return queue

    async def queue_delete(
        self,
        queue_name: str,
        timeout: TimeoutType = None,
        if_unused: bool = False,
        if_empty: bool = False,
        nowait: bool = False,
    ) -> aiormq.spec.Queue.DeleteOk:
        await self.ready()
        result = await super().queue_delete(
            queue_name=queue_name,
            timeout=timeout,
            if_unused=if_unused,
            if_empty=if_empty,
            nowait=nowait,
        )
        self._queues.pop(queue_name, None)
        return result


__all__ = ("RobustChannel",)
