import abc
import asyncio
import warnings
from collections import deque
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Tuple

import aiohttp
from aiohttp.client_exceptions import ClientError
from yarl import URL

from .log import logger
from .mypy_types import OptLoop
from .record import Record


DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=5 * 60)
BATCHES_MAX_COUNT = 10 ** 4

DataList = List[Dict[str, Any]]
SndBatches = Deque[Tuple[int, DataList]]
SendDataCoro = Callable[[DataList], Awaitable[bool]]


class TransportABC(abc.ABC):
    @abc.abstractmethod
    def send(self, record: Record) -> None:  # pragma: no cover
        """Sends data to zipkin collector."""
        pass

    @abc.abstractmethod
    async def close(self) -> None:  # pragma: no cover
        """Performs additional cleanup actions if required."""
        pass


class StubTransport(TransportABC):
    """Dummy transport, which logs spans to a limited queue."""

    def __init__(self, queue_length: int = 100) -> None:
        logger.info("Zipkin address was not provided, using stub transport")
        self.records: Deque[Record] = deque(maxlen=queue_length)

    def send(self, record: Record) -> None:
        self.records.append(record)

    async def close(self) -> None:
        pass


class BatchManager:
    def __init__(
        self,
        max_size: int,
        send_interval: float,
        attempt_count: int,
        send_data: SendDataCoro,
    ) -> None:
        loop = asyncio.get_event_loop()
        self._max_size = max_size
        self._send_interval = send_interval
        self._send_data = send_data
        self._attempt_count = attempt_count
        self._max = BATCHES_MAX_COUNT
        self._sending_batches: SndBatches = deque([], maxlen=self._max)
        self._active_batch: Optional[DataList] = None
        self._ender = loop.create_future()
        self._timer: Optional[asyncio.Future[Any]] = None
        self._sender_task = asyncio.ensure_future(self._sender_loop())

    def add(self, data: Dict[str, Any]) -> None:
        if self._active_batch is None:
            self._active_batch = []
        self._active_batch.append(data)
        if len(self._active_batch) >= self._max_size:
            self._sending_batches.append((0, self._active_batch))
            self._active_batch = None
            if self._timer is not None and not self._timer.done():
                self._timer.cancel()

    async def stop(self) -> None:
        self._ender.set_result(None)

        await self._sender_task
        await self._send()

        if self._timer is not None:
            self._timer.cancel()
            try:
                await self._timer
            except asyncio.CancelledError:
                pass

    async def _sender_loop(self) -> None:
        while not self._ender.done():
            await self._wait()
            await self._send()

    async def _send(self) -> None:
        if self._active_batch is not None:
            self._sending_batches.append((0, self._active_batch))
            self._active_batch = None

        batches = self._sending_batches.copy()
        self._sending_batches = deque([], maxlen=self._max)
        for attempt, batch in batches:
            if not await self._send_data(batch):
                attempt += 1
                if attempt < self._attempt_count:
                    self._sending_batches.append((attempt, batch))

    async def _wait(self) -> None:
        self._timer = asyncio.ensure_future(asyncio.sleep(self._send_interval))

        await asyncio.wait(
            [self._timer, self._ender],
            return_when=asyncio.FIRST_COMPLETED,
        )


class Transport(TransportABC):
    def __init__(
        self,
        address: str,
        send_interval: float = 5,
        loop: OptLoop = None,
        *,
        send_max_size: int = 100,
        send_attempt_count: int = 3,
        send_timeout: Optional[aiohttp.ClientTimeout] = None
    ) -> None:
        if loop is not None:
            warnings.warn(
                "loop parameter is deprecated and ignored",
                DeprecationWarning,
                stacklevel=2,
            )
        self._address = URL(address)
        self._queue: DataList = []
        self._closing = False
        self._send_interval = send_interval
        if send_timeout is None:
            send_timeout = DEFAULT_TIMEOUT
        self._session = aiohttp.ClientSession(
            timeout=send_timeout,
            headers={"Content-Type": "application/json"},
        )
        self._batch_manager = BatchManager(
            send_max_size,
            send_interval,
            send_attempt_count,
            self._send_data,
        )

    def send(self, record: Record) -> None:
        data = record.asdict()
        self._batch_manager.add(data)

    async def _send_data(self, data: DataList) -> bool:
        try:

            async with self._session.post(self._address, json=data) as resp:
                body = await resp.text()
                if resp.status >= 300:
                    msg = "zipkin responded with code: {} and body: {}".format(
                        resp.status, body
                    )
                    raise RuntimeError(msg)

        except (asyncio.TimeoutError, ClientError):
            return False
        except Exception as exc:  # pylint: disable=broad-except
            # that code should never fail and break application
            logger.error("Can not send spans to zipkin", exc_info=exc)
        return True

    async def close(self) -> None:
        if self._closing:
            return

        self._closing = True
        await self._batch_manager.stop()
        await self._session.close()
