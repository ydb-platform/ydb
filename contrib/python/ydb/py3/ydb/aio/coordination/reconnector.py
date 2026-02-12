from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

from ... import issues
from ..._grpc.grpcwrapper.common_utils import IToProto
from ..._grpc.grpcwrapper.ydb_coordination import FromServer
from .stream import CoordinationStream

logger = logging.getLogger(__name__)


class CoordinationReconnector:
    def __init__(self, driver, node_path: str, timeout_millis: int = 30000):
        self._driver = driver
        self._node_path = node_path
        self._timeout_millis = timeout_millis
        self._wait_timeout = timeout_millis / 1000

        self._stream = None
        self._session_id = None

        self._pending_futures: Dict[int, asyncio.Future[Any]] = {}
        self._pending_requests: Dict[int, IToProto] = {}

        self._send_lock = asyncio.Lock()
        self._connection_task: Optional[asyncio.Task[Any]] = None
        self._closed = False

    async def stop(self):
        self._closed = True

        if self._connection_task:
            self._connection_task.cancel()
            try:
                await self._connection_task
            except asyncio.CancelledError:
                pass

        if self._stream:
            await self._stream.close()

        for fut in self._pending_futures.values():
            if not fut.done():
                fut.set_exception(asyncio.CancelledError())

        self._pending_futures.clear()
        self._pending_requests.clear()

    async def send_and_wait(self, req: IToProto):
        if self._closed:
            raise issues.Error("Reconnector closed")

        if self._connection_task is None:
            self._connection_task = asyncio.create_task(self._connection_loop())

        while not self._stream or self._stream._closed:
            await asyncio.sleep(0)

        req_id = getattr(req, "req_id")
        loop = asyncio.get_running_loop()
        fut = loop.create_future()

        self._pending_futures[req_id] = fut
        self._pending_requests[req_id] = req

        async with self._send_lock:
            await self._stream.send(req)

        return await asyncio.wait_for(fut, self._wait_timeout)

    async def _connection_loop(self):
        while not self._closed:
            try:
                stream = CoordinationStream(self._driver)
                await stream.start_session(
                    self._node_path,
                    self._timeout_millis,
                    session_id=self._session_id,
                )

                self._stream = stream
                self._session_id = stream.session_id

                for req in self._pending_requests.values():
                    await stream.send(req)

                await self._dispatch_loop(stream)

            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.debug("Coordination stream error: %r", exc)
            finally:
                if self._stream:
                    await self._stream.close()
                    self._stream = None

    async def _dispatch_loop(self, stream):
        while not self._closed and self._stream is stream:
            resp = await stream.receive(self._wait_timeout)
            if not resp:
                continue

            fs = FromServer.from_proto(resp)
            payload = next(
                (
                    getattr(fs, name)
                    for name in (
                        "acquire_semaphore_result",
                        "release_semaphore_result",
                        "describe_semaphore_result",
                        "create_semaphore_result",
                        "update_semaphore_result",
                        "delete_semaphore_result",
                    )
                    if fs.raw.HasField(name)
                ),
                None,
            )

            if not payload:
                continue

            fut = self._pending_futures.pop(payload.req_id, None)
            self._pending_requests.pop(payload.req_id, None)

            if fut and not fut.done():
                fut.set_result(payload)
