from __future__ import annotations

import asyncio
import logging
from typing import Optional

from ... import issues, _apis
from ..._grpc.grpcwrapper.common_utils import IToProto, GrpcWrapperAsyncIO
from ..._grpc.grpcwrapper.ydb_coordination import (
    FromServer,
    SessionStart,
    Ping,
)

logger = logging.getLogger(__name__)


class CoordinationStream:
    def __init__(self, driver):
        self._driver = driver
        self._stream = GrpcWrapperAsyncIO(FromServer.from_proto)

        self._incoming = asyncio.Queue()
        self._reader_task: Optional[asyncio.Task] = None

        self._closed = False
        self.session_id: Optional[int] = None

    async def start_session(
        self,
        path: str,
        timeout_millis: int,
        session_id: Optional[int] = None,
    ):
        await self._stream.start(
            self._driver,
            _apis.CoordinationService.Stub,
            _apis.CoordinationService.Session,
        )

        self._stream.write(
            SessionStart(
                path=path,
                timeout_millis=timeout_millis,
                session_id=int(session_id) if session_id is not None else 0,
            )
        )

        while True:
            resp = await self._stream.receive(
                timeout=3,
                is_coordination_calls=True,
            )
            if resp is None:
                continue

            fs = FromServer.from_proto(resp)
            if fs.session_started:
                self.session_id = int(fs.session_started.session_id)
                break

        self._reader_task = asyncio.create_task(self._reader_loop())

    async def _reader_loop(self):
        try:
            while True:
                resp = await self._stream.receive(
                    timeout=3,
                    is_coordination_calls=True,
                )
                if resp is None:
                    continue

                fs = FromServer.from_proto(resp)

                if fs.opaque:
                    try:
                        self._stream.write(Ping(fs.opaque))
                    except Exception:
                        break
                    continue

                await self._incoming.put(resp)

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.debug("CoordinationStream reader stopped: %r", exc)
        finally:
            self._closed = True
            await self._incoming.put(None)

            try:
                await self._stream.close()
            except Exception:
                pass

    async def send(self, req: IToProto):
        if self._closed:
            raise issues.Error("Coordination stream closed")
        self._stream.write(req)

    async def receive(self, timeout: Optional[float] = None):
        if self._closed:
            raise issues.Error("Coordination stream closed")

        if timeout is None:
            return await self._incoming.get()

        return await asyncio.wait_for(self._incoming.get(), timeout)

    async def close(self):
        if self._closed:
            return

        self._closed = True

        if self._reader_task:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except asyncio.CancelledError:
                pass
            self._reader_task = None

        try:
            await self._stream.close()
        except Exception:
            pass
