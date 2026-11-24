from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod

from google.protobuf.internal.containers import RepeatedCompositeFieldContainer

from temporalio.api.common.v1.message_pb2 import Payload

PayloadSequence = list[Payload] | RepeatedCompositeFieldContainer[Payload]


class VisitorFunctions(ABC):
    """Functions invoked by generated payload visitors."""

    @abstractmethod
    async def visit_payload(self, payload: Payload) -> None:
        """Visit a single payload."""
        ...

    @abstractmethod
    async def visit_payloads(self, payloads: PayloadSequence) -> None:
        """Visit a sequence of payloads together."""
        ...

    async def visit_system_nexus_envelope(self, _payload: Payload) -> None:
        """Visit a recognized system Nexus envelope payload."""
        return None

    def checkpoint(self) -> int | None:
        """Return a marker for visits scheduled after this point, if supported."""
        return None

    async def drain_since(self, _checkpoint: int) -> None:
        """Wait for visits scheduled after ``checkpoint`` to finish."""
        return None


class BoundedVisitorFunctions(VisitorFunctions):
    """Wraps VisitorFunctions to cap concurrent payload visits via a semaphore.

    After the full traversal, call drain() to await all in-flight tasks.
    """

    def __init__(self, inner: VisitorFunctions, concurrency_limit: int) -> None:
        """Create a bounded wrapper around the given visitor functions."""
        self._inner = inner
        self._sem = asyncio.Semaphore(concurrency_limit)
        self._tasks: list[asyncio.Task[None]] = []

    async def visit_payload(self, payload: Payload) -> None:
        """Visit a single payload once capacity is available."""
        await self._sem.acquire()

        async def _run() -> None:
            try:
                await self._inner.visit_payload(payload)
            finally:
                self._sem.release()

        self._tasks.append(asyncio.create_task(_run()))

    async def visit_payloads(self, payloads: PayloadSequence) -> None:
        """Visit a sequence of payloads once capacity is available."""
        await self._sem.acquire()

        async def _run() -> None:
            try:
                await self._inner.visit_payloads(payloads)
            finally:
                self._sem.release()

        self._tasks.append(asyncio.create_task(_run()))

    async def visit_system_nexus_envelope(self, payload: Payload) -> None:
        """Visit a system Nexus envelope payload once capacity is available."""
        await self._sem.acquire()

        async def _run() -> None:
            try:
                await self._inner.visit_system_nexus_envelope(payload)
            finally:
                self._sem.release()

        self._tasks.append(asyncio.create_task(_run()))

    def checkpoint(self) -> int:
        """Return a marker for tasks scheduled after this point."""
        return len(self._tasks)

    async def drain_since(self, checkpoint: int) -> None:
        """Wait for tasks scheduled after ``checkpoint`` to finish.

        This lets system-envelope traversal finish mutating its decoded value
        before that value is serialized again, without waiting for unrelated
        visits that were already in progress.
        """
        await self._drain_tasks(self._tasks[checkpoint:])

    async def drain(self) -> None:
        """Wait for all in-flight background tasks to complete.

        On cancellation or error, cancels all remaining tasks and awaits
        them so their finally blocks run before this coroutine returns.
        """
        await self._drain_tasks(self._tasks)

    async def _drain_tasks(self, tasks: list[asyncio.Task[None]]) -> None:
        """Wait for the given tasks, cancelling all tasks if one fails."""
        if not tasks:
            return
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            raise
