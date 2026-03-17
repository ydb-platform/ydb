"""
Regression tests for Issue #167: Expose shutdown event to content stream generator.

Problem: When server shuts down, generators get CancelledError with no opportunity
for graceful farewell events or async cleanup. These tests verify that:
1. A user-provided shutdown_event is set by the library on shutdown
2. Generators can use this event to exit cooperatively within a grace period
3. Force cancellation still occurs if generator doesn't exit in time
4. Backward compatibility is maintained when no shutdown_event is provided
"""

import anyio
import pytest

from sse_starlette.sse import AppStatus, EventSourceResponse
from tests.anyio_compat import collapse_excgroups

# Watcher polls every 0.5s; add margin for test system load.
WATCHER_POLL_INTERVAL = 0.5
WATCHER_DETECT_TIMEOUT = WATCHER_POLL_INTERVAL + 0.3


class TestIssue167ShutdownEvent:
    """Tests for cooperative shutdown via shutdown_event parameter."""

    @pytest.mark.asyncio
    async def test_shutdownEvent_whenShutdownDetected_thenEventIsSet(self):
        """The library must set the user's shutdown_event when server shutdown is detected."""
        shutdown_event = anyio.Event()

        async def gen():
            while True:
                yield {"data": "tick"}
                await anyio.sleep(0.1)

        async def mock_send(*args, **kwargs):
            pass

        async def mock_receive():
            await anyio.sleep(100)
            return {"type": "http.disconnect"}

        response = EventSourceResponse(
            gen(),
            ping=0,
            shutdown_event=shutdown_event,
            shutdown_grace_period=2.0,
        )

        async with anyio.create_task_group() as tg:

            async def run_response():
                with collapse_excgroups():
                    await response({}, mock_receive, mock_send)

            tg.start_soon(run_response)
            await anyio.sleep(0.3)

            # Trigger shutdown
            AppStatus.should_exit = True
            await anyio.sleep(WATCHER_DETECT_TIMEOUT)

            assert shutdown_event.is_set(), (
                "Library must set shutdown_event on shutdown"
            )
            tg.cancel_scope.cancel()

    @pytest.mark.asyncio
    async def test_shutdownGracePeriod_whenGeneratorExitsInTime_thenCleanShutdown(self):
        """Generator that sees shutdown_event and exits within grace period
        should complete without CancelledError."""
        shutdown_event = anyio.Event()
        farewell_sent = False
        was_cancelled = False

        async def gen():
            nonlocal farewell_sent, was_cancelled
            try:
                while not shutdown_event.is_set():
                    yield {"data": "tick"}
                    with anyio.move_on_after(0.1):
                        await shutdown_event.wait()
                # Farewell event after seeing shutdown
                yield {"event": "shutdown", "data": "bye"}
                farewell_sent = True
            except anyio.get_cancelled_exc_class():
                was_cancelled = True
                raise

        collected_bodies = []

        async def mock_send(*args, **kwargs):
            msg = args[0] if args else {}
            if isinstance(msg, dict) and msg.get("type") == "http.response.body":
                body = msg.get("body", b"")
                if body:
                    collected_bodies.append(body)

        async def mock_receive():
            await anyio.sleep(100)
            return {"type": "http.disconnect"}

        response = EventSourceResponse(
            gen(),
            ping=0,
            shutdown_event=shutdown_event,
            shutdown_grace_period=3.0,
        )

        async with anyio.create_task_group() as tg:

            async def run_response():
                with collapse_excgroups():
                    await response({}, mock_receive, mock_send)

            tg.start_soon(run_response)
            await anyio.sleep(0.3)

            # Trigger shutdown
            AppStatus.should_exit = True

            # Wait for response to finish (watcher + generator cleanup)
            await anyio.sleep(WATCHER_DETECT_TIMEOUT + 0.5)
            tg.cancel_scope.cancel()

        assert farewell_sent, "Generator should have sent farewell event"
        assert not was_cancelled, "Generator should NOT have been force-cancelled"
        # Verify farewell bytes appeared in output
        combined = b"".join(collected_bodies)
        assert b"event: shutdown" in combined, "Farewell event must reach the client"
        assert b"data: bye" in combined

    @pytest.mark.asyncio
    async def test_shutdownGracePeriod_whenGeneratorIgnoresEvent_thenForceCancelAfterTimeout(
        self,
    ):
        """Generator that ignores shutdown_event must be force-cancelled
        after grace period expires."""
        shutdown_event = anyio.Event()
        was_cancelled = False

        async def gen():
            nonlocal was_cancelled
            try:
                while True:  # deliberately ignores shutdown_event
                    yield {"data": "tick"}
                    await anyio.sleep(0.1)
            except anyio.get_cancelled_exc_class():
                was_cancelled = True
                raise

        async def mock_send(*args, **kwargs):
            pass

        async def mock_receive():
            await anyio.sleep(100)
            return {"type": "http.disconnect"}

        grace_period = 0.5
        response = EventSourceResponse(
            gen(),
            ping=0,
            shutdown_event=shutdown_event,
            shutdown_grace_period=grace_period,
        )

        with collapse_excgroups():
            async with anyio.create_task_group() as tg:

                async def run_response():
                    with collapse_excgroups():
                        await response({}, mock_receive, mock_send)

                tg.start_soon(run_response)
                await anyio.sleep(0.3)

                # Trigger shutdown
                AppStatus.should_exit = True

                # Wait for watcher + grace period + margin
                await anyio.sleep(WATCHER_DETECT_TIMEOUT + grace_period + 0.5)
                # If force-cancel worked, run_response should have finished
                # and we should be able to cancel the outer group cleanly
                tg.cancel_scope.cancel()

        assert shutdown_event.is_set(), "Event must be set even if generator ignores it"
        assert was_cancelled, "Generator must be force-cancelled after grace period"

    @pytest.mark.asyncio
    async def test_noShutdownEvent_whenShutdownDetected_thenImmediateCancel(self):
        """Without shutdown_event, behavior is identical to pre-#167: immediate cancel."""
        was_cancelled = False

        async def gen():
            nonlocal was_cancelled
            try:
                while True:
                    yield {"data": "tick"}
                    await anyio.sleep(0.1)
            except anyio.get_cancelled_exc_class():
                was_cancelled = True
                raise

        async def mock_send(*args, **kwargs):
            pass

        async def mock_receive():
            await anyio.sleep(100)
            return {"type": "http.disconnect"}

        # No shutdown_event — default behavior
        response = EventSourceResponse(gen(), ping=0)

        with collapse_excgroups():
            async with anyio.create_task_group() as tg:

                async def run_response():
                    with collapse_excgroups():
                        await response({}, mock_receive, mock_send)

                tg.start_soon(run_response)
                await anyio.sleep(0.3)

                AppStatus.should_exit = True
                # Should cancel quickly (watcher poll + small margin, no grace period)
                await anyio.sleep(WATCHER_DETECT_TIMEOUT + 0.3)
                tg.cancel_scope.cancel()

        assert was_cancelled, (
            "Generator must be cancelled immediately (no grace period)"
        )

    @pytest.mark.asyncio
    async def test_shutdownEvent_whenGracePeriodZero_thenEventSetButImmediateCancel(
        self,
    ):
        """With shutdown_event but grace_period=0, event IS set but cancel is immediate."""
        shutdown_event = anyio.Event()
        was_cancelled = False

        async def gen():
            nonlocal was_cancelled
            try:
                while True:
                    yield {"data": "tick"}
                    await anyio.sleep(0.1)
            except anyio.get_cancelled_exc_class():
                was_cancelled = True
                raise

        async def mock_send(*args, **kwargs):
            pass

        async def mock_receive():
            await anyio.sleep(100)
            return {"type": "http.disconnect"}

        response = EventSourceResponse(
            gen(),
            ping=0,
            shutdown_event=shutdown_event,
            shutdown_grace_period=0,
        )

        with collapse_excgroups():
            async with anyio.create_task_group() as tg:

                async def run_response():
                    with collapse_excgroups():
                        await response({}, mock_receive, mock_send)

                tg.start_soon(run_response)
                await anyio.sleep(0.3)

                AppStatus.should_exit = True
                await anyio.sleep(WATCHER_DETECT_TIMEOUT + 0.3)
                tg.cancel_scope.cancel()

        assert shutdown_event.is_set(), "Event must be set even with grace_period=0"
        assert was_cancelled, "Cancel must be immediate with grace_period=0"

    def test_shutdownGracePeriod_whenNegative_thenRaisesValueError(self):
        """Negative shutdown_grace_period must be rejected."""
        with pytest.raises(ValueError, match="shutdown_grace_period must be >= 0"):
            EventSourceResponse(iter([]), shutdown_grace_period=-1)
