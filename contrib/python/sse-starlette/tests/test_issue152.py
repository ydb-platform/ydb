"""
Regression tests for Issue #152: Watcher Task Leak.

Bug: ContextVar creates isolated state per async context, so each SSE
connection (running in fresh ASGI context) spawns its own _shutdown_watcher.
N connections = N watchers = CPU exhaustion over time.

Fix: Use threading.local() for per-thread state, so all connections in the
same thread share one watcher.

See: thoughts/issue152-watcher-leak-analysis.md
"""

import asyncio

import anyio
import pytest

# Watcher polls every 0.5s, so we need >0.5s for it to detect shutdown.
# Add margin for test system load variance.
WATCHER_POLL_INTERVAL = 0.5
WATCHER_DETECT_TIMEOUT = WATCHER_POLL_INTERVAL + 0.2  # 0.7s


class TestIssue152WatcherLeak:
    """Regression tests for Issue #152: only one watcher per thread."""

    @pytest.mark.asyncio
    async def test_single_watcher_per_thread(self):
        """
        Issue #152 regression: Only one watcher should be started per thread.

        In real ASGI apps:
        - Each request runs in the SAME thread (threading.local is shared)
        - Each request runs in a NEW async context (ContextVar is isolated)

        With threading.local (fixed): state persists → 1 watcher per thread
        With ContextVar (bug): state isolated per context → N watchers
        """
        import sse_starlette.sse as sse_module
        from sse_starlette.sse import _ensure_watcher_started_on_this_loop

        watcher_starts = []
        original_watcher = sse_module._shutdown_watcher

        async def tracking_watcher():
            """Track when watcher is started, exit immediately."""
            watcher_starts.append(True)
            # Don't run the actual polling loop

        sse_module._shutdown_watcher = tracking_watcher

        try:
            num_connections = 10

            # Simulate 10 SSE connections in the same thread
            # With threading.local: state persists, so only 1 watcher
            # DON'T reset between calls - this simulates real ASGI behavior
            for _ in range(num_connections):
                _ensure_watcher_started_on_this_loop()

            await asyncio.sleep(0.1)

            assert len(watcher_starts) == 1, (
                f"Issue #152: {len(watcher_starts)} watchers started for "
                f"{num_connections} connections. Expected 1 shared watcher."
            )
        finally:
            sse_module._shutdown_watcher = original_watcher

    @pytest.mark.asyncio
    async def test_watcher_broadcasts_to_all_events(self):
        """Verify that one watcher can signal multiple events.

        This test also serves as coverage for test_issue132.py's broadcast test.
        """
        import sse_starlette.sse as sse_module
        from sse_starlette.sse import (
            _ensure_watcher_started_on_this_loop,
            _get_shutdown_state,
        )

        # Start watcher (state reset by conftest fixture)
        _ensure_watcher_started_on_this_loop()

        # Create multiple events (simulating multiple SSE connections)
        state = _get_shutdown_state()
        events = [anyio.Event() for _ in range(5)]
        for event in events:
            state.events.add(event)

        try:
            # Trigger shutdown
            sse_module.AppStatus.should_exit = True

            # Wait for watcher to broadcast
            await asyncio.sleep(WATCHER_DETECT_TIMEOUT)

            # All events should be set
            for i, event in enumerate(events):
                assert event.is_set(), f"Event {i} was not signaled by watcher"
        finally:
            for event in events:
                state.events.discard(event)

    @pytest.mark.asyncio
    async def test_rapid_ensure_calls_spawn_single_watcher(self):
        """Multiple rapid calls to _ensure_watcher_started don't spawn multiple watchers."""
        import sse_starlette.sse as sse_module
        from sse_starlette.sse import _ensure_watcher_started_on_this_loop

        watcher_starts = []
        original_watcher = sse_module._shutdown_watcher

        async def tracking_watcher():
            watcher_starts.append(True)

        sse_module._shutdown_watcher = tracking_watcher

        try:
            # Rapid-fire 100 calls
            for _ in range(100):
                _ensure_watcher_started_on_this_loop()

            await asyncio.sleep(0.1)

            assert len(watcher_starts) == 1, (
                f"Rapid calls spawned {len(watcher_starts)} watchers, expected 1"
            )
        finally:
            sse_module._shutdown_watcher = original_watcher

    @pytest.mark.asyncio
    async def test_event_removal_during_broadcast_is_safe(self):
        """Removing an event from the set during broadcast doesn't crash.

        The watcher iterates over list(state.events) to avoid mutation issues.
        """
        import sse_starlette.sse as sse_module
        from sse_starlette.sse import (
            _ensure_watcher_started_on_this_loop,
            _get_shutdown_state,
        )

        _ensure_watcher_started_on_this_loop()

        state = _get_shutdown_state()
        events = [anyio.Event() for _ in range(3)]
        for event in events:
            state.events.add(event)

        # Remove one event before shutdown triggers broadcast
        state.events.discard(events[1])

        # Trigger shutdown - should not crash even though set was modified
        sse_module.AppStatus.should_exit = True
        await asyncio.sleep(WATCHER_DETECT_TIMEOUT)

        # Remaining events should be signaled
        assert events[0].is_set(), "Event 0 should be set"
        assert events[2].is_set(), "Event 2 should be set"

    @pytest.mark.asyncio
    async def test_watcher_cleanup_allows_restart(self):
        """After watcher exits, a new connection can start a new watcher.

        The watcher's finally block resets watcher_started=False.
        """
        import sse_starlette.sse as sse_module
        from sse_starlette.sse import (
            _ensure_watcher_started_on_this_loop,
            _get_shutdown_state,
        )

        watcher_starts = []
        original_watcher = sse_module._shutdown_watcher

        async def tracking_watcher():
            watcher_starts.append(True)
            state = _get_shutdown_state()
            try:
                while not sse_module.AppStatus.should_exit:
                    await asyncio.sleep(0.1)
            finally:
                state.watcher_started = False

        sse_module._shutdown_watcher = tracking_watcher

        try:
            # First watcher
            _ensure_watcher_started_on_this_loop()
            await asyncio.sleep(0.05)
            assert len(watcher_starts) == 1

            # Trigger shutdown to exit first watcher
            sse_module.AppStatus.should_exit = True
            await asyncio.sleep(0.15)  # Wait for watcher to exit

            # Reset for second round
            sse_module.AppStatus.should_exit = False

            # Second watcher should be allowed to start
            _ensure_watcher_started_on_this_loop()
            await asyncio.sleep(0.05)

            assert len(watcher_starts) == 2, (
                f"Expected watcher to restart after cleanup (2 sequential spawns), "
                f"got {len(watcher_starts)}"
            )
        finally:
            sse_module._shutdown_watcher = original_watcher
