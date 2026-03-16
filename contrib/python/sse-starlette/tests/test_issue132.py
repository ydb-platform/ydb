"""
Tests for Issue #132: Graceful shutdown when monkey-patch fails.

Issue #132: The monkey-patch of Server.handle_exit doesn't work with
`uvicorn app:app` because uvicorn registers signal handlers BEFORE
importing the app (uvicorn 0.29+).

The fix: _shutdown_watcher() now checks both AppStatus.should_exit
(monkey-patch worked) and uvicorn's Server.should_exit via signal
handler introspection (monkey-patch failed).

Manual Test:
  # Terminal 1
  uvicorn examples.example:app

  # Terminal 2
  curl http://localhost:8000/sse

  # Terminal 1
  Ctrl+C  # Should exit gracefully now
"""

import signal
from unittest.mock import MagicMock, patch

import anyio
import pytest

from sse_starlette.sse import (
    AppStatus,
    _get_shutdown_state,
    _get_uvicorn_server,
    _shutdown_watcher,
)


class TestUvicornServerIntrospection:
    """Test _get_uvicorn_server() signal handler introspection."""

    def test_returns_server_when_handler_is_bound_method(self):
        """Should extract Server from bound method handler."""
        mock_server = MagicMock()
        mock_server.should_exit = False

        with patch("sse_starlette.sse.signal.getsignal") as mock_getsignal:
            mock_handler = MagicMock()
            mock_handler.__self__ = mock_server
            mock_getsignal.return_value = mock_handler

            result = _get_uvicorn_server()
            assert result is mock_server

    def test_returns_none_when_handler_is_sig_dfl(self):
        """Should return None for SIG_DFL (default handler)."""
        with patch("sse_starlette.sse.signal.getsignal") as mock_getsignal:
            mock_getsignal.return_value = signal.SIG_DFL

            result = _get_uvicorn_server()
            assert result is None

    def test_returns_none_when_handler_is_sig_ign(self):
        """Should return None for SIG_IGN (ignored signal)."""
        with patch("sse_starlette.sse.signal.getsignal") as mock_getsignal:
            mock_getsignal.return_value = signal.SIG_IGN

            result = _get_uvicorn_server()
            assert result is None

    def test_returns_none_when_handler_lacks_self(self):
        """Should return None when handler is a function (not bound method)."""
        with patch("sse_starlette.sse.signal.getsignal") as mock_getsignal:
            mock_getsignal.return_value = lambda sig, frame: None

            result = _get_uvicorn_server()
            assert result is None

    def test_returns_none_when_self_lacks_should_exit(self):
        """Should return None when __self__ doesn't have should_exit attribute."""
        mock_obj = MagicMock(spec=[])  # No attributes

        with patch("sse_starlette.sse.signal.getsignal") as mock_getsignal:
            mock_handler = MagicMock()
            mock_handler.__self__ = mock_obj
            mock_getsignal.return_value = mock_handler

            result = _get_uvicorn_server()
            assert result is None

    def test_returns_none_on_exception(self):
        """Should return None if introspection fails with any exception."""
        with patch(
            "sse_starlette.sse.signal.getsignal", side_effect=Exception("test error")
        ):
            result = _get_uvicorn_server()
            assert result is None


class TestShutdownWatcherDualSource:
    """Test _shutdown_watcher() detects shutdown from both sources."""

    @pytest.mark.asyncio
    async def test_detects_appstatus_should_exit(self):
        """Should detect when AppStatus.should_exit is set (monkey-patch worked)."""
        state = _get_shutdown_state()
        event = anyio.Event()
        state.events.add(event)

        async def set_should_exit():
            await anyio.sleep(0.1)
            AppStatus.should_exit = True

        async with anyio.create_task_group() as tg:
            tg.start_soon(_shutdown_watcher)
            tg.start_soon(set_should_exit)

            # Wait for event to be signaled (with timeout)
            with anyio.fail_after(2):
                await event.wait()

        assert AppStatus.should_exit is True

    @pytest.mark.asyncio
    async def test_detects_uvicorn_server_should_exit(self):
        """Should detect when uvicorn Server.should_exit is set (Issue #132)."""
        # Create a mock uvicorn server
        mock_server = MagicMock()
        mock_server.should_exit = False

        state = _get_shutdown_state()
        event = anyio.Event()
        state.events.add(event)

        async def set_server_should_exit():
            await anyio.sleep(0.1)
            mock_server.should_exit = True

        # Patch _get_uvicorn_server to return our mock
        with patch("sse_starlette.sse._get_uvicorn_server", return_value=mock_server):
            async with anyio.create_task_group() as tg:
                tg.start_soon(_shutdown_watcher)
                tg.start_soon(set_server_should_exit)

                # Wait for event to be signaled (with timeout)
                with anyio.fail_after(2):
                    await event.wait()

        # AppStatus.should_exit should be synced
        assert AppStatus.should_exit is True

    @pytest.mark.asyncio
    async def test_fallback_when_no_uvicorn_server(self):
        """Should work when _get_uvicorn_server returns None."""
        state = _get_shutdown_state()
        event = anyio.Event()
        state.events.add(event)

        async def set_should_exit():
            await anyio.sleep(0.1)
            AppStatus.should_exit = True

        # Ensure no uvicorn server is found
        with patch("sse_starlette.sse._get_uvicorn_server", return_value=None):
            async with anyio.create_task_group() as tg:
                tg.start_soon(_shutdown_watcher)
                tg.start_soon(set_should_exit)

                with anyio.fail_after(2):
                    await event.wait()

        assert event.is_set()
