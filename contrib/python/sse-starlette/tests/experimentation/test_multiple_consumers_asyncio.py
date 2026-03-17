import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, List
import pytest
import httpx
import uvicorn
from async_timeout import timeout
import portend
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class ServerManager:
    """Manages the lifecycle of a test server instance"""

    def __init__(self, app_path: str, host: str = "localhost", port: int = None):
        self.app_path = app_path
        self.host = host
        self.port = port or portend.find_available_local_port()
        self.server = None
        self._startup_complete = asyncio.Event()
        self._shutdown_complete = asyncio.Event()

    async def startup(self) -> None:
        """Start the server in a separate task"""
        config = uvicorn.Config(
            app=self.app_path,
            host=self.host,
            port=self.port,
            log_level="error",
            loop="asyncio",
        )

        self.server = uvicorn.Server(config=config)

        # Store the original startup handler
        original_startup = self.server.startup

        # Create a wrapper that preserves the original signature
        async def startup_wrapper(*args, **kwargs):
            await original_startup(*args, **kwargs)
            self._startup_complete.set()

        self.server.startup = startup_wrapper

        # Start the server
        self._server_task = asyncio.create_task(self.server.serve())

        try:
            async with timeout(10):  # 10 second timeout for startup
                await self._startup_complete.wait()

                # Additional health check
                retry_count = 0
                while retry_count < 5:  # Try 5 times with exponential backoff
                    if await self.health_check():
                        break
                    retry_count += 1
                    await asyncio.sleep(0.2 * (2**retry_count))
                else:
                    raise RuntimeError("Server health check failed after retries")

        except Exception as e:
            # If startup fails, ensure we clean up
            await self.shutdown()
            if isinstance(e, asyncio.TimeoutError):
                raise RuntimeError("Server failed to start within timeout") from e
            raise

    async def shutdown(self) -> None:
        """Shutdown the server gracefully"""
        if self.server and not self._shutdown_complete.is_set():
            try:
                self.server.should_exit = True
                if hasattr(self, "_server_task"):
                    try:
                        async with timeout(5):  # 5 second timeout for shutdown
                            await self._server_task
                    except asyncio.TimeoutError:
                        # Force cancel if graceful shutdown fails
                        self._server_task.cancel()
                        try:
                            await self._server_task
                        except asyncio.CancelledError:
                            pass
            finally:
                self._shutdown_complete.set()
                self.server = None
                self._startup_complete.clear()

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

    async def health_check(self) -> bool:
        """Check if server is responding"""
        try:
            async with httpx.AsyncClient() as client:
                async with timeout(1):  # 1 second timeout for health check
                    response = await client.get(f"{self.url}/health")
                    return response.status_code == 200
        except Exception:
            return False


@asynccontextmanager
async def server_context(app_path: str) -> AsyncIterator[ServerManager]:
    """Context manager for server lifecycle"""
    server = ServerManager(app_path)
    try:
        await server.startup()
        yield server
    finally:
        await server.shutdown()


class SSEClient:
    """Client for consuming SSE streams"""

    def __init__(self, url: str, expected_lines: int):
        self.url = url
        self.expected_lines = expected_lines
        self.received_lines = 0
        self.errors: List[Exception] = []

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    async def connect_and_consume(self) -> None:
        """Connect to SSE stream and consume messages"""
        try:
            async with httpx.AsyncClient() as client:
                async with timeout(20):  # 20 second timeout for stream consumption
                    async with client.stream("GET", self.url) as response:
                        async for line in response.aiter_lines():
                            if line.strip():  # Only count non-empty lines
                                self.received_lines += 1
                                if self.received_lines >= self.expected_lines:
                                    break
        except Exception as e:
            self.errors.append(e)
            raise


@pytest.mark.asyncio
@pytest.mark.experimentation
@pytest.mark.parametrize(
    "app_path,expected_lines",
    [
        ("tests.integration.main_endless:app", 14),
        ("tests.integration.main_endless_conditional:app", 2),
    ],
)
async def test_sse_multiple_consumers(
    app_path: str, expected_lines: int, num_consumers: int = 3
):
    """Test multiple consumers connecting to SSE endpoint"""

    async with server_context(app_path) as server:
        # Create and start consumers
        clients = [
            SSEClient(f"{server.url}/endless", expected_lines)
            for _ in range(num_consumers)
        ]

        # Run consumers concurrently with timeout
        async with timeout(30):  # 30 second timeout for entire test
            try:
                # Create tasks for all consumers
                consumer_tasks = [
                    asyncio.create_task(client.connect_and_consume())
                    for client in clients
                ]

                # Wait for all consumers or first error
                done, pending = await asyncio.wait(
                    consumer_tasks, return_when=asyncio.FIRST_EXCEPTION
                )

                # Cancel any pending tasks
                for task in pending:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

                # Check results and gather errors
                errors = []
                for task in done:
                    try:
                        await task
                    except Exception as e:
                        errors.append(e)

                # Verify expectations
                for i, client in enumerate(clients):
                    assert client.received_lines == expected_lines, (
                        f"Client {i} received {client.received_lines} lines, expected {expected_lines}"
                    )

                assert not errors, f"Consumers encountered errors: {errors}"

            except asyncio.TimeoutError:
                raise RuntimeError("Test timed out waiting for consumers")
            finally:
                # Ensure all tasks are properly cleaned up
                for task in consumer_tasks:
                    if not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
