import asyncio
import logging

import httpx
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

_log = logging.getLogger(__name__)


class SSEServerContainer(DockerContainer):
    def __init__(self, app_path: str):
        super().__init__("sse_starlette:latest")
        self.app_path = app_path

        # Specify platform for amd64 image on arm64 hosts
        self.with_kwargs(platform="linux/amd64")

        # Mount the current directory into the container
        import os

        project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        self.with_volume_mapping(host=project_root, container="/app")
        self.with_name("sse_starlette_test")
        self.with_command(
            f"uvicorn {self.app_path} --host 0.0.0.0 --port 8000 --log-level info"
        )

        # Expose the port
        self.with_exposed_ports(8000)

        # Wait for server to be ready (applied during start())
        self.waiting_for(LogMessageWaitStrategy("Application startup complete"))


async def consume_events(url: str, expected_lines: int = 2):
    """Simulate Client: Stream the SSE endpoint and count received lines."""
    i = 0
    async with httpx.AsyncClient() as client:
        try:
            async with client.stream("GET", url) as response:
                async for line in response.aiter_lines():
                    if line.strip():
                        _log.info(f"Received line: {line}")
                        i += 1
        except (httpx.RemoteProtocolError, httpx.ReadError) as e:
            _log.error(f"Error during streaming: {str(e)}")
            return i, str(e)
    return i, None


@pytest.mark.integration
@pytest.mark.parametrize(
    ("app_path", "expected_lines"),
    [
        ("tests.integration.main_endless:app", 14),
        ("tests.integration.main_endless_conditional:app", 2),
    ],
)
async def test_sse_server_termination(caplog, app_path, expected_lines):
    caplog.set_level(logging.DEBUG)
    N_CONSUMERS = 3

    # Start server in container
    container = SSEServerContainer(app_path)
    container.start()

    try:
        port = container.get_exposed_port(8000)
        url = f"http://localhost:{port}/endless"

        # Create background tasks for consumers
        tasks = [
            asyncio.create_task(consume_events(url, expected_lines))
            for _ in range(N_CONSUMERS)
        ]

        # Wait a bit then kill the server
        await asyncio.sleep(1)
        container.stop(force=True)

        # Now wait for all tasks to complete
        results = await asyncio.gather(*tasks)

        # Check error count: one connection error per client
        error_count = sum(1 for _, error in results if error is not None)
        assert error_count == N_CONSUMERS, (
            f"Expected {N_CONSUMERS} errors, got {error_count}"
        )

        # Verify error messages
        for _, error in results:
            assert (
                error
                and "peer closed connection without sending complete message body (incomplete chunked read)"
                in error.lower()
            ), "Expected peer closed connection error"

        # Check message counts
        message_counts = [count for count, _ in results]
        _log.info(f"Message counts received: {message_counts}")

        # Since we're killing the server early, we expect incomplete message counts
        assert all(count < expected_lines for count in message_counts), (
            f"Expected all counts to be less than {expected_lines}, got {message_counts}"
        )

    finally:
        # Cleanup container if it's still around
        try:
            container.stop(force=True)
        except Exception as e:
            _log.debug(f"Error during cleanup: {e}")
