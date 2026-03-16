import asyncio
import logging
import os
import subprocess
import threading
import time
from functools import partial
from pathlib import Path

import httpcore
import httpx
import psutil
import pytest
import requests

_log = logging.getLogger(__name__)

ROOT_PATH = Path(__file__).parent.parent.parent
URL = "http://localhost"
PORT = 8001
LOG_LEVEL = "info"
SERVER_READY_TIMEOUT = 5  # Max seconds to wait for the server to be ready

server_process = None  # Global variable to hold the server process
server_ready_event = threading.Event()  # Event to signal when the server is ready


def check_server_is_ready(port: int):
    """Check if the server is ready by making a GET request to the URL."""
    for _ in range(SERVER_READY_TIMEOUT):
        try:
            response = requests.get(f"{URL}:{port}/health")
            if response.status_code == 200:
                _log.info("Server is ready.")
                return True
        except requests.ConnectionError:
            _log.debug("Server not ready yet...")
        time.sleep(1)
    return False


def get_available_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def run_server(server_command: str, port: int):
    global server_process
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT_PATH)  # Set PYTHONPATH to include the project root
    server_process = subprocess.Popen(
        server_command, shell=True, cwd=ROOT_PATH, env=env
    )
    if check_server_is_ready(port):
        server_ready_event.set()  # Signal that the server is ready
    else:
        _log.debug("Server did not become ready in time, terminating server process.")
        terminate_server()
        server_ready_event.set()  # allow pytest to fail after passing the Event barrier
        raise Exception("Server did not become ready in time.")


def terminate_server():
    if server_process:
        try:
            _log.debug("Attempting to terminate the server process.")
            assert isinstance(server_process, subprocess.Popen)  # please mypy
            parent = psutil.Process(server_process.pid)
            for child in parent.children(recursive=True):
                child.terminate()
            parent.terminate()
            try:
                # fix uvicorn breaking change: https://github.com/encode/uvicorn/compare/0.28.1...0.29.0
                parent.wait(timeout=1)
            except psutil.TimeoutExpired:
                _log.info(
                    "Server process did not terminate after 1 second, killing it."
                )
                parent.kill()
                parent.wait()
            server_process.wait()
            _log.debug("Server process terminated.")
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            _log.warning("Server process could not be terminated.")
        except subprocess.TimeoutExpired:
            _log.warning(
                "Timeout waiting for server process to terminate. Forcing kill."
            )
            server_process.kill()  # Force kill if not terminated after timeout
            server_process.wait()  # Wait for the kill to take effect
        finally:
            server_ready_event.clear()


async def make_arequest(url, expected_lines=2):
    """Simulate Client:
    Stream the SSE endpoint, and count the number of lines received.
    """
    _log.info(f"{threading.current_thread().ident}: Starting making requests to {url=}")
    i = 0
    async with httpx.AsyncClient() as client:
        try:
            # stream client for line-by-line output
            async with client.stream("GET", url) as response:
                async for line in response.aiter_lines():
                    print(
                        f"{threading.current_thread().ident}: Streaming response {i=}, {line=}"
                    )
                    i += 1
        except httpx.RemoteProtocolError as e:
            _log.error(e)
        except httpcore.RemoteProtocolError as e:
            _log.error(e)
        finally:
            assert i == expected_lines, (
                f"Expected {expected_lines} lines"
            )  # not part of test runner, failure is not reported

        _log.info(
            f"{threading.current_thread().ident}: Stopping making requests to {url=}, finished after {i=} responses."
        )
        # expected output lines:
        # i=0, line='data: 1'
        # i=1, line=''
        # ...
        assert i == expected_lines, (
            f"Expected {expected_lines} lines"
        )  # not part of test runner, failure is not reported


@pytest.mark.skipif(os.name == "nt", reason="Skip on Windows")
@pytest.mark.experimentation
@pytest.mark.parametrize(
    ("server_command", "expected_lines"),
    [
        (
            "uvicorn tests.integration.main_endless:app --host localhost --port {port} --log-level {log_level}",
            14,
        ),
        (
            "uvicorn tests.integration.main_endless_conditional:app --host localhost --port {port} --log-level {log_level}",
            2,
        ),
    ],
)
def test_stop_server_with_many_consumers(caplog, server_command, expected_lines):
    # Given
    caplog.set_level(logging.DEBUG)
    N_CONSUMER = 3
    port = get_available_port()

    # Start server
    server_command = server_command.format(port=port, log_level=LOG_LEVEL)
    _log.info(f"Starting server with command: {server_command}")
    server_to_run = partial(run_server, server_command, port)
    server_thread = threading.Thread(target=server_to_run)
    server_thread.start()

    server_ready_event.wait()  # Wait for the server to become ready
    if server_process is None or server_process.poll() is not None:
        pytest.fail("Server did not start.")

    # Initialize threads
    threads = []
    for _ in range(N_CONSUMER):
        thread = threading.Thread(
            target=lambda: asyncio.run(
                make_arequest(f"{URL}:{port}/endless", expected_lines=expected_lines)
            )
        )
        threads.append(thread)

    for thread in threads:
        thread.start()

    # Wait and then stop server
    time.sleep(1)  # Simulate some operation time

    # When: the server is stopped unexpectedly
    terminate_server()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    server_thread.join()  # Ensure server thread is cleaned up

    # Then: Consumers report errors
    time.sleep(0.5)
    errors = [r.message for r in caplog.records if r.levelname == "ERROR"]
    assert len(errors) == N_CONSUMER, f"Expected {N_CONSUMER} errors, got {len(errors)}"
    # consumers: 'peer closed connection without sending complete message body (incomplete chunked read)'
    assert (
        "peer closed connection without sending complete message body (incomplete chunked read)"
        in errors
    )
    time.sleep(0.2)
