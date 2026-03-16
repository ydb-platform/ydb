import asyncio
import logging
import math
from functools import partial

import anyio
import anyio.lowlevel
import pytest
from starlette.background import BackgroundTask
from starlette.testclient import TestClient

from sse_starlette.sse import EventSourceResponse
from sse_starlette.sse import SendTimeoutError
from tests.anyio_compat import collapse_excgroups

_log = logging.getLogger(__name__)


# Test fixtures and helpers
@pytest.fixture
def mock_generator():
    async def numbers(minimum, maximum):
        for i in range(minimum, maximum + 1):
            await asyncio.sleep(0.1)
            yield i

    return numbers


@pytest.fixture
def mock_memory_channels():
    async def setup():
        send_chan, recv_chan = anyio.create_memory_object_stream(math.inf)
        return send_chan, recv_chan

    return setup


class TestEventSourceResponse:
    @pytest.mark.parametrize(
        "input_type,separator,expected_output",
        [
            ("integer", "\r\n", b"data: 1\r\n\r\n"),
            ("dict_simple", "\r\n", b"data: 1\r\n\r\n"),
            ("dict_with_event", "\r\n", b"event: message\r\ndata: 1\r\n\r\n"),
            ("dict_with_event", "\r", b"event: message\rdata: 1\r\r"),
        ],
    )
    async def test_response_send_whenValidInput_thenGeneratesExpectedOutput(
        self,
        mock_generator,
        input_type,
        separator,
        expected_output,
    ):
        # Arrange
        async def app(scope, receive, send):
            async def format_output(value):
                if input_type == "integer":
                    return value
                elif input_type == "dict_simple":
                    return dict(data=value)
                else:
                    return dict(data=value, event="message")

            async def generate():
                generator = mock_generator(1, 5)
                async for value in generator:
                    yield await format_output(value)

            response = EventSourceResponse(generate(), ping=0.2, sep=separator)
            await response(scope, receive, send)

        # Act
        client = TestClient(app)
        response = client.get("/")

        # Assert
        assert expected_output in response.content
        assert response.content.decode().count("ping") == 2

    @pytest.mark.parametrize(
        "producer_output,expected_sse_response",
        [
            # Test raw integers being converted to SSE format
            ("raw_integer", b"data: 1\r\n\r\n"),
            # Test dict with just data field
            ("simple_dict", b"data: 1\r\n\r\n"),
            # Test dict with both event and data fields
            ("event_dict", b"event: message\r\ndata: 1\r\n\r\n"),
        ],
    )
    def test_eventSourceResponse_whenUsingMemoryChannel_thenHandlesAsyncQueueCorrectly(
        self, producer_output, expected_sse_response
    ):
        """Tests that EventSourceResponse can properly consume data from an async memory channel.

        This test verifies the producer-consumer pattern where:
        1. Producer (stream_numbers) puts data into a memory channel
        2. Consumer (EventSourceResponse) reads from that channel and formats as SSE

        This differs from direct generator tests by:
        - Using separate producer/consumer components
        - Testing async queue-based communication
        - Verifying SSE works with buffered async data sources
        """

        # Arrange
        async def app(scope, receive, send):
            # Create bounded memory channel for producer-consumer communication
            send_chan, recv_chan = anyio.create_memory_object_stream(
                max_buffer_size=math.inf
            )

            # Producer function that writes to the channel
            async def stream_numbers(producer_channel, start, end):
                async with producer_channel:
                    for i in range(start, end + 1):
                        await anyio.sleep(0.1)  # Simulate async data production

                        # Format data based on test case
                        if producer_output == "raw_integer":
                            data = i
                        elif producer_output == "simple_dict":
                            data = dict(data=i)
                        else:  # event_dict
                            data = dict(data=i, event="message")

                        # Send to channel for consumption
                        await producer_channel.send(data)

            # Create SSE response that consumes from channel
            response = EventSourceResponse(
                recv_chan,  # Consumer reads from receive channel
                data_sender_callable=partial(
                    stream_numbers, send_chan, 1, 5
                ),  # Producer writes to send channel
                ping=0.2,
            )
            await response(scope, receive, send)

        # Act
        client = TestClient(app)
        response = client.get("/")

        # Assert
        assert response.content.decode().count("ping") == 2
        assert expected_sse_response in response.content

    @pytest.mark.anyio
    async def test_disconnect_whenClientDisconnects_thenHandlesGracefully(
        self, httpx_client, caplog
    ):
        # Arrange
        caplog.set_level(logging.DEBUG)

        # Act & Assert
        with pytest.raises(TimeoutError):
            with anyio.fail_after(1) as scope:
                try:
                    async with anyio.create_task_group() as tg:
                        # https://www.python-httpx.org/async/#streaming-responses
                        tg.start_soon(httpx_client.get, "/endless")
                finally:
                    assert scope.cancel_called is True
                    assert "chunk: b'data: 4\\r\\n\\r\\n'" in caplog.text
                    assert "Disconnected from client" in caplog.text

    @pytest.mark.anyio
    async def test_send_whenTimeoutOccurs_thenRaisesSendTimeoutError(self):
        # Arrange
        # Send timeout is set to 0.5s, but `send` will take 1s. Expect SendTimeoutError.
        cleanup_executed = False

        async def event_publisher():
            try:
                yield {"event": "test", "data": "data"}
                pytest.fail("Should not reach this point")
            finally:
                nonlocal cleanup_executed
                cleanup_executed = True

        async def mock_send(*args, **kwargs):
            await anyio.sleep(1.0)

        async def mock_receive():
            await anyio.lowlevel.checkpoint()
            return {"type": "message"}

        response = EventSourceResponse(event_publisher(), send_timeout=0.5)

        # Act & Assert
        with pytest.raises(SendTimeoutError):
            with collapse_excgroups():
                await response({}, mock_receive, mock_send)

        assert cleanup_executed, "Cleanup should be executed on timeout"

    def test_headers_whenCustomHeadersProvided_thenMergesCorrectly(self):
        # Arrange
        custom_headers = {
            "cache-control": "no-cache",
            "x-accel-buffering": "yes",  # Should not override
            "connection": "close",  # Should not override
            "x-custom-header": "custom-value",
        }

        # Act
        response = EventSourceResponse(range(1, 5), headers=custom_headers, ping=0.2)
        headers = dict((h.decode(), v.decode()) for h, v in response.raw_headers)

        # Assert
        assert headers["cache-control"] == "no-cache"
        assert headers["x-accel-buffering"] == "no"  # Should keep default
        assert headers["connection"] == "keep-alive"  # Should keep default
        assert headers["x-custom-header"] == "custom-value"
        assert headers["content-type"] == "text/event-stream; charset=utf-8"

    def test_headers_whenCreated_thenHasCorrectCharset(self, mock_generator):
        # Arrange
        generator = mock_generator(1, 5)

        # Act
        response = EventSourceResponse(generator, ping=0.2)
        content_type_headers = [
            (h.decode(), v.decode())
            for h, v in response.raw_headers
            if h.decode() == "content-type"
        ]

        # Assert
        assert len(content_type_headers) == 1
        header_name, header_value = content_type_headers[0]
        assert header_value == "text/event-stream; charset=utf-8"

    @pytest.mark.anyio
    async def test_ping_whenConcurrentWithEvents_thenRespectsLocking(self):
        # Sequencing here is as follows to reproduce race condition:
        # t=0.5s - event_publisher sends the first response item,
        #          claiming the lock and going to sleep for 1 second so until t=1.5s.
        # t=1.0s - ping task wakes up and tries to call send while we know
        #          that event_publisher is still blocked inside it and holding the lock
        # Arrange
        lock = anyio.Lock()

        async def event_publisher():
            for i in range(2):
                await anyio.sleep(0.5)
                yield i

        async def send(*args, **kwargs):
            # Raises WouldBlock if called while someone else already holds the lock
            lock.acquire_nowait()
            await anyio.sleep(1.0)
            lock.release()

        async def receive():
            await anyio.lowlevel.checkpoint()
            return {"type": "message"}

        response = EventSourceResponse(event_publisher(), ping=1)

        # Act & Assert
        with pytest.raises(anyio.WouldBlock):
            with collapse_excgroups():
                await response({}, receive, send)

    def test_pingInterval_whenCreated_thenUsesDefaultValue(self):
        # Arrange & Act
        response = EventSourceResponse(0)

        # Assert
        assert response.ping_interval == response.DEFAULT_PING_INTERVAL

    def test_pingInterval_whenValidValueSet_thenUpdatesInterval(self):
        # Arrange
        response = EventSourceResponse(0)
        new_interval = 25

        # Act
        response.ping_interval = new_interval

        # Assert
        assert response.ping_interval == new_interval

    def test_pingInterval_whenStringProvided_thenRaisesTypeError(self):
        # Arrange
        response = EventSourceResponse(0)
        invalid_interval = "ten"

        # Act & Assert
        with pytest.raises(TypeError, match="ping interval must be int"):
            response.ping_interval = invalid_interval

    def test_pingInterval_whenNegativeValue_thenRaisesValueError(self):
        # Arrange
        response = EventSourceResponse(0)
        negative_interval = -42

        # Act & Assert
        with pytest.raises(ValueError, match="ping interval must be greater than 0"):
            response.ping_interval = negative_interval

    def test_compression_whenEnabled_thenRaisesNotImplemented(self):
        # Arrange
        response = EventSourceResponse(range(1, 5))

        # Act & Assert
        with pytest.raises(NotImplementedError):
            response.enable_compression()

    @pytest.mark.anyio
    async def test_backgroundTask_whenProvided_thenExecutesAfterResponse(self):
        # Arrange
        task_executed = False

        async def background_task():
            nonlocal task_executed
            task_executed = True

        async def mock_send(*args, **kwargs):
            pass

        async def mock_receive():
            await anyio.lowlevel.checkpoint()
            return {"type": "http.disconnect"}

        response = EventSourceResponse([], background=BackgroundTask(background_task))

        # Act
        await response({}, mock_receive, mock_send)

        # Assert
        assert task_executed, "Background task should be executed"
