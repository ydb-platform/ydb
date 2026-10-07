import asyncio
import logging
import queue
import threading
import zlib
from collections.abc import Callable, Iterable, Iterator
from typing import cast

import lz4.frame

from clickhouse_connect.driver.asyncqueue import EOF_SENTINEL, AsyncSyncQueue
from clickhouse_connect.driver.asyncqueue import Full as AsyncQueueFull
from clickhouse_connect.driver.compression import _zstd_decompressor, available_compression
from clickhouse_connect.driver.exceptions import Error, OperationalError
from clickhouse_connect.driver.transform import Transform
from clickhouse_connect.driver.types import ByteSource, Closable

logger: logging.Logger = logging.getLogger(__name__)

__all__ = [
    "StreamingResponseSource",
    "StreamingFileAdapter",
    "StreamingInsertSource",
    "ReadAheadSource",
    "QueuedStreamSource",
    "start_streaming_response",
]

if "br" in available_compression:
    import brotli
else:
    brotli = None


class StreamingResponseSource(Closable):
    """Streaming source that feeds chunks from async producer to sync consumer."""

    READ_BUFFER_SIZE = 1024 * 1024

    def __init__(self, response, encoding: str | None = None, exception_tag: str | None = None):
        self.response = response
        self.encoding = encoding
        self.exception_tag = exception_tag

        # maxsize=10 means max ~10 socket reads buffered
        self.queue: AsyncSyncQueue[bytes | Exception] = AsyncSyncQueue(maxsize=10)

        self._decompressor = None
        self._decompressor_initialized = False

        # Multiple accesses to .gen must return the same generator, not create new ones
        self._gen_cache: Iterator[bytes] | None = None

        self._loop: asyncio.AbstractEventLoop | None = None
        self._producer_task: asyncio.Task | None = None
        self._producer_started = threading.Event()
        self._producer_error: Exception | None = None
        self._producer_completed = False

    def _release_lease(self):
        release = getattr(self.response, "_lease_release", None)
        if release is not None:
            release()

    async def start_producer(self, loop: asyncio.AbstractEventLoop):
        """Start the async producer task.
        Must be called from the event loop thread before consuming.
        """

        async def producer():
            """Async producer: reads chunks from response, feeds queue."""
            data_sent = False
            try:
                while True:
                    chunk = await self.response.content.read(self.READ_BUFFER_SIZE)
                    if not chunk:
                        break
                    data_sent = True
                    await self.queue.async_q.put(chunk)

                await self.queue.async_q.put(EOF_SENTINEL)
                self._producer_completed = True

            except Exception as e:
                logger.error("Producer error while streaming response: %s", e, exc_info=True)
                if not data_sent:
                    e = OperationalError("Failed to read response data from server")
                self._producer_error = e

                try:
                    await self.queue.async_q.put(e)
                except RuntimeError:
                    pass

            finally:
                self.queue.shutdown()
                self._release_lease()

        self._loop = loop
        self._producer_task = loop.create_task(producer())
        self._producer_started.set()

    @property
    def gen(self) -> Iterator[bytes]:
        """Generator that yields decompressed chunks.

        CRITICAL: Returns cached generator to prevent multiple generators
        from competing to read from the same queue.
        """
        if self._gen_cache is not None:
            return self._gen_cache

        self._gen_cache = self._create_generator()
        return self._gen_cache

    def _create_generator(self) -> Iterator[bytes]:
        """Creates the actual generator function."""
        if not self._producer_started.wait(timeout=5.0):
            raise RuntimeError("Producer failed to start within timeout")

        if self.encoding and not self._decompressor_initialized:
            self._decompressor_initialized = True
            try:
                self._decompressor = self._create_decompressor(self.encoding)
            except Exception as e:
                logger.error("Failed to create decompressor for %s: %s", self.encoding, e)
                raise

        while True:
            chunk = self.queue.sync_q.get()

            if chunk is EOF_SENTINEL:
                if self._decompressor:
                    try:
                        if hasattr(self._decompressor, "flush"):
                            final = self._decompressor.flush()
                            if final:
                                yield final
                    except Exception as e:
                        logger.error("Error flushing decompressor: %s", e, exc_info=True)
                        raise
                break

            if isinstance(chunk, Exception):
                raise chunk

            if self._decompressor:
                try:
                    if hasattr(self._decompressor, "decompress"):
                        decompressed = self._decompressor.decompress(chunk)
                    else:
                        decompressed = self._decompressor.process(chunk)
                    if decompressed:
                        yield decompressed
                except Exception as e:
                    logger.error("Decompression error: %s", e, exc_info=True)
                    raise
            else:
                yield chunk

    @staticmethod
    def _create_decompressor(encoding: str):
        """Create incremental decompressor for encoding."""
        if encoding == "gzip":
            return zlib.decompressobj(16 + zlib.MAX_WBITS)

        if encoding == "deflate":
            return zlib.decompressobj()

        if encoding == "br":
            if brotli is not None:
                return brotli.Decompressor()
            raise ImportError("brotli compression requires 'brotli' package. Install with: pip install brotli")

        if encoding == "zstd":
            return _zstd_decompressor()

        if encoding == "lz4":
            return lz4.frame.LZ4FrameDecompressor()

        raise ValueError(f"Unsupported compression encoding: {encoding}")

    async def aclose(self):
        """Async cleanup resources"""
        self.queue.shutdown()

        if self._producer_task and not self._producer_task.done():
            self._producer_task.cancel()
            try:
                await self._producer_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        if self.response and not self.response.closed:
            if not self._producer_completed:
                self.response.close()
                await asyncio.sleep(0.05)
        self._release_lease()

    def close(self):
        """Synchronous cleanup resources"""
        self.queue.shutdown()

        def cleanup():
            if self._producer_task and not self._producer_task.done():
                self._producer_task.cancel()
            if self.response and not self.response.closed:
                if not self._producer_completed:
                    self.response.close()

        # Task cancellation and aiohttp response teardown must run on the event
        # loop thread. close() is normally called from an executor thread.
        if self._loop is not None and not self._loop.is_closed():
            try:
                self._loop.call_soon_threadsafe(cleanup)
            except RuntimeError:
                cleanup()
        else:
            cleanup()
        self._release_lease()


async def start_streaming_response(response, encoding: str | None = None, exception_tag: str | None = None) -> StreamingResponseSource:
    """Create a StreamingResponseSource and start its producer on the running loop.

    This is the async byte bridge: an async producer reads response chunks onto
    a bounded queue that a sync consumer (usually parsing in an executor) drains.
    """
    source = StreamingResponseSource(response, encoding=encoding, exception_tag=exception_tag)
    await source.start_producer(asyncio.get_running_loop())
    return source


class QueuedStreamSource(Closable):
    """A streaming source paired with the bounded queue that feeds parsed items
    from a sync producer (running in an executor) to an async consumer."""

    def __init__(self, source: StreamingResponseSource, maxsize: int = 10):
        self.source = source
        self.queue: AsyncSyncQueue = AsyncSyncQueue(maxsize=maxsize)

    def pump(self, produce: Callable[[], Iterable]) -> None:
        """Run produce() in an executor, feeding its items into the queue.
        Must be called from the event loop thread. A RuntimeError from a queue
        put means the queue was shut down and ends the producer quietly; any
        error from produce() itself is queued for the consumer to raise."""
        queue = self.queue

        def producer():
            try:
                for item in produce():
                    try:
                        queue.sync_q.put(item)
                    except RuntimeError:
                        return
                try:
                    queue.sync_q.put(EOF_SENTINEL)
                except RuntimeError:
                    return
            except Exception as e:
                try:
                    queue.sync_q.put(e)
                except Exception:
                    pass
            finally:
                queue.shutdown()

        asyncio.get_running_loop().run_in_executor(None, producer)

    async def items(self):
        """Async generator yielding queued items without blocking the event loop."""
        while True:
            item = await self.queue.async_q.get()
            if item is EOF_SENTINEL:
                break
            if isinstance(item, Exception):
                raise item
            yield item

    async def aclose(self):
        self.queue.shutdown()
        await self.source.aclose()

    def close(self):
        self.queue.shutdown()
        self.source.close()


class StreamingFileAdapter:
    """File-like adapter for PyArrow streaming."""

    def __init__(self, streaming_source):
        self.streaming_source = streaming_source
        self.gen = streaming_source.gen
        self.buffer = b""
        self.closed = False
        self.eof = False

    def read(self, size: int = -1) -> bytes:
        """Read up to size bytes from stream"""
        if self.closed or self.eof:
            return b""

        if size != -1 and len(self.buffer) >= size:
            result = self.buffer[:size]
            self.buffer = self.buffer[size:]
            return result

        chunks = [self.buffer] if self.buffer else []
        current_len = len(self.buffer)
        self.buffer = b""

        while (size == -1 or current_len < size) and not self.eof:
            try:
                chunk = next(self.gen)
                if chunk:
                    chunks.append(chunk)
                    current_len += len(chunk)
                else:
                    self.eof = True
                    break
            except StopIteration:
                self.eof = True
                break

        full_data = b"".join(chunks)

        if size == -1 or len(full_data) <= size:
            return full_data

        result = full_data[:size]
        self.buffer = full_data[size:]
        return result

    def close(self):
        self.closed = True


class StreamingInsertSource:
    """Streaming source for async inserts (reverse bridge)"""

    def __init__(self, transform: Transform, context, loop: asyncio.AbstractEventLoop, maxsize: int = 10):
        self.transform = transform
        self.context = context
        self.loop = loop
        self.queue: AsyncSyncQueue[bytes | bytearray | Exception] = AsyncSyncQueue(maxsize=maxsize)
        self._stop_event = threading.Event()
        self._producer_future = None
        self._started = False

    def start_producer(self):
        if self._started:
            raise RuntimeError("Producer already started")
        self._started = True

        def producer():
            try:
                block_gen = self.transform.build_insert(self.context)
                while not self._stop_event.is_set():
                    try:
                        block = next(block_gen)
                    except StopIteration:
                        self._put(EOF_SENTINEL)
                        return

                    if not self._put(block):
                        return

            except Exception as e:
                # Driver errors are deterministic client-side refusals, not operational failures.
                if isinstance(e, Error):
                    logger.debug("Insert producer error: %s", e)
                else:
                    logger.error("Insert producer error: %s", e, exc_info=True)
                if getattr(self.context, "insert_exception", None) is None:
                    self.context.insert_exception = e
                if not self._stop_event.is_set():
                    self._put(e)
            finally:
                self.queue.shutdown()

        self._producer_future = self.loop.run_in_executor(None, producer)

    async def async_generator(self):
        """Async generator that yields blocks for aiohttp streaming."""
        if not self._started:
            raise RuntimeError("Producer not started, call start_producer() first")

        try:
            while True:
                chunk = await self.queue.async_q.get()

                if chunk is EOF_SENTINEL:
                    break

                if isinstance(chunk, Exception):
                    raise chunk

                yield chunk

        except Exception as e:
            if isinstance(e, Error):
                logger.debug("Insert consumer error: %s", e)
            else:
                logger.error("Insert consumer error: %s", e, exc_info=True)
            raise
        finally:
            self._stop_event.set()
            self.queue.shutdown()
            if self._producer_future and not self._producer_future.done():
                try:
                    await self._producer_future
                except Exception:
                    pass

    async def close(self, timeout: float | None = 1.0):
        """Shut down the queue and wait for the producer thread to terminate. Pass ``timeout=None`` to wait without a deadline."""
        self._stop_event.set()
        self.queue.shutdown()
        if self._producer_future and not self._producer_future.done():
            try:
                if timeout is None:
                    await self._producer_future
                else:
                    await asyncio.wait_for(asyncio.shield(self._producer_future), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("Insert producer did not finish within timeout")
            except Exception:
                pass

    def _put(self, item: bytes | bytearray | Exception) -> bool:
        while not self._stop_event.is_set():
            try:
                self.queue.sync_q.put(item, timeout=0.1)
                return True
            except AsyncQueueFull:
                continue
            except RuntimeError:
                return False
        return False


def _read_ahead_producer(
    source: ByteSource,
    out: queue.Queue[tuple[str, object]],
    stop_event: threading.Event,
) -> None:
    def put(item: tuple[str, object]) -> bool:
        while not stop_event.is_set():
            try:
                out.put(item, timeout=0.1)
                return True
            except queue.Full:
                continue
        return False

    try:
        for chunk in source.gen:
            if not put(("data", chunk)):
                return
    except BaseException as ex:  # noqa: BLE001 - forwarded to the consumer thread verbatim
        put(("error", ex))
    finally:
        put(("eof", None))


def _read_ahead_consumer(source_queue: queue.Queue[tuple[str, object]]) -> Iterator[bytes]:
    while True:
        tag, payload = source_queue.get()
        if tag == "data":
            yield cast(bytes, payload)
        elif tag == "error":
            raise cast(BaseException, payload)
        else:  # eof
            return


def _drain_read_ahead_queue(source_queue: queue.Queue[tuple[str, object]]) -> None:
    try:
        while True:
            source_queue.get_nowait()
    except queue.Empty:
        pass


def _release_abandoned_read_ahead(source: ByteSource, source_queue: queue.Queue[tuple[str, object]]) -> None:
    try:
        _drain_read_ahead_queue(source_queue)
    finally:
        try:
            source.close()
        except Exception:  # noqa: BLE001 - finalizers must not raise
            pass


def _finalize_read_ahead_off_loop(
    loop: asyncio.AbstractEventLoop,
    source: ByteSource,
    source_queue: queue.Queue[tuple[str, object]],
    producer_thread: threading.Thread,
) -> None:
    try:
        if producer_thread.is_alive():
            producer_thread.join(timeout=1.0)
    except Exception:  # noqa: BLE001 - finalizers must not raise
        pass
    try:
        loop.call_soon_threadsafe(_release_abandoned_read_ahead, source, source_queue)
    except Exception:  # noqa: BLE001 - a closed loop must not leak the source
        _release_abandoned_read_ahead(source, source_queue)


class ReadAheadSource(Closable):
    """Reads chunks from a byte source on a daemon thread into a bounded queue so transport overlaps decode.

    The consumer generator re-raises any producer-side exception verbatim, in stream order, so the wrapping
    codec's error mapping, exception-tag scanning, and last-chunk heuristics run unchanged on the consumer thread.
    """

    def __init__(self, source: ByteSource, maxsize: int = 16):
        self.source: ByteSource | None = source
        self.exception_tag: str | None = getattr(source, "exception_tag", None)
        self.queue: queue.Queue[tuple[str, object]] = queue.Queue(maxsize=maxsize)
        self._stop_event = threading.Event()
        self._gen_cache: Iterator[bytes] | None = None
        self._thread = threading.Thread(
            target=_read_ahead_producer,
            args=(source, self.queue, self._stop_event),
            name="clickhouse-read-ahead",
            daemon=True,
        )
        self._thread.start()

    def __del__(self) -> None:
        try:
            if self._stop_event.is_set():
                return
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                self.close()
                return

            self._stop_event.set()
            source, self.source = self.source, None
            if source is None:
                return
            try:
                loop.run_in_executor(
                    None,
                    _finalize_read_ahead_off_loop,
                    loop,
                    source,
                    self.queue,
                    self._thread,
                )
            except Exception:
                _finalize_read_ahead_off_loop(loop, source, self.queue, self._thread)
        except Exception:  # noqa: BLE001 - finalizers must not raise
            pass

    @property
    def gen(self) -> Iterator[bytes]:
        if self._gen_cache is None:
            self._gen_cache = _read_ahead_consumer(self.queue)
        return self._gen_cache

    def _drain(self):
        _drain_read_ahead_queue(self.queue)

    def _release_source(self):
        source, self.source = self.source, None
        if source is not None:
            source.close()

    def close(self) -> None:
        # Join the producer before closing the source. A queue-blocked producer returns within one put
        # timeout of the stop event; a read-blocked producer exits after its in-flight read returns. Closing
        # the source only after the join keeps the transport single-reader: the sync source drains on close,
        # which would race a producer still reading it.
        self._stop_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout=1.0)
        self._drain()
        self._release_source()

    async def aclose(self) -> None:
        self._stop_event.set()
        if self._thread.is_alive():
            # Join off the event loop so the worst-case wait never blocks it.
            await asyncio.get_running_loop().run_in_executor(None, self._thread.join, 1.0)
        self._drain()
        # Release on the loop thread: the async source's close cancels its producer task, which must not
        # run from an executor thread.
        self._release_source()


class _SyncStreamingInsertSource:
    """Bounded producer/consumer source for sync inserts."""

    def __init__(self, transform: Transform, context, maxsize: int = 10):
        self.transform = transform
        self.context = context
        self.queue: queue.Queue[bytes | Exception | object] = queue.Queue(maxsize=maxsize)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = False

    def start_producer(self):
        if self._started:
            raise RuntimeError("Producer already started")
        self._started = True
        self._thread = threading.Thread(target=self._producer, name="clickhouse-insert-producer", daemon=True)
        self._thread.start()

    @property
    def gen(self) -> Iterator[bytes]:
        if not self._started:
            raise RuntimeError("Producer not started, call start_producer() first")
        try:
            while True:
                chunk = self.queue.get()
                if chunk is EOF_SENTINEL:
                    break
                if isinstance(chunk, Exception):
                    raise chunk
                yield cast(bytes, chunk)
        finally:
            self.close()

    def close(self, timeout: float | None = 1.0):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)

    def _producer(self):
        try:
            block_gen = self.transform.build_insert(self.context)
            while not self._stop_event.is_set():
                try:
                    block = next(block_gen)
                except StopIteration:
                    self._put(EOF_SENTINEL)
                    return
                if not self._put(block):
                    return
        except Exception as ex:
            # Driver errors are deterministic client-side refusals, not operational failures.
            if isinstance(ex, Error):
                logger.debug("Insert producer error: %s", ex)
            else:
                logger.error("Insert producer error: %s", ex, exc_info=True)
            if getattr(self.context, "insert_exception", None) is None:
                self.context.insert_exception = ex
            if not self._stop_event.is_set():
                self._put(ex)

    def _put(self, item: bytes | Exception | object) -> bool:
        while not self._stop_event.is_set():
            try:
                self.queue.put(item, timeout=0.1)
                return True
            except queue.Full:
                continue
        return False
