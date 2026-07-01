import asyncio
import logging
import threading
import zlib
from collections.abc import Iterator

import lz4.frame
import zstandard

from clickhouse_connect.driver.asyncqueue import EOF_SENTINEL, AsyncSyncQueue
from clickhouse_connect.driver.compression import available_compression
from clickhouse_connect.driver.exceptions import OperationalError
from clickhouse_connect.driver.types import Closable

logger = logging.getLogger(__name__)

__all__ = ["StreamingResponseSource", "StreamingFileAdapter", "StreamingInsertSource"]

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
        self.queue = AsyncSyncQueue(maxsize=10)

        self._decompressor = None
        self._decompressor_initialized = False

        # Multiple accesses to .gen must return the same generator, not create new ones
        self._gen_cache = None

        self._producer_task = None
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
            return zstandard.ZstdDecompressor().decompressobj()

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

        if self._producer_task and not self._producer_task.done():
            self._producer_task.cancel()

        if self.response and not self.response.closed:
            if not self._producer_completed:
                self.response.close()
        self._release_lease()


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

    def __init__(self, transform, context, loop: asyncio.AbstractEventLoop, maxsize: int = 10):
        self.transform = transform
        self.context = context
        self.loop = loop
        self.queue = AsyncSyncQueue(maxsize=maxsize)
        self._producer_future = None
        self._started = False

    def start_producer(self):
        if self._started:
            raise RuntimeError("Producer already started")
        self._started = True

        def producer():
            try:
                for block in self.transform.build_insert(self.context):
                    self.queue.sync_q.put(block)

                self.queue.sync_q.put(EOF_SENTINEL)

            except Exception as e:
                logger.error("Insert producer error: %s", e, exc_info=True)
                try:
                    self.queue.sync_q.put(e)
                except Exception:
                    pass
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
            logger.error("Insert consumer error: %s", e, exc_info=True)
            raise
        finally:
            if self._producer_future and not self._producer_future.done():
                try:
                    await self._producer_future
                except Exception:
                    pass

    async def close(self, timeout: float | None = 1.0):
        """Shut down the queue and wait for the producer thread to terminate. Pass ``timeout=None`` to wait without a deadline."""
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
