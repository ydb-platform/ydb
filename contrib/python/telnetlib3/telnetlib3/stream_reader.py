"""Module provides class TelnetReader and TelnetReaderUnicode."""

from __future__ import annotations

# std imports
import re
import sys
import codecs
import asyncio
import logging
import warnings
from typing import Callable, Optional
from asyncio import format_helpers

__all__ = ("TelnetReader", "TelnetReaderUnicode")

_DEFAULT_LIMIT = 2**16  # 64 KiB


class TelnetReader:
    """
    Telnet protocol stream reader.

    A copy of :class:`asyncio.StreamReader` with telnet-aware readline().
    """

    _source_traceback = None

    def __init__(self, limit: int = _DEFAULT_LIMIT) -> None:
        """Initialize TelnetReader with optional buffer size limit."""
        self.log = logging.getLogger(__name__)
        # The line length limit is  a security feature;
        # it also doubles as half the buffer limit.

        if limit <= 0:
            raise ValueError("Limit cannot be <= 0")

        self._limit = limit
        self._buffer = bytearray()
        self._eof = False  # Whether we're done.
        self._waiter: Optional[asyncio.Future[None]] = None
        self._exception: Optional[Exception] = None
        self._transport: Optional[asyncio.BaseTransport] = None
        self._paused = False
        try:
            loop = asyncio.get_running_loop()
            if loop.get_debug():
                self._source_traceback = format_helpers.extract_stack(sys._getframe(1))
        except RuntimeError:
            pass

    def __repr__(self) -> str:
        """Description of stream encoding state."""
        info = [type(self).__name__]
        if self._buffer:
            info.append(f"{len(self._buffer)} bytes")
        if self._eof:
            info.append("eof")
        if self._limit != _DEFAULT_LIMIT:
            info.append(f"limit={self._limit}")
        if self._waiter:
            info.append(f"waiter={self._waiter!r}")
        if self._exception:
            info.append(f"exception={self._exception!r}")
        if self._transport:
            info.append(f"transport={self._transport!r}")
        if self._paused:
            info.append("paused")
        info.append("encoding=False")
        return f"<{' '.join(info)}>"

    def exception(self) -> Optional[Exception]:
        """Return the exception if set, otherwise None."""
        return self._exception

    def set_exception(self, exc: Exception) -> None:
        """Set the exception and wake up any waiting coroutine."""
        self._exception = exc

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.cancelled():
                waiter.set_exception(exc)

    def _wakeup_waiter(self) -> None:
        """Wakeup read*() functions waiting for data or EOF."""
        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.cancelled():
                waiter.set_result(None)

    def set_transport(self, transport: asyncio.BaseTransport) -> None:
        """Set the transport for flow control."""
        self._transport = transport

    def _maybe_resume_transport(self) -> None:
        if self._paused and len(self._buffer) <= self._limit:
            self._paused = False
            self._transport.resume_reading()

    def feed_eof(self) -> None:
        """
        Mark EOF on the reader and wake any pending readers.

        This should be called by the Telnet protocol when the underlying transport indicates end-of-
        input (e.g., in connection_lost()). It sets the internal EOF flag and wakes any read
        coroutines waiting for more data.

        Application code typically should not call this method directly. To gracefully close a
        connection from application code, call writer.close() and await writer.wait_closed(). The
        protocol will eventually call feed_eof() on the reader as part of shutdown.

        After feed_eof(), read() will drain remaining buffered bytes and then return b"";
        readline()/iteration will stop at EOF.
        """
        self._eof = True
        self._wakeup_waiter()

    def at_eof(self) -> bool:
        """Return True if the buffer is empty and 'feed_eof' was called."""
        return self._eof and not self._buffer

    def feed_data(self, data: bytes) -> None:
        """Feed data bytes to the reader buffer."""
        if not data:
            return

        self._buffer.extend(data)
        self._wakeup_waiter()

        if self._transport is not None and not self._paused and len(self._buffer) > 2 * self._limit:
            try:
                self._transport.pause_reading()  # type: ignore[attr-defined]
            except NotImplementedError:
                # The transport can't be paused.
                # We'll just have to buffer all data.
                # Forget the transport so we don't keep trying.
                self._transport = None
            else:
                self._paused = True

    async def _wait_for_data(self, func_name: str) -> None:
        """
        Wait until feed_data() or feed_eof() is called.

        If stream was paused, automatically resume it.
        """
        # StreamReader uses a future to link the protocol feed_data() method
        # to a read coroutine. Running two read coroutines at the same time
        # would have an unexpected behaviour. It would not possible to know
        # which coroutine would get the next data.
        if self._waiter is not None:
            raise RuntimeError(
                f"{func_name}() called while another coroutine is "
                f"already waiting for incoming data"
            )

        # Waiting for data while paused will make deadlock, so prevent it.
        # This is essential for readexactly(n) for case when n > self._limit.
        if self._paused:
            self._paused = False
            self._transport.resume_reading()

        self._waiter = asyncio.get_running_loop().create_future()
        try:
            await self._waiter
        finally:
            self._waiter = None

    async def readuntil(self, separator: bytes = b"\n") -> bytes:
        """
        Read data from the stream until ``separator`` is found.

        On success, the data and separator will be removed from the internal buffer (consumed).
        Returned data will include the separator at the end.

        Configured stream limit is used to check result. Limit sets the maximal length of data that
        can be returned, not counting the separator.

        If an EOF occurs and the complete separator is still not found, an IncompleteReadError
        exception will be raised, and the internal buffer will be reset.  The
        IncompleteReadError.partial attribute may contain the separator partially.

        If the data cannot be read because of over limit, a LimitOverrunError exception  will be
        raised, and the data will be left in the internal buffer, so it can be read again.

        :raises ValueError: If separator is empty.
        :raises asyncio.LimitOverrunError: If separator is not found and buffer exceeds limit.
        :raises asyncio.IncompleteReadError: If EOF is reached before separator is found.
        """
        seplen = len(separator)
        if seplen == 0:
            raise ValueError("Separator should be at least one-byte string")

        if self._exception is not None:
            raise self._exception

        # Consume whole buffer except last bytes, which length is
        # one less than seplen. Let's check corner cases with
        # separator='SEPARATOR':
        # * we have received almost complete separator (without last
        #   byte). i.e buffer='some textSEPARATO'. In this case we
        #   can safely consume len(separator) - 1 bytes.
        # * last byte of buffer is first byte of separator, i.e.
        #   buffer='abcdefghijklmnopqrS'. We may safely consume
        #   everything except that last byte, but this require to
        #   analyze bytes of buffer that match partial separator.
        #   This is slow and/or require FSM. For this case our
        #   implementation is not optimal, since require rescanning
        #   of data that is known to not belong to separator. In
        #   real world, separator will not be so long to notice
        #   performance problems. Even when reading MIME-encoded
        #   messages :)

        # `offset` is the number of bytes from the beginning of the buffer
        # where there is no occurrence of `separator`.
        offset = 0

        # Loop until we find `separator` in the buffer, exceed the buffer size,
        # or an EOF has happened.
        while True:
            buflen = len(self._buffer)

            # Check if we now have enough data in the buffer for `separator` to
            # fit.
            if buflen - offset >= seplen:
                isep = self._buffer.find(separator, offset)

                if isep != -1:
                    # `separator` is in the buffer. `isep` will be used later
                    # to retrieve the data.
                    break

                # see upper comment for explanation.
                offset = buflen + 1 - seplen
                if offset > self._limit:
                    raise asyncio.LimitOverrunError(
                        "Separator is not found, and chunk exceed the limit", offset
                    )

            # Complete message (with full separator) may be present in buffer
            # even when EOF flag is set. This may happen when the last chunk
            # adds data which makes separator be found. That's why we check for
            # EOF *ater* inspecting the buffer.
            if self._eof:
                chunk = bytes(self._buffer)
                self._buffer.clear()
                raise asyncio.IncompleteReadError(chunk, None)

            # _wait_for_data() will resume reading if stream was paused.
            await self._wait_for_data("readuntil")

        if isep > self._limit:
            raise asyncio.LimitOverrunError(
                "Separator is found, but chunk is longer than limit", isep
            )

        result = bytes(self._buffer[: isep + seplen])
        del self._buffer[: isep + seplen]
        self._maybe_resume_transport()
        return result

    async def readuntil_pattern(self, pattern: re.Pattern[bytes]) -> bytes:
        """
        Read data from the stream until ``pattern`` is found.

        On success, the data and pattern will be removed from the internal buffer (consumed).
        Returned data will include the pattern at the end.

        Configured stream limit is used to check result. Limit sets the maximal length of data that
        can be returned, not counting the pattern.

        If an EOF occurs and the complete pattern is still not found, an IncompleteReadError
        exception will be raised, and the internal buffer will be reset. The
        IncompleteReadError.partial attribute may contain the pattern partially.

        If the data cannot be read because of over limit, a LimitOverrunError exception will be
        raised, and the data will be left in the internal buffer, so it can be read again.

        :raises ValueError: If pattern is None, not a re.Pattern, or not a bytes pattern.
        :raises asyncio.LimitOverrunError: If pattern is not found and buffer exceeds limit.
        :raises asyncio.IncompleteReadError: If EOF is reached before pattern is found.
        """
        if pattern is None or not isinstance(pattern, re.Pattern):
            raise ValueError("pattern should be a re.Pattern object")

        if not isinstance(pattern.pattern, bytes):
            raise ValueError("Only bytes patterns are supported")

        if self._exception is not None:
            raise self._exception

        while True:
            # Search for the pattern in the buffer.
            match = pattern.search(self._buffer)

            if match:
                # Pattern was found.
                match_end = match.end()

                # Check if the return data exceeds the limit.
                if match_end > self._limit:
                    raise asyncio.LimitOverrunError(
                        "Pattern is found, but chunk is longer than limit", match_end
                    )

                # Consume and return the data including the pattern.
                chunk = self._buffer[:match_end]
                del self._buffer[:match_end]
                self._maybe_resume_transport()
                return bytes(chunk)

            # If we're here, the pattern wasn't found in the current buffer.

            # Check if the buffer has grown beyond the limit without a match.
            if len(self._buffer) > self._limit:
                raise asyncio.LimitOverrunError(
                    "Pattern not found, and buffer exceed the limit", len(self._buffer)
                )

            # If the stream is at EOF, and we still haven't found the pattern,
            # raise an exception with the partial data. This is checked after
            # searching the buffer, as the last received chunk might complete the pattern.
            if self._eof:
                partial = bytes(self._buffer)
                self._buffer.clear()
                raise asyncio.IncompleteReadError(partial, None)

            # Wait for more data to arrive since the pattern was not found and
            # we are not at EOF.
            await self._wait_for_data("readuntil_pattern")

    async def read(self, n: int = -1) -> bytes:
        """
        Read up to `n` bytes from the stream.

        If n is not provided, or set to -1, read until EOF and return all read
        bytes. If the EOF was received and the internal buffer is empty, return
        an empty bytes object.

        If n is zero, return empty bytes object immediately.

        If n is positive, this function try to read `n` bytes, and may return
        less or equal bytes than requested, but at least one byte. If EOF was
        received before any byte is read, this function returns empty byte
        object.

        Returned value is not limited with limit, configured at stream
        creation.

        If stream was paused, this function will automatically resume it if
        needed.
        """
        if self._exception is not None:
            raise self._exception

        if n == 0:
            return b""

        if n < 0:
            # This used to just loop creating a new waiter hoping to
            # collect everything in self._buffer, but that would
            # deadlock if the subprocess sends more than self.limit
            # bytes.  So just call self.read(self._limit) until EOF.
            blocks = []
            while True:
                block = await self.read(self._limit)
                if not block:
                    break
                blocks.append(block)
            return b"".join(blocks)

        if not self._buffer and not self._eof:
            await self._wait_for_data("read")

        # This will work right even if buffer is less than n bytes
        data = bytes(self._buffer[:n])
        del self._buffer[:n]

        self._maybe_resume_transport()
        return data

    async def readexactly(self, n: int) -> bytes:
        """
        Read exactly `n` bytes.

        Raise an IncompleteReadError if EOF is reached before `n` bytes can be
        read. The IncompleteReadError.partial attribute of the exception will
        contain the partial read bytes.

        if n is zero, return empty bytes object.

        Returned value is not limited with limit, configured at stream
        creation.

        If stream was paused, this function will automatically resume it if
        needed.

        :raises ValueError: If n is negative.
        :raises asyncio.IncompleteReadError: If EOF is reached before n bytes are read.
        """
        if n < 0:
            raise ValueError("readexactly size can not be less than zero")

        if self._exception is not None:
            raise self._exception

        if n == 0:
            return b""

        while len(self._buffer) < n:
            if self._eof:
                incomplete = bytes(self._buffer)
                self._buffer.clear()
                raise asyncio.IncompleteReadError(incomplete, n)

            await self._wait_for_data("readexactly")

        if len(self._buffer) == n:
            data = bytes(self._buffer)
            self._buffer.clear()
        else:
            data = bytes(self._buffer[:n])
            del self._buffer[:n]
        self._maybe_resume_transport()
        return data

    def __aiter__(self) -> "TelnetReader":
        return self

    async def __anext__(self) -> bytes:
        val = await self.readline()
        if val == b"":
            raise StopAsyncIteration
        return val

    # these next two are deprecated in 2.0.1, feed_eof should just be called,
    # instead of the commit 260dd63a that introduced a close() method on a
    # reader.
    @property
    def connection_closed(self) -> bool:
        """Deprecated: use at_eof() instead."""
        warnings.warn(
            "connection_closed property removed, use at_eof() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._eof

    def close(self) -> None:
        """
        Deprecated: use feed_eof() instead.

        TelnetReader.close() existed briefly and is deprecated. Protocol
        implementations should call feed_eof() to signal end-of-input.
        Application code should not close the reader directly; instead,
        call writer.close() and await writer.wait_closed() to initiate a
        graceful shutdown.
        """
        warnings.warn(
            "close() is deprecated; use feed_eof() instead", DeprecationWarning, stacklevel=2
        )
        self.feed_eof()

    async def readline(self) -> bytes:
        r"""
        Read one line.

        Where "line" is a sequence of characters ending with CR LF, LF,
        or CR NUL. This readline function is a strict interpretation of
        Telnet Protocol :rfc:`854`.

          The sequence "CR LF" must be treated as a single "new line" character
          and used whenever their combined action is intended; The sequence "CR
          NUL" must be used where a carriage return alone is actually desired;
          and the CR character must be avoided in other contexts.

        And therefore, a line does not yield for a stream containing a
        CR if it is not succeeded by NUL or LF.

        ================= =====================
        Given stream      readline() yields
        ================= =====================
        ``--\r\x00---``   ``--\r``, ``---`` *...*
        ``--\r\n---``     ``--\r\n``, ``---`` *...*
        ``--\n---``       ``--\n``, ``---`` *...*
        ``--\r---``       ``--\r``, ``---`` *...*
        ================= =====================

        If EOF is received before the termination of a line, the method will
        yield the partially read string.
        """
        if self._exception is not None:
            raise self._exception

        line = bytearray()
        not_enough = True

        while not_enough:
            while self._buffer and not_enough:
                search_results_pos_kind = (
                    (self._buffer.find(b"\r\n"), b"\r\n"),
                    (self._buffer.find(b"\r\x00"), b"\r\x00"),
                    (self._buffer.find(b"\r"), b"\r"),
                    (self._buffer.find(b"\n"), b"\n"),
                )

                # sort by (position, length * -1), so that the
                # smallest sorted value is the longest-match,
                # preferring '\r\n' over '\r', for example.
                matches = [
                    (_pos, len(_kind) * -1, _kind)
                    for _pos, _kind in search_results_pos_kind
                    if _pos != -1
                ]

                if not matches:
                    line.extend(self._buffer)
                    self._buffer.clear()
                    continue

                # position is nearest match,
                pos, _, kind = min(matches)
                if kind == b"\r\x00":
                    # trim out '\x00'
                    begin, end = pos + 1, pos + 2
                elif kind == b"\r\n":
                    begin = end = pos + 2
                else:
                    # '\r' or '\n'
                    begin = end = pos + 1
                line.extend(self._buffer[:begin])
                del self._buffer[:end]
                not_enough = False

            if self._eof:
                break

            if not_enough:
                await self._wait_for_data("readline")

        self._maybe_resume_transport()
        buf = bytes(line)
        return buf


class TelnetReaderUnicode(TelnetReader):
    """
    Unicode-decoding Telnet stream reader.

    Extends TelnetReader to provide automatic decoding of bytes to unicode strings using a
    configurable encoding determined by callback function.
    """

    #: Late-binding instance of :class:`codecs.IncrementalDecoder`, some
    #: bytes may be lost if the protocol's encoding is changed after
    #: previously receiving a partial multibyte.  This isn't common in
    #: practice, however.
    _decoder = None

    def __init__(
        self,
        fn_encoding: Callable[..., str],
        *,
        limit: int = _DEFAULT_LIMIT,
        encoding_errors: str = "replace",
    ) -> None:
        """
        A Unicode StreamReader interface for Telnet protocol.

        :param fn_encoding: Function callback, receiving boolean keyword argument
            ``incoming=True``, which is used by the callback to determine what
            encoding should be used to decode the value in the direction specified.
        """
        super().__init__(limit=limit)

        self.fn_encoding = fn_encoding
        self.encoding_errors = encoding_errors

    def decode(self, buf: bytes, final: bool = False) -> str:
        """Decode bytes ``buf`` using preferred encoding."""
        if buf == b"":
            return ""  # EOF

        encoding = self.fn_encoding(incoming=True)

        # late-binding,
        if self._decoder is None or encoding != getattr(self._decoder, "_encoding", ""):
            self._decoder = codecs.getincrementaldecoder(encoding)(errors=self.encoding_errors)
            setattr(self._decoder, "_encoding", encoding)

        return self._decoder.decode(buf, final)

    async def readline(self) -> str:  # type: ignore[override]
        """
        Read one line.

        See ancestor method, :func:`~TelnetReader.readline` for details.
        """
        buf = await super().readline()
        return self.decode(buf)

    async def read(self, n: int = -1) -> str:  # type: ignore[override]
        """
        Read up to *n* bytes.

        If the EOF was received and the internal buffer is empty, return an empty string.

        :param n: If *n* is not provided, or set to -1, read until EOF and return all characters as
            one large string.
        """
        if self._exception is not None:
            raise self._exception

        if not n:
            return ""

        if n < 0:
            # This used to just loop creating a new waiter hoping to
            # collect everything in self._buffer, but that would
            # deadlock if the subprocess sends more than self.limit
            # bytes.  So just call self.read(self._limit) until EOF.
            blocks = []
            while True:
                block = await self.read(self._limit)
                if not block:
                    # eof
                    break
                blocks.append(block)
            return "".join(blocks)

        if not self._buffer and not self._eof:
            await self._wait_for_data("read")

        buf = self.decode(bytes(self._buffer))
        if n < 0 or len(buf) <= n:
            u_data = buf
            self._buffer.clear()
        else:
            u_data = ""
            while n > len(u_data):
                u_data += self.decode(bytes([self._buffer.pop(0)]))

        self._maybe_resume_transport()
        return u_data

    async def readexactly(self, n: int) -> str:  # type: ignore[override]
        """
        Read exactly *n* unicode characters.

        :raises asyncio.IncompleteReadError: if the end of the stream is
            reached before *n* can be read. The
            :attr:`asyncio.IncompleteReadError.partial` attribute of the
            exception contains the partial read characters.
        """
        if self._exception is not None:
            raise self._exception

        blocks: list[str] = []
        while n > 0:
            block = await self.read(n)
            if not block:
                partial = "".join(blocks)
                raise asyncio.IncompleteReadError(
                    partial, len(partial) + n  # type: ignore[arg-type]
                )
            blocks.append(block)
            n -= len(block)

        return "".join(blocks)

    def __repr__(self) -> str:
        """Description of stream encoding state."""
        encoding = None
        if callable(self.fn_encoding):
            encoding = self.fn_encoding(incoming=True)
        return (
            "<TelnetReaderUnicode encoding={encoding!r} limit={self._limit!r} "
            "buflen={buflen} eof={self._eof}>".format(
                encoding=encoding, buflen=len(self._buffer), self=self
            )
        )
