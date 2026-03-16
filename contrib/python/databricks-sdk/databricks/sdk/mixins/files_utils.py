from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from typing import Any, BinaryIO, Callable, Iterable, Optional


@dataclass
class CreateDownloadUrlResponse:
    """Response from the download URL API call."""

    url: str
    """The presigned URL to download the file."""
    headers: dict[str, str]
    """Headers to use when making the download request."""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CreateDownloadUrlResponse:
        """Create an instance from a dictionary."""
        if "url" not in data:
            raise ValueError("Missing 'url' in response data")
        headers = data["headers"] if "headers" in data else {}
        parsed_headers = {x["name"]: x["value"] for x in headers}
        return cls(url=data["url"], headers=parsed_headers)


class _ConcatenatedInputStream(BinaryIO):
    """This class joins two input streams into one."""

    def __init__(self, head_stream: BinaryIO, tail_stream: BinaryIO):
        if not head_stream.readable():
            raise ValueError("head_stream is not readable")
        if not tail_stream.readable():
            raise ValueError("tail_stream is not readable")

        self._head_stream = head_stream
        self._tail_stream = tail_stream
        self._head_size = None
        self._tail_size = None

    def close(self) -> None:
        try:
            self._head_stream.close()
        finally:
            self._tail_stream.close()

    def fileno(self) -> int:
        raise AttributeError()

    def flush(self) -> None:
        raise NotImplementedError("Stream is not writable")

    def isatty(self) -> bool:
        raise NotImplementedError()

    def read(self, __n: int = -1) -> bytes:
        head = self._head_stream.read(__n)
        remaining_bytes = __n - len(head) if __n >= 0 else __n
        tail = self._tail_stream.read(remaining_bytes)
        return head + tail

    def readable(self) -> bool:
        return True

    def readline(self, __limit: int = -1) -> bytes:
        # Read and return one line from the stream.
        # If __limit is specified, at most __limit bytes will be read.
        # The line terminator is always b'\n' for binary files.
        head = self._head_stream.readline(__limit)
        if len(head) > 0 and head[-1:] == b"\n":
            # end of line happened before (or at) the limit
            return head

        # if __limit >= 0, len(head) can't exceed limit
        remaining_bytes = __limit - len(head) if __limit >= 0 else __limit
        tail = self._tail_stream.readline(remaining_bytes)
        return head + tail

    def readlines(self, __hint: int = -1) -> list[bytes]:
        # Read and return a list of lines from the stream.
        # Hint can be specified to control the number of lines read: no more lines will be read
        # If the total size (in bytes/characters) of all lines so far exceeds hint.

        # In fact, BytesIO(bytes) will not read next line if total size of all lines
        # *equals or* exceeds hint.

        head_result = self._head_stream.readlines(__hint)
        head_total_bytes = sum(len(line) for line in head_result)

        if 0 < __hint <= head_total_bytes and head_total_bytes > 0:
            # We reached (or passed) the hint by reading from head_stream, or exhausted head_stream.

            if head_result[-1][-1:] == b"\n":
                # If we reached/passed the hint and also stopped at the line break, return.
                return head_result

            # Reading from head_stream could have stopped only because the stream was exhausted
            if len(self._head_stream.read(1)) > 0:
                raise ValueError(
                    f"Stream reading finished prematurely after reading {head_total_bytes} bytes, reaching or exceeding hint {__hint}"
                )

            # We need to finish reading the current line, now from tail_stream.

            tail_result = self._tail_stream.readlines(1)  # We will only read the first line from tail_stream.
            assert len(tail_result) <= 1
            if len(tail_result) > 0:
                # We will then append the tail as the last line of the result.
                return head_result[:-1] + [head_result[-1] + tail_result[0]]
            else:
                return head_result

        # We did not reach the hint by reading head_stream but exhausted it, continue reading from tail_stream
        # with an adjusted hint
        if __hint >= 0:
            remaining_bytes = __hint - head_total_bytes
        else:
            remaining_bytes = __hint

        tail_result = self._tail_stream.readlines(remaining_bytes)

        if head_total_bytes > 0 and head_result[-1][-1:] != b"\n" and len(tail_result) > 0:
            # If head stream does not end with the line break, we need to concatenate
            # the last line of the head result and the first line of tail result
            return head_result[:-1] + [head_result[-1] + tail_result[0]] + tail_result[1:]
        else:
            # Otherwise, just append two lists of lines.
            return head_result + tail_result

    def _get_stream_size(self, stream: BinaryIO) -> int:
        prev_offset = stream.tell()
        try:
            stream.seek(0, os.SEEK_END)
            return stream.tell()
        finally:
            stream.seek(prev_offset, os.SEEK_SET)

    def _get_head_size(self) -> int:
        if self._head_size is None:
            self._head_size = self._get_stream_size(self._head_stream)
        return self._head_size

    def _get_tail_size(self) -> int:
        if self._tail_size is None:
            self._tail_size = self._get_stream_size(self._tail_stream)
        return self._tail_size

    def seek(self, __offset: int, __whence: int = os.SEEK_SET) -> int:
        if not self.seekable():
            raise NotImplementedError("Stream is not seekable")

        if __whence == os.SEEK_SET:
            if __offset < 0:
                # Follow native buffer behavior
                raise ValueError(f"Negative seek value: {__offset}")

            head_size = self._get_head_size()

            if __offset <= head_size:
                self._head_stream.seek(__offset, os.SEEK_SET)
                self._tail_stream.seek(0, os.SEEK_SET)
            else:
                self._head_stream.seek(0, os.SEEK_END)  # move head stream to the end
                self._tail_stream.seek(__offset - head_size, os.SEEK_SET)

        elif __whence == os.SEEK_CUR:
            current_offset = self.tell()
            new_offset = current_offset + __offset
            if new_offset < 0:
                # gracefully don't seek before start
                new_offset = 0
            self.seek(new_offset, os.SEEK_SET)

        elif __whence == os.SEEK_END:
            if __offset > 0:
                # Python allows to seek beyond the end of stream.

                # Move head to EOF and tail to (EOF + offset), so subsequent tell()
                # returns len(head) + len(tail) + offset, same as for native buffer
                self._head_stream.seek(0, os.SEEK_END)
                self._tail_stream.seek(__offset, os.SEEK_END)
            else:
                self._tail_stream.seek(__offset, os.SEEK_END)
                tail_pos = self._tail_stream.tell()
                if tail_pos > 0:
                    # target position lies within the tail, move head to EOF
                    self._head_stream.seek(0, os.SEEK_END)
                else:
                    tail_size = self._get_tail_size()
                    self._head_stream.seek(__offset + tail_size, os.SEEK_END)
        else:
            raise ValueError(__whence)
        return self.tell()

    def seekable(self) -> bool:
        return self._head_stream.seekable() and self._tail_stream.seekable()

    def __getattribute__(self, name: str) -> Any:
        if name == "fileno":
            raise AttributeError()
        elif name in ["tell", "seek"] and not self.seekable():
            raise AttributeError()

        return super().__getattribute__(name)

    def tell(self) -> int:
        if not self.seekable():
            raise NotImplementedError()

        # Assuming that tail stream stays at 0 until head stream is exhausted
        return self._head_stream.tell() + self._tail_stream.tell()

    def truncate(self, __size: Optional[int] = None) -> int:
        raise NotImplementedError("Stream is not writable")

    def writable(self) -> bool:
        return False

    def write(self, __s: bytes) -> int:
        raise NotImplementedError("Stream is not writable")

    def writelines(self, __lines: Iterable[bytes]) -> None:
        raise NotImplementedError("Stream is not writable")

    def __next__(self) -> bytes:
        # IOBase [...] supports the iterator protocol, meaning that an IOBase object can be
        # iterated over yielding the lines in a stream. [...] See readline().
        result = self.readline()
        if len(result) == 0:
            raise StopIteration
        return result

    def __iter__(self) -> "BinaryIO":
        return self

    def __enter__(self) -> "BinaryIO":
        self._head_stream.__enter__()
        self._tail_stream.__enter__()
        return self

    def __exit__(self, __type, __value, __traceback) -> None:
        self._head_stream.__exit__(__type, __value, __traceback)
        self._tail_stream.__exit__(__type, __value, __traceback)

    def __str__(self) -> str:
        return f"Concat: {self._head_stream}, {self._tail_stream}]"


class _PresignedUrlDistributor:
    """
    Distributes and manages presigned URLs for downloading files.

    This class ensures thread-safe access to a presigned URL, allowing retrieval and invalidation.
    When the URL is invalidated, a new one will be fetched using the provided function.
    """

    def __init__(self, get_new_url_func: Callable[[], CreateDownloadUrlResponse]):
        """
        Initialize the distributor.

        Args:
            get_new_url_func: A callable that returns a new presigned URL response.
        """
        self._get_new_url_func = get_new_url_func
        self._current_url = None
        self.current_version = 0
        self.lock = threading.RLock()

    def get_url(self) -> tuple[CreateDownloadUrlResponse, int]:
        """
        Get the current presigned URL and its version.

        Returns:
            A tuple containing the current presigned URL response and its version.
        """
        with self.lock:
            if self._current_url is None:
                self._current_url = self._get_new_url_func()
            return self._current_url, self.current_version

    def invalidate_url(self, version: int) -> None:
        """
        Invalidate the current presigned URL if the version matches. If the version does not match,
        the URL remains unchanged. This ensures that only the most recent version can invalidate the URL.

        Args:
            version: The version to check before invalidating the URL.
        """
        with self.lock:
            if version == self.current_version:
                self._current_url = None
                self.current_version += 1
