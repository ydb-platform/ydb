"""Input/output helpers."""

from typing import IO


def read_byte_checked(io: IO[bytes]) -> int:
    """
    Read one byte.

    Raises:
        EOFError: the stream has ended
    """
    buffer = io.read(1)
    if not buffer:
        raise EOFError
    return buffer[0]


def read_checked(io: IO[bytes], count: int) -> bytes:
    """
    Read the specified number of bytes.

    Raises:
        EOFError: the stream has ended
    """
    buffer = io.read(count)
    if len(buffer) < count:
        raise EOFError(f"{count} bytes expected, but only {len(buffer)} read")
    return buffer
