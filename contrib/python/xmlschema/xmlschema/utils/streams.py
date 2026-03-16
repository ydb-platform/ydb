#
# Copyright (c), 2024-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from io import BufferedIOBase
from threading import Lock
from typing import Any, Optional, Union


DEFAULT_BUFFER_SIZE = 8 * 1024


def is_file_object(obj: object) -> bool:
    return hasattr(obj, 'read') and hasattr(obj, 'seekable') \
        and hasattr(obj, 'tell') and hasattr(obj, 'seek') \
        and hasattr(obj, 'closed') and hasattr(obj, 'close')


class DefusableReader(BufferedIOBase):
    """
    A class for wrapping a not seekable buffered IO stream in a partially seekable
    stream that can be defused. The initial buffer size is 64KiB and can't be lower
    than io.DEFAULT_BUFFER_SIZE.
    """
    def __init__(self, fp: BufferedIOBase, initial_buffer_size: int = 64 * 1024):
        if not isinstance(fp, BufferedIOBase):
            raise TypeError(
                f'"fp" argument must an instance of {BufferedIOBase} or a derived class'
            )
        if not fp.readable():
            raise OSError('"fp" argument must be readable')
        if fp.closed:
            raise OSError('"fp" argument must be a not closed file descriptor')
        if initial_buffer_size < DEFAULT_BUFFER_SIZE:
            initial_buffer_size = DEFAULT_BUFFER_SIZE

        buf = bytearray()
        buf += fp.read(initial_buffer_size)
        self._buffer = buf
        self._buffer_size = len(buf)
        self._fp = fp
        self._pos = 0
        self._fp_lock = Lock()

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        state.pop('_fp_lock', None)
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._xpath_lock = Lock()

    def readable(self) -> bool:
        return self._fp.readable()

    def seekable(self) -> bool:
        self._checkClosed()
        return self._pos < self._buffer_size or self._fp.seekable()

    def seek(self, pos: int, whence: int = 0) -> int:
        if self.closed:
            raise ValueError("seek on closed file")
        if not isinstance(pos, int):
            raise TypeError(f"{pos!r} is not an integer")

        with self._fp_lock:
            if whence == 0:
                if pos < 0:
                    raise ValueError(f"negative seek position {pos!r}")
            elif whence == 1:
                pos = max(0, self._pos + pos)
            elif whence == 2:
                pos = self._fp.seek(pos, 2)
            else:
                raise ValueError("unsupported whence value")

            if pos > self._buffer_size and whence != 2:
                pos = self._fp.seek(pos)
            elif self._pos > self._buffer_size:
                self._fp.seek(self._buffer_size)
            self._pos = pos
            return self._pos

    def tell(self) -> int:
        return self._pos

    def getbuffer(self) -> memoryview:
        if self.closed:
            raise ValueError("getbuffer on closed file")
        return memoryview(self._buffer)

    def close(self) -> None:
        with self._fp_lock:
            self._buffer.clear()
            self._fp.close()

    def read(self, size: Optional[int] = None) -> bytes:
        self._checkClosed()
        if size is not None and size < -1:
            raise ValueError("invalid number of bytes to read")

        with self._fp_lock:
            return self._read_unlocked(size)

    def _read_unlocked(self, size: Optional[int] = None) -> bytes:
        data: Union[bytes, bytearray]

        if self._pos >= self._buffer_size:
            data = self._fp.read(size)
            self._pos += len(data)
            return data

        buffer = self._buffer[self._pos:]
        if size is not None and size > -1:
            if size <= len(buffer):
                data = buffer[:size]
            else:
                chunk = self._fp.read(size - len(buffer))
                data = buffer if not chunk else buffer + chunk

        elif hasattr(self._fp, 'readall'):
            chunk = self._fp.readall()
            data = buffer if not chunk else buffer + chunk
        else:
            chunks: list[Union[bytes, bytearray]] = [buffer]
            while True:
                chunk = self._fp.read()
                if not chunk:
                    break
                chunks.append(chunk)
            data = b"".join(chunks)

        self._pos += len(data)
        if isinstance(data, bytearray):
            return bytes(data)
        return data

    def read1(self, size: int = -1) -> bytes:
        return self.read(size)
