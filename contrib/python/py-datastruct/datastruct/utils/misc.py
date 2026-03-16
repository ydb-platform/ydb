#  Copyright (c) Kuba Szczodrzyński 2023-1-6.

from abc import ABC
from io import SEEK_CUR, SEEK_END, SEEK_SET
from typing import IO, AnyStr


def repstr(string, length: int):
    # a significantly faster version of https://stackoverflow.com/a/9021522/9438331
    return (string * (length // len(string) + 1))[0:length]


def pad_up(x: int, n: int) -> int:
    """Return how many bytes of padding is needed to align 'x'
    up to block size of 'n'."""
    return (n - (x % n)) % n


def dict2str(data: dict) -> str:
    return ", ".join(f"{k}={v}" for k, v in data.items())


class SizingIO(IO[bytes], ABC):
    pos: int = 0
    size: int = 0

    def tell(self) -> int:
        return self.pos

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        if whence == SEEK_SET:
            self.pos = offset
        elif whence == SEEK_CUR:
            self.pos += offset
        elif whence == SEEK_END:
            self.pos = self.size - offset
        self.size = max(self.pos, self.size)
        return self.pos

    def write(self, s: AnyStr) -> int:
        if not isinstance(s, int):
            s = len(s)
        self.pos += s
        self.size = max(self.pos, self.size)
        return s

    def add(self, n: int):
        self.pos += n
        self.size = max(self.pos, self.size)


class MemoryIO(IO[bytes], ABC):
    def __init__(self, address: int):
        self.start = address
        self.addr = address

    def tell(self) -> int:
        return self.addr - self.start

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        if whence == SEEK_SET:
            self.addr = self.start + offset
        elif whence == SEEK_CUR:
            self.addr += offset
        elif whence == SEEK_END:
            raise NotImplementedError("No size")
        return self.addr

    def read(self, n: int = None) -> bytes:
        from ctypes import c_char

        data = (c_char * n).from_address(self.addr)
        self.addr += n
        return data.raw
