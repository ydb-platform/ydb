import io
import numbers
import os
from collections.abc import Iterator, Sequence
from typing import SupportsInt, TypeAlias, TypeVar

from _typeshed import FileDescriptorLike, ReadableBuffer, StrOrBytesPath

FILENO_ERRORS: tuple[Exception]
__write__ = os.write

def send_offset(fd: int, buf: ReadableBuffer, offset: int) -> int: ...

fsencode = os.fsencode
fsdecode = os.fsdecode

MaybeFileNo: TypeAlias = numbers.Integral | io.IOBase

def maybe_fileno(f: MaybeFileNo) -> numbers.Integral: ...
def get_fdmax(default: int | None = ...) -> int | None: ...

_T = TypeVar("_T")

def uniq(it: Sequence[_T]) -> Iterator[_T]: ...

closerange = os.closerange

def close_open_fds(keep: Sequence[MaybeFileNo] | None = ...) -> None: ...
def get_errno(exc: Exception | None) -> int: ...
def spawnv_passfds(
    path: StrOrBytesPath, args: os._ExecVArgs, passfds: Sequence[MaybeFileNo]
) -> int: ...
def isblocking(handle: FileDescriptorLike) -> bool: ...
def setblocking(handle: FileDescriptorLike, blocking: bool) -> None: ...

E_PSUTIL_MISSING: str
E_RESOURCE_MISSING: str

def maxrss_to_kb(v: SupportsInt) -> int: ...
def mem_rss() -> int: ...
