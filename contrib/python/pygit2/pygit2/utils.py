# Copyright 2010-2025 The pygit2 contributors
#
# This file is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2,
# as published by the Free Software Foundation.
#
# In addition to the permissions in the GNU General Public License,
# the authors give you unlimited permission to link the compiled
# version of this file into combinations with other programs,
# and to distribute those combinations without any restriction
# coming from the use of this file.  (The General Public License
# restrictions do apply in other respects; for example, they cover
# modification of the file, and distribution when not linked into
# a combined executable.)
#
# This file is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; see the file COPYING.  If not, write to
# the Free Software Foundation, 51 Franklin Street, Fifth Floor,
# Boston, MA 02110-1301, USA.

import contextlib
import os
from collections.abc import Generator, Iterator, Sequence
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Generic,
    Optional,
    Protocol,
    TypeVar,
    Union,
    overload,
)

# Import from pygit2
from .ffi import C, ffi

if TYPE_CHECKING:
    from ._libgit2.ffi import ArrayC, GitStrrayC, char, char_pointer


def maybe_string(ptr: 'char_pointer | None') -> str | None:
    if not ptr:
        return None

    return ffi.string(ptr).decode('utf8', errors='surrogateescape')


@overload
def to_bytes(
    s: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    encoding: str = 'utf-8',
    errors: str = 'strict',
) -> bytes: ...
@overload
def to_bytes(
    s: Union['ffi.NULL_TYPE', None],
    encoding: str = 'utf-8',
    errors: str = 'strict',
) -> Union['ffi.NULL_TYPE']: ...
def to_bytes(
    s: Union[str, bytes, 'ffi.NULL_TYPE', os.PathLike[str], os.PathLike[bytes], None],
    encoding: str = 'utf-8',
    errors: str = 'strict',
) -> Union[bytes, 'ffi.NULL_TYPE']:
    if s == ffi.NULL or s is None:
        return ffi.NULL

    if hasattr(s, '__fspath__'):
        s = os.fspath(s)

    if isinstance(s, bytes):
        return s

    return s.encode(encoding, errors)  # type: ignore[union-attr]


def to_str(s: str | bytes | os.PathLike[str] | os.PathLike[bytes]) -> str:
    if hasattr(s, '__fspath__'):
        s = os.fspath(s)

    if type(s) is str:
        return s

    if type(s) is bytes:
        return s.decode()

    raise TypeError(f'unexpected type "{repr(s)}"')


def ptr_to_bytes(ptr_cdata) -> bytes:
    """
    Convert a pointer coming from C code (<cdata 'some_type *'>)
    to a byte buffer containing the address that the pointer refers to.
    """

    pp = ffi.new('void **', ptr_cdata)
    return bytes(ffi.buffer(pp)[:])


@contextlib.contextmanager
def new_git_strarray() -> Generator['GitStrrayC', None, None]:
    strarray = ffi.new('git_strarray *')
    yield strarray
    C.git_strarray_dispose(strarray)


def strarray_to_strings(arr) -> list[str]:
    """
    Return a list of strings from a git_strarray pointer.

    Free the strings contained in the git_strarry, this means it won't be usable after
    calling this function.
    """
    try:
        return [ffi.string(arr.strings[i]).decode('utf-8') for i in range(arr.count)]
    finally:
        C.git_strarray_dispose(arr)


class StrArray:
    """A git_strarray wrapper

    Use this in order to get a git_strarray* to pass to libgit2 out of a
    list of strings. This has a context manager, which you should use, e.g.

        with StrArray(list_of_strings) as arr:
            C.git_function_that_takes_strarray(arr.ptr)

    To make a pre-existing git_strarray point to the provided list of strings,
    use the context manager's assign_to() method:

        struct = ffi.new('git_strarray *', [ffi.NULL, 0])
        with StrArray(list_of_strings) as arr:
            arr.assign_to(struct)

    The above construct is still subject to FFI scoping rules, i.e. the
    contents of 'struct' only remain valid within the StrArray context.
    """

    __array: 'GitStrrayC | ffi.NULL_TYPE'
    __strings: list['None | ArrayC[char]']
    __arr: 'ArrayC[char_pointer]'

    def __init__(self, lst: None | Sequence[str | os.PathLike[str]]):
        # Allow passing in None as lg2 typically considers them the same as empty
        if lst is None:
            self.__array = ffi.NULL
            return

        if not isinstance(lst, (list, tuple)):
            raise TypeError('Value must be a list')

        strings: list[None | 'ArrayC[char]'] = [None] * len(lst)
        for i in range(len(lst)):
            li = lst[i]
            if not isinstance(li, str) and not hasattr(li, '__fspath__'):
                raise TypeError('Value must be a string or PathLike object')

            strings[i] = ffi.new('char []', to_bytes(li))

        self.__arr = ffi.new('char *[]', strings)
        self.__strings = strings
        self.__array = ffi.new('git_strarray *', [self.__arr, len(strings)])  # type: ignore[call-overload]

    def __enter__(self) -> 'StrArray':
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        pass

    @property
    def ptr(self) -> 'GitStrrayC | ffi.NULL_TYPE':
        return self.__array

    def assign_to(self, git_strarray: 'GitStrrayC') -> None:
        if self.__array == ffi.NULL:
            git_strarray.strings = ffi.NULL
            git_strarray.count = 0
        else:
            git_strarray.strings = self.__arr
            git_strarray.count = len(self.__strings)


T = TypeVar('T')
U = TypeVar('U', covariant=True)


class SequenceProtocol(Protocol[U]):
    def __len__(self) -> int: ...
    def __getitem__(self, index: int) -> U: ...


class GenericIterator(Generic[T]):
    """Helper to easily implement an iterator.

    The constructor gets a container which must implement __len__ and
    __getitem__
    """

    def __init__(self, container: SequenceProtocol[T]) -> None:
        self.container = container
        self.length = len(container)
        self.idx = 0

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        idx = self.idx
        if idx >= self.length:
            raise StopIteration

        self.idx += 1
        return self.container[idx]
