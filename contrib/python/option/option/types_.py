# MIT License

# Copyright (c) 2018-2022 Peijun Ma

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# pylint: skip-file

import sys
from typing import TypeVar

if sys.version_info >= (3, 8):
    from typing import Protocol
else:
    from typing_extensions import Protocol


T = TypeVar('T')
U = TypeVar('U')
A = TypeVar('A')

K = TypeVar('K')
V = TypeVar('V')

E = TypeVar('E')
F = TypeVar('F')


class SupportsDunderLT(Protocol):
    def __lt__(self, __other: object) -> bool: ...


class SupportsDunderGT(Protocol):
    def __gt__(self, __other: object) -> bool: ...


class SupportsDunderLE(Protocol):
    def __le__(self, __other: object) -> bool: ...


class SupportsDunderGE(Protocol):
    def __ge__(self, __other: object) -> bool: ...
