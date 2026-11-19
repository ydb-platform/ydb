# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Final

PROFILE_FRAME_TYPE: Final = "profile.frame.type"
"""
Describes the interpreter or compiler of a single frame.
"""


class ProfileFrameTypeValues(Enum):
    DOTNET = "dotnet"
    """[.NET](https://wikipedia.org/wiki/.NET)."""
    JVM = "jvm"
    """[JVM](https://wikipedia.org/wiki/Java_virtual_machine)."""
    KERNEL = "kernel"
    """[Kernel](https://wikipedia.org/wiki/Kernel_(operating_system))."""
    NATIVE = "native"
    """Can be one of but not limited to [C](https://wikipedia.org/wiki/C_(programming_language)), [C++](https://wikipedia.org/wiki/C%2B%2B), [Go](https://wikipedia.org/wiki/Go_(programming_language)) or [Rust](https://wikipedia.org/wiki/Rust_(programming_language)). If possible, a more precise value MUST be used."""
    PERL = "perl"
    """[Perl](https://wikipedia.org/wiki/Perl)."""
    PHP = "php"
    """[PHP](https://wikipedia.org/wiki/PHP)."""
    CPYTHON = "cpython"
    """[Python](https://wikipedia.org/wiki/Python_(programming_language))."""
    RUBY = "ruby"
    """[Ruby](https://wikipedia.org/wiki/Ruby_(programming_language))."""
    V8JS = "v8js"
    """[V8JS](https://wikipedia.org/wiki/V8_(JavaScript_engine))."""
    BEAM = "beam"
    """[Erlang](https://en.wikipedia.org/wiki/BEAM_(Erlang_virtual_machine))."""
    GO = "go"
    """[Go](https://wikipedia.org/wiki/Go_(programming_language)),."""
    RUST = "rust"
    """[Rust](https://wikipedia.org/wiki/Rust_(programming_language))."""
    LUAJIT = "luajit"
    """[LuaJIT](https://en.wikipedia.org/wiki/LuaJIT)."""
